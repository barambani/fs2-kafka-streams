package com.iravid.fs2.kafka.streams

import cats.effect.{ Concurrent }
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.implicits._
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.client.{ CommitRequest, RecordStream }
import fs2.Stream
import java.nio.file.Path
import org.apache.kafka.common.TopicPartition
import scodec.Codec

trait ReadOnlyTable[F[_], K, V] {
  def get(k: K): F[Option[V]]

  def scan: Stream[F, (K, V)]
}

object ReadOnlyTable {
  object inMemory {
    def table[F[_]: Concurrent, K, V](stream: Stream[F, (K, V)]): F[ReadOnlyTable[F, K, V]] =
      for {
        ref <- Ref[F].of(Map[K, V]())
        updateProcess <- stream
                          .evalMap {
                            case (k, v) =>
                              ref.update(_ + (k -> v))
                          }
                          .compile
                          .drain
                          .start
        table = new ReadOnlyTable[F, K, V] {
          def get(k: K): F[Option[V]] = ref.get.map(_.get(k))
          def scan: Stream[F, (K, V)] =
            Stream.eval(ref.get).flatMap(m => Stream.fromIterator(m.iterator))
        }
      } yield table

    def partitioned[F[_]: Concurrent, K, T](recordStream: RecordStream.Partitioned[F, T])(
      key: T => K): Stream[F, (TopicPartition, ReadOnlyTable[F, K, T])] =
      recordStream.records.flatMap {
        case (tp, stream) =>
          Stream
            .eval {
              table {
                stream
                  .collect {
                    case EnvT(_, Right(t)) => t
                  }
                  .map(t => key(t) -> t)
              }
            }
            .map(tp -> _)
      }

    def plain[F[_]: Concurrent, K, T](recordStream: RecordStream.Plain[F, T])(
      key: T => K): F[ReadOnlyTable[F, K, T]] =
      table {
        recordStream.records
          .collect {
            case EnvT(_, Right(t)) => t
          }
          .map(t => key(t) -> t)
      }
  }

  object persistent {
    def partitioned[F[_]: Concurrent, K: Codec, V: Codec](
      stores: KeyValueStores[Codec, Path, F],
      storeKey: TopicPartition => Path,
      recordStream: RecordStream.Partitioned[F, V])(
      key: V => K
    ): Stream[F, (TopicPartition, ReadOnlyTable[F, K, V])] =
      recordStream.records.flatMap {
        case (tp, stream) =>
          Stream.resource(stores.open[K, V](storeKey(tp))) flatMap { store =>
            Stream.eval {
              stream
                .evalMap { record =>
                  record.fa.traverse { value =>
                    store.put(key(value), value)
                  } <* recordStream.commitQueue.requestCommit(
                    CommitRequest(record.env.topic, record.env.partition, record.env.offset)
                  )
                }
                .compile
                .drain
                .start
                .as(tp -> new ReadOnlyTable[F, K, V] {
                  def get(k: K): F[Option[V]] = store.get(k)
                  def scan: Stream[F, (K, V)] = store.scan
                })
            }
          }
      }

    def plain[F[_]: Concurrent, K, V](
      store: KeyValueStore[F, K, V],
      recordStream: RecordStream.Plain[F, V])(key: V => K): F[ReadOnlyTable[F, K, V]] =
      for {
        updateProcess <- recordStream.records
                          .evalMap { t =>
                            t.fa.traverse { v =>
                              store.put(key(v), v)
                            } <* recordStream.commitQueue.requestCommit(
                              CommitRequest(t.env.topic, t.env.partition, t.env.offset)
                            )
                          }
                          .compile
                          .drain
                          .start
        table = new ReadOnlyTable[F, K, V] {
          def get(k: K): F[Option[V]] = store.get(k)
          def scan: Stream[F, (K, V)] = store.scan
        }
      } yield table
  }

}
