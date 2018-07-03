package com.iravid.fs2.kafka.streams

import cats.effect.{ Concurrent }
import cats.effect.concurrent.{ Deferred, Ref }
import cats.effect.implicits._
import cats.implicits._
import cats.effect.Resource
import fs2.Stream
import scodec.Codec

trait ReadOnlyTable[F[_], K, V] {
  def get(k: K): F[Option[V]]

  def scan: Stream[F, (K, V)]
}

object ReadOnlyTable {
  def inMemoryFromStream[F[_]: Concurrent, K, V](
    stream: Stream[F, (K, V)]): Resource[F, ReadOnlyTable[F, K, V]] = {
    val resources = for {
      ref      <- Ref[F].of(Map[K, V]())
      shutdown <- Deferred[F, Either[Throwable, Unit]]
      updateProcess <- stream
                        .interruptWhen(shutdown.get)
                        .evalMap(pair => ref.update(_ + pair))
                        .compile
                        .drain
                        .start
      table = new ReadOnlyTable[F, K, V] {
        def get(k: K): F[Option[V]] = ref.get.map(_.get(k))
        def scan: Stream[F, (K, V)] =
          Stream.eval(ref.get).flatMap(m => Stream.fromIterator(m.iterator))
      }
    } yield (shutdown, table)

    Resource
      .make(resources) {
        case (shutdown, _) =>
          shutdown.complete(Right(()))
      }
      .map(_._2)
  }

  def persistentFromStream[F[_]: Concurrent, K: Codec, V: Codec](
    store: KeyValueStore[F, K, V],
    stream: Stream[F, (K, V)]): Resource[F, ReadOnlyTable[F, K, V]] = {
    val resources = for {
      shutdown <- Deferred[F, Either[Throwable, Unit]]
      updateProcess <- stream
                        .interruptWhen(shutdown.get)
                        .evalMap {
                          case (key, value) => store.put(key, value)
                        }
                        .compile
                        .drain
                        .start
      table = new ReadOnlyTable[F, K, V] {
        def get(k: K): F[Option[V]] = store.get(k)
        def scan: Stream[F, (K, V)] = store.scan
      }
    } yield (shutdown, table)

    Resource
      .make(resources) {
        case (shutdown, _) => shutdown.complete(Right(()))
      }
      .map(_._2)
  }

}
