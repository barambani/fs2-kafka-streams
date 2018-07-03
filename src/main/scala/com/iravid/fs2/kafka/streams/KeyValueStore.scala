package com.iravid.fs2.kafka.streams

import cats.effect.{ Resource, Sync }
import cats.implicits._
import fs2.Stream
import org.rocksdb.RocksDB
import scodec.Codec
import scodec.bits.BitVector

import scala.collection.JavaConverters._

trait KeyValueStore[F[_], K, V] {
  def get(k: K): F[Option[V]]

  def getAll(ks: List[K]): F[Map[K, V]]

  def put(k: K, v: V): F[Unit]

  def delete(k: K): F[Unit]

  def scan: Stream[F, (K, V)]
}

class RocksDBKeyValueStore[F[_], K: Codec, V: Codec](rocksdb: RocksDB)(implicit F: Sync[F],
                                                                       K: Codec[K],
                                                                       V: Codec[V])
    extends KeyValueStore[F, K, V] {

  def get(k: K): F[Option[V]] = F.delay {
    val serializedKey = K.encode(k).require.toByteArray

    Option(rocksdb.get(serializedKey))
      .map(bytes => V.decodeValue(BitVector.view(bytes)).require)
  }

  def getAll(ks: List[K]): F[Map[K, V]] = F.delay {
    val serializedKeys = ks.map(K.encode(_).require.toByteArray)
    val result = rocksdb.multiGet(serializedKeys.asJava).asScala.toMap

    result.map {
      case (k, v) =>
        K.decodeValue(BitVector.view(k)).require ->
          V.decodeValue(BitVector.view(v)).require
    }
  }

  def delete(k: K): F[Unit] = F.delay {
    rocksdb.delete(K.encode(k).require.toByteArray)
  }

  def put(k: K, v: V): F[Unit] = F.delay {
    rocksdb.put(K.encode(k).require.toByteArray, V.encode(v).require.toByteArray)
  }

  def scan: Stream[F, (K, V)] =
    Stream
      .bracket {
        F.delay {
          val iterator = rocksdb.newIterator()
          iterator.seekToFirst()
          iterator
        }
      }(iterator => F.delay(iterator.close()))
      .flatMap { iterator =>
        Stream.repeatEval {
          F.delay {
            if (iterator.isValid()) {
              val key = K.decodeValue(BitVector.view(iterator.key())).require
              val value = V.decodeValue(BitVector.view(iterator.value())).require

              iterator.next()

              Some((key, value))
            } else None
          }
        }.unNoneTerminate
      }
}

object RocksDBKeyValueStore {
  def create[F[_], K: Codec, V: Codec](path: String)(
    implicit F: Sync[F]): Resource[F, KeyValueStore[F, K, V]] =
    Resource
      .make {
        F.delay {
          RocksDB.open(path)
        }
      }(rocksdb => F.delay(rocksdb.close()))
      .map(new RocksDBKeyValueStore[F, K, V](_))
}
