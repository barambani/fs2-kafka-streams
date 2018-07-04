package com.iravid.fs2.kafka.streams

import cats.effect.{ Resource, Sync }
import cats.implicits._
import java.nio.file.Path
import org.rocksdb.RocksDB
import scodec.Codec

trait KeyValueStores[C[_], P, F[_]] {
  def open[K: C, V: C](storeKey: P): Resource[F, KeyValueStore[F, K, V]]
}

class RocksDBKeyValueStores[F[_]](implicit F: Sync[F]) extends KeyValueStores[Codec, Path, F] {
  def open[K: Codec, V: Codec](storeKey: Path): Resource[F, KeyValueStore[F, K, V]] =
    Resource
      .make {
        F.delay {
          RocksDB.open(storeKey.toAbsolutePath.toString)
        }
      }(rocksdb => F.delay(rocksdb.close()))
      .map(rocksdb => new RocksDBKeyValueStore[F, K, V](rocksdb))
}
