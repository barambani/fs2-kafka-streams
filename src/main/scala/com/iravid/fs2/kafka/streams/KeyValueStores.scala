package com.iravid.fs2.kafka.streams

import cats.effect.concurrent.Ref
import cats.effect.{ Resource, Sync }
import cats.implicits._
import java.nio.file.Path
import org.rocksdb.{ ColumnFamilyDescriptor, ColumnFamilyHandle, Options, RocksDB }
import scodec.Codec

import scala.collection.JavaConverters._

trait KeyValueStores[C[_], P, F[_]] {
  def open[K: C, V: C](storeKey: P): Resource[F, KeyValueStore[F, K, V]]
}

class RocksDBKeyValueStores[F[_]](implicit F: Sync[F]) extends KeyValueStores[Codec, Path, F] {
  def open[K: Codec, V: Codec](storeKey: Path): Resource[F, KeyValueStore[F, K, V]] =
    Resource
      .make {
        for {
          cfNames <- F.delay(
                      RocksDB.listColumnFamilies(new Options(), storeKey.toAbsolutePath.toString))
          cfDescs = cfNames.asScala.map(new ColumnFamilyDescriptor(_))
          storeAndHandles <- F.delay {
                              val cfHandles = new java.util.ArrayList[ColumnFamilyHandle]()
                              val store =
                                RocksDB.open(
                                  storeKey.toAbsolutePath.toString,
                                  cfDescs.asJava,
                                  cfHandles)

                              (store, cfHandles.asScala.toList)
                            }
          (store, handles) = storeAndHandles
          defaultAndRest <- {
            val (before, after) =
              handles.span(h => java.util.Arrays.equals(h.getName, RocksDB.DEFAULT_COLUMN_FAMILY))
            val default = after.headOption
            val rest = before ++ after.drop(1)

            default match {
              case Some(d) => F.pure((d, rest))
              case None =>
                F.raiseError[(ColumnFamilyHandle, List[ColumnFamilyHandle])](
                  new Exception("Could not locate default column family!"))
            }
          }
          (default, rest) = defaultAndRest
          handlesRef <- Ref[F].of(rest.map(h => h.getID -> h).toMap)
        } yield (store, handlesRef, default)
      } {
        case (rocksdb, columnFamilyHandles, defaultColumnFamily) =>
          for {
            handles <- columnFamilyHandles.get
            _ <- handles.values.toList.traverse_ { handle =>
                  F.delay(handle.close())
                }
            _ <- F.delay(defaultColumnFamily.close())
            _ <- F.delay(rocksdb.close())
          } yield ()
      }
      .map {
        case (rocksdb, columnFamilyHandles, defaultColumnFamily) =>
          new RocksDBKeyValueStore(
            rocksdb,
            columnFamilyHandles,
            defaultColumnFamily
          )
      }
}
