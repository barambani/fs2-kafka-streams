package com.iravid.fs2.kafka.streams

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import fs2.Stream
import java.nio.charset.StandardCharsets
import org.rocksdb.{ ColumnFamilyDescriptor, ColumnFamilyHandle => RocksDBColFHandle, RocksDB }
import scodec.Codec
import scodec.bits.BitVector

import scala.collection.JavaConverters._

trait KeyValueStore[F[_], K, V] {
  type ColumnFamilyHandle

  def columnFamilies: F[List[ColumnFamilyHandle]]
  def createColumnFamily(name: String): F[ColumnFamilyHandle]

  def get(k: K): F[Option[V]]
  def get(columnFamily: ColumnFamilyHandle, k: K): F[Option[V]]

  def getAll(ks: List[K]): F[Map[K, V]]
  def getAll(columnFamily: ColumnFamilyHandle, ks: List[K]): F[Map[K, V]]

  def put(k: K, v: V): F[Unit]
  def put(columnFamily: ColumnFamilyHandle, k: K, v: V): F[Unit]

  def delete(k: K): F[Unit]
  def delete(columnFamily: ColumnFamilyHandle, k: K): F[Unit]

  def scan: Stream[F, (K, V)]
  def scan(columnFamily: ColumnFamilyHandle): Stream[F, (K, V)]
}

class RocksDBKeyValueStore[F[_], K, V](
  rocksdb: RocksDB,
  rocksDbColumnFamilies: Ref[F, Map[Int, RocksDBColFHandle]],
  defaultColumnFamily: RocksDBColFHandle)(implicit F: Sync[F], K: Codec[K], V: Codec[V])
    extends KeyValueStore[F, K, V] {
  type ColumnFamilyHandle = Int

  def columnFamilies: F[List[ColumnFamilyHandle]] =
    rocksDbColumnFamilies.get.map(_.values.map(_.getID).toList)
  def createColumnFamily(name: String) =
    F.delay(
        rocksdb.createColumnFamily(
          new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8))))
      .flatMap { handle =>
        rocksDbColumnFamilies.update(_ + (handle.getID -> handle)).as(handle.getID)
      }

  def get0(h: RocksDBColFHandle, k: K) =
    F.delay {
      Option(rocksdb.get(h, K.encode(k).require.toByteArray))
        .map(bytes => V.decodeValue(BitVector.view(bytes)).require)
    }

  def get(k: K): F[Option[V]] = get0(defaultColumnFamily, k)
  def get(columnFamily: ColumnFamilyHandle, k: K): F[Option[V]] =
    rocksDbColumnFamilies.get
      .flatMap(
        _.get(columnFamily)
          .flatTraverse(get0(_, k)))

  def getAll0(h: RocksDBColFHandle, ks: List[K]): F[Map[K, V]] = F.delay {
    val serializedKeys = ks.map(K.encode(_).require.toByteArray)
    val result = rocksdb.multiGet(List.fill(ks.size)(h).asJava, serializedKeys.asJava).asScala.toMap

    result.map {
      case (k, v) =>
        K.decodeValue(BitVector.view(k)).require ->
          V.decodeValue(BitVector.view(v)).require
    }
  }
  def getAll(ks: List[K]) = getAll0(defaultColumnFamily, ks)
  def getAll(columnFamily: ColumnFamilyHandle, ks: List[K]): F[Map[K, V]] =
    rocksDbColumnFamilies.get.flatMap(_.get(columnFamily) match {
      case Some(h) => getAll0(h, ks)
      case None    => F.pure(Map())
    })

  def delete0(h: RocksDBColFHandle, k: K) = F.delay {
    rocksdb.delete(h, K.encode(k).require.toByteArray)
  }
  def delete(k: K) = delete0(defaultColumnFamily, k)
  def delete(columnFamily: ColumnFamilyHandle, k: K) =
    rocksDbColumnFamilies.get.flatMap(
      _.get(columnFamily)
        .traverse_(delete0(_, k)))

  def put0(h: RocksDBColFHandle, k: K, v: V) = F.delay {
    rocksdb.put(h, K.encode(k).require.toByteArray, V.encode(v).require.toByteArray)
  }
  def put(k: K, v: V) = put0(defaultColumnFamily, k, v)
  def put(columnFamily: ColumnFamilyHandle, k: K, v: V) =
    rocksDbColumnFamilies.get.flatMap(
      _.get(columnFamily)
        .traverse_(put0(_, k, v)))

  def scan0(h: RocksDBColFHandle): Stream[F, (K, V)] =
    Stream
      .bracket {
        F.delay {
          val iterator = rocksdb.newIterator(h)
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

  def scan: Stream[F, (K, V)] = scan0(defaultColumnFamily)
  def scan(columnFamily: ColumnFamilyHandle): Stream[F, (K, V)] =
    Stream.eval(rocksDbColumnFamilies.get.map(_.get(columnFamily))) flatMap {
      case None    => Stream.empty
      case Some(h) => scan0(h)
    }
}
