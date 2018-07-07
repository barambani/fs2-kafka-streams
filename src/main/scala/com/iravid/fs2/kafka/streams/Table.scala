package com.iravid.fs2.kafka.streams

import scodec.{ Attempt, Codec, Err }

trait Table[F[_], K, V] {
  def putAll(data: List[(K, V)]): F[Unit]

  def getAll(k: List[K]): F[Map[K, V]]

  def commit(offset: Long): F[Unit]

  def delete(k: K): F[Unit]
}

object RocksDBTable {
  case object CommitKey {
    import scodec.Encoder
    import scodec.codecs._

    implicit val CommitKeyCodec: Codec[CommitKey.type] = Codec(
      Encoder((c: CommitKey.type) => utf8_32.encode("CommitKey")),
      utf8_32.emap {
        case "CommitKey" => Attempt.successful(CommitKey)
        case other       => Attempt.failure(Err.General(s"Bad value for CommitKey: ${other}", Nil))
      }
    )

    implicit val K: Key.Aux[Codec, CommitKey.type, Long] =
      Key.instance(CommitKeyCodec, ulong(63))
  }
}

class RocksDBTable[F[_], K: Codec, V: Codec, CF](store: PolyKVStore.Aux[F, Codec, CF],
                                                 dataCF: CF,
                                                 offsetsCF: CF) {
  import RocksDBTable._

  implicit val K: Key.Aux[Codec, K, V] = Key.instance

  def putAll(data: List[(K, V)]): F[Unit] = store.putAll(dataCF, data)

  def getAll(ks: List[K]): F[Map[K, V]] = store.getAll(dataCF, ks)

  def delete(k: K): F[Unit] = store.delete(dataCF, k)

  def commit(offset: Long): F[Unit] =
    store.put(offsetsCF, CommitKey, offset)
}
