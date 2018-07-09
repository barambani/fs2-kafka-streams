package com.iravid.fs2.kafka.streams

import cats.Functor
import cats.implicits._
import cats.effect.concurrent.Ref
import scodec.{ Attempt, Codec, Err }

trait Table[F[_], K, V] {
  def putAll(data: List[(K, V)]): F[Unit]

  def getAll(k: List[K]): F[Map[K, Option[V]]]

  def commit(offset: Long): F[Unit]

  def delete(k: K): F[Unit]
}

object InMemoryTable {
  import cats.data.Kleisli

  case class State[K, V](data: Map[K, V], offset: Long)

  def kleisli[F[_]: Functor, K, V]: Table[Kleisli[F, Ref[F, State[K, V]], ?], K, V] =
    new Table[Kleisli[F, Ref[F, State[K, V]], ?], K, V] {
      def commit(offset: Long): Kleisli[F, Ref[F, State[K, V]], Unit] =
        Kleisli(_.update(state => state.copy(offset = offset)))

      def delete(k: K): Kleisli[F, Ref[F, State[K, V]], Unit] =
        Kleisli(_.update(state => state.copy(data = state.data - k)))

      def getAll(k: List[K]): Kleisli[F, Ref[F, State[K, V]], Map[K, Option[V]]] =
        Kleisli(_.get.map(state => k.fproduct(state.data.get).toMap))

      def putAll(data: List[(K, V)]): Kleisli[F, Ref[F, State[K, V]], Unit] =
        Kleisli(_.update(state => state.copy(data = state.data ++ data)))
    }

  trait MonadState[F[_], S] {
    def get: F[S]
    def set: F[Unit]
    def update(f: S => S): F[Unit]
  }

  def mtl[F[_]: Functor, K, V](implicit S: MonadState[F, State[K, V]]): Table[F, K, V] =
    new Table[F, K, V] {
      def commit(offset: Long): F[Unit] =
        S.update(state => state.copy(offset = offset))

      def delete(k: K): F[Unit] =
        S.update(state => state.copy(data = state.data - k))

      def getAll(k: List[K]): F[Map[K, Option[V]]] =
        S.get.map(state => k.fproduct(state.data.get).toMap)

      def putAll(data: List[(K, V)]): F[Unit] =
        S.update(state => state.copy(data = state.data ++ data))
    }
}

class InMemoryTable[F[_]: Functor, K, V](ref: Ref[F, InMemoryTable.State[K, V]])
    extends Table[F, K, V] {
  def putAll(data: List[(K, V)]): F[Unit] =
    ref.update(state => state.copy(data = state.data ++ data))

  def getAll(k: List[K]): F[Map[K, Option[V]]] =
    ref.get.map(state => k.fproduct(state.data.get).toMap)

  def commit(offset: Long): F[Unit] =
    ref.update(state => state.copy(offset = offset))

  def delete(k: K): F[Unit] =
    ref.update(state => state.copy(data = state.data - k))
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

class RocksDBTable[F[_]: Functor, K: Codec, V: Codec, CF](store: PolyKVStore.Aux[F, Codec, CF],
                                                          dataCF: CF,
                                                          offsetsCF: CF)
    extends Table[F, K, V] {
  import RocksDBTable._

  implicit val K: Key.Aux[Codec, K, V] = Key.instance

  def putAll(data: List[(K, V)]): F[Unit] = store.putAll(dataCF, data)

  def getAll(ks: List[K]): F[Map[K, Option[V]]] =
    store.getAll(dataCF, ks).map { retrievedMap =>
      ks.fproduct(retrievedMap.get).toMap
    }

  def delete(k: K): F[Unit] = store.delete(dataCF, k)

  def commit(offset: Long): F[Unit] =
    store.put(offsetsCF, CommitKey, offset)
}
