package com.iravid.fs2.kafka

import cats.effect.Effect
import cats.implicits._
import fs2.Stream
import java.util.Properties
import scala.concurrent.ExecutionContext

import scala.concurrent.duration.FiniteDuration

case class Consumer[F[_]](commitQueue: CommitQueue[F], records: Stream[F, ByteRecord])

object Consumer {
  import KafkaConsumerFunctions._

  def apply[F[_]: Effect](settings: Properties, subscription: Subscription,
    maxPendingCommits: Int, pollInterval: FiniteDuration, pollTimeout: FiniteDuration)(
    implicit ec: ExecutionContext): F[Consumer[F]] =
    for {
      consumer <- createConsumer[F](settings)
      _ <- subscribe(consumer, subscription, None)
      commitQueue <- CommitQueue.create[F](maxPendingCommits)
      commits = commitQueue.queue.dequeue
      polls = Stream.every(pollInterval).as(Poll)
      res = commits.either(polls)
      .evalMap {
        case Left(req) =>
          (commit(consumer, req.asOffsetMap).void.attempt >>= req.promise.complete)
            .as(List.empty[ByteRecord])
        case Right(Poll) =>
          poll(consumer, pollTimeout)
      }
      .flatMap(Stream.emits(_))
    } yield Consumer(commitQueue, res)
}