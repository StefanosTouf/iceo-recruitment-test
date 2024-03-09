package com.example.stream

import fs2.Stream
import cats.effect.std.Queue
import cats.MonadThrow
import com.example.model.OrderRow
import cats.effect.Concurrent
import cats.syntax.all._

// Holds queues to which users can route order updates.
final class Exchange[F[_]](
  orderTopics: Vector[Queue[F, Option[OrderRow]]]
)(implicit F: MonadThrow[F]) {
  def topicCount: Int =
    orderTopics.length

  // Always routes updates with the same orderId to the same Queue
  // Assumes that the orderId is random enough to be evenly spread to the workers using hashcode
  def publish(order: OrderRow): F[Unit] = {
    val queueIdx: Int =
      Math.abs(order.orderId.hashCode() % topicCount)

    orderTopics.get(queueIdx) match {
      case None => F.raiseError(new RuntimeException(s"Queue index out of bounds, should be impossible. idx: $queueIdx"))
      case Some(value) => value.offer(Some(order))
    }
  }

  def getStreams: Stream[F, Stream[F, OrderRow]] =
    Stream.iterable(orderTopics.map(q => Stream.fromQueueNoneTerminated(q)))

  def signalTermination: F[Unit] =
    orderTopics.traverse_(_.offer(None))
}

object Exchange {
  def apply[F[_]: Concurrent](topicsCount: Int): F[Exchange[F]] = {
    val queues: F[Vector[Queue[F,Option[OrderRow]]]] =
      Vector
        .fill(topicsCount)(Queue.unbounded[F, Option[OrderRow]])
        .sequence

    queues.map(new Exchange[F](_))
  }
}