package com.example.stream

import cats.data.EitherT
import cats.effect.{Ref, Resource, Concurrent}
import cats.effect.kernel.Async
import cats.effect.std.Queue
import fs2.Stream
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import skunk._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt
import cats.instances.queue
import cats.MonadThrow

final class Exchange[F[_]](
  orderTopics: Vector[Queue[F, Option[OrderRow]]]
)(implicit F: MonadThrow[F]) {
  def topicCount: Int =
    orderTopics.length

  def publish(order: OrderRow): F[Unit] = {
    val queueIdx: Int =
      Math.abs(order.orderId.hashCode() % topicCount)

    orderTopics.get(queueIdx) match {
      case None => F.raiseError(new RuntimeException(s"Queue index out of bounds, should be impossible: $queueIdx"))
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

// All SQL queries inside the Queries object are correct and should not be changed
final class TransactionStream[F[_]](
  operationTimer: FiniteDuration,
  acceptableUpdateStaleness: FiniteDuration,
  orders: Exchange[F],
  session: Resource[F, Session[F]],
  transactionCounter: Ref[F, Int], // updated if long IO succeeds
  stateManager: StateManager[F]    // utility for state management
)(implicit F: Async[F], logger: Logger[F]) {

  def stream: Stream[F, Unit] = {
    orders
      .getStreams
      .map(_.evalMap(order => F.uncancelable(_ => processUpdate(order)))) // TODO: uncancelable looks dangerous here
      .parJoin(orders.topicCount)
  }

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled
  private def processUpdate(updatedOrder: OrderRow): F[Unit] = {
    PreparedQueries(session)
      .use { queries =>
        // Get current known order state
        val getState: F[Option[OrderRow]] =
          stateManager.getOrderState(updatedOrder, queries)

        // TODO: Info log when waiting for state
        val getOrWaitForState: F[Option[OrderRow]] =
          getState >>= {
            case None        => F.sleep(acceptableUpdateStaleness) >> getState
            case Some(state) => F.pure(Option(state))
          }

        val makeTransaction: F[Option[TransactionRow]] =
          for {
            maybeState <- getOrWaitForState
            maybeTx    =  maybeState >>= (TransactionRow(_, updatedOrder))
          } yield maybeTx


        for {
          tx <- makeTransaction

          _ <- tx match {
            // TODO: Make this log more useful
            case None => logger.warn(s"Dropped invalid transation")
            case Some(transaction) =>
              // parameters for order update
              // We know that updatedOrder.orderId == state.orderId
              val params =
                updatedOrder.filled *: updatedOrder.orderId *: EmptyTuple

              for {
                // Perform long running operation first since its success is a prerequisite for a state update
                _ <- performLongRunningOperation(transaction).value.void.onError(th =>
                      logger.error(th)(s"Got error when performing long running IO!")
                    )
                // update order with params
                _ <- queries.updateOrder.execute(params)
                // insert the transaction
                _ <- queries.insertTransaction.execute(transaction)
              } yield ()
          }

        } yield ()
      }
  }

  // represents some long running IO that can fail
  private def performLongRunningOperation(transaction: TransactionRow): EitherT[F, Throwable, Unit] = {
    EitherT.liftF[F, Throwable, Unit](
      F.sleep(operationTimer) *>
        stateManager.getSwitch.flatMap {
          case false =>
            transactionCounter
              .updateAndGet(_ + 1)
              .flatMap(count =>
                logger.info(
                  s"Updated counter to $count by transaction with amount ${transaction.amount} for order ${transaction.orderId}!"
                )
              )
          case true => F.raiseError(throw new Exception("Long running IO failed!"))
        }
    )
  }

  // helper methods for testing
  def publish(update: OrderRow): F[Unit]                                          = orders.publish(update)
  def getCounter: F[Int]                                                          = transactionCounter.get
  def setSwitch(value: Boolean): F[Unit]                                          = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = stateManager.add(order, insert)
  def runRemainder: F[Unit]                                                       = orders.signalTermination >> stream.compile.drain
  // helper methods for testing
}

object TransactionStream {

  def apply[F[_]: Async: Logger](
    operationTimer: FiniteDuration,
    session: Resource[F, Session[F]],
    acceptableUpdateStaleness: FiniteDuration = 5.seconds,
    maxConcurrent: Int = 10
  ): Resource[F, TransactionStream[F]] = {
    Resource.make {
      for {
        counter      <- Ref.of(0)
        exchange     <- Exchange(maxConcurrent)
        stateManager <- StateManager.apply
      } yield new TransactionStream[F](
        operationTimer,
        acceptableUpdateStaleness,
        exchange,
        session,
        counter,
        stateManager
      )
    }{ txStream => txStream.runRemainder }
  }
}
