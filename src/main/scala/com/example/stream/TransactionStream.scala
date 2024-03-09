package com.example.stream

import cats.data.EitherT
import cats.effect.{Ref, Resource}
import cats.effect.kernel.Async
import cats.effect.std.Queue
import fs2.Stream
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import skunk._

import scala.concurrent.duration.FiniteDuration
import cats.instances.queue

// All SQL queries inside the Queries object are correct and should not be changed
final class TransactionStream[F[_]](
  operationTimer: FiniteDuration,
  orders: Queue[F, Option[OrderRow]],
  session: Resource[F, Session[F]],
  transactionCounter: Ref[F, Int], // updated if long IO succeeds
  stateManager: StateManager[F]    // utility for state management
)(implicit F: Async[F], logger: Logger[F]) {

  def stream: Stream[F, Unit] = {
    Stream
      .fromQueueNoneTerminated(orders)
      .evalMap(order => F.uncancelable(_ => processUpdate(order))) // TODO: uncancelable looks dangerous here
  }

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled
  private def processUpdate(updatedOrder: OrderRow): F[Unit] = {
    PreparedQueries(session)
      .use { queries =>
        for {
          // Get current known order state
          state <- stateManager.getOrderState(updatedOrder, queries)
          _ <- TransactionRow(state = state, updated = updatedOrder) match {
            // TODO: Log ignored transaction
            case None => F.unit // Ignore invalid transaction
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
  def publish(update: OrderRow): F[Unit]                                          = orders.offer(Some(update))
  def getCounter: F[Int]                                                          = transactionCounter.get
  def setSwitch(value: Boolean): F[Unit]                                          = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = stateManager.add(order, insert)
  def runRemainder: F[Unit]                                                       = orders.offer(None) >> stream.compile.drain
  // helper methods for testing
}

object TransactionStream {

  def apply[F[_]: Async: Logger](
    operationTimer: FiniteDuration,
    session: Resource[F, Session[F]]
  ): Resource[F, TransactionStream[F]] = {
    Resource.make {
      for {
        counter      <- Ref.of(0)
        queue        <- Queue.unbounded[F, Option[OrderRow]]
        stateManager <- StateManager.apply
      } yield new TransactionStream[F](
        operationTimer,
        queue,
        session,
        counter,
        stateManager
      )
    }{ txStream => txStream.runRemainder }
  }
}
