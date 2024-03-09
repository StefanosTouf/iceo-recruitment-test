package com.example.stream

import cats.effect.Async
import com.example.model.OrderRow
import com.example.persistence.PreparedQueries
import skunk.PreparedCommand
import cats.syntax.all._
import fs2.concurrent.SignallingRef

//Utility for managing placed order state
//This can be used by other components, for example a stream that performs order placement will use add method
final class StateManager[F[_]: Async](ioSwitch: SignallingRef[F, Boolean]) {

  def getOrderState(order: OrderRow, queries: PreparedQueries[F]): F[Option[OrderRow]] = {
    queries.getOrder.option(order.orderId)
  }

  def add(row: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = {
    insert.execute(row).void
  }

  def getSwitch: F[Boolean]              = ioSwitch.get
  def setSwitch(value: Boolean): F[Unit] = ioSwitch.set(value)
}

object StateManager {

  def apply[F[_]: Async]: F[StateManager[F]] = {
    SignallingRef.of(false).map(new StateManager[F](_))
  }
}
