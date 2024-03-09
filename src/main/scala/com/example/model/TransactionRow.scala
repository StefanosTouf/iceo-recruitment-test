package com.example.model

import java.time.Instant
import java.util.UUID

case class TransactionRow(
  id: UUID,
  orderId: String, // the same as OrderRow
  amount: BigDecimal,
  createdAt: Instant
)

object TransactionRow {

  def apply(state: OrderRow, updated: OrderRow): Option[TransactionRow] = {
    val amount =
      updated.filled - state.filled

    // from README: All transactions must have positive (greater than zero) amount
    if (amount > 0)
      Some(
        TransactionRow(
          id = UUID.randomUUID(), // generate some id for our transaction
          orderId = state.orderId,
          amount = updated.filled - state.filled,
          createdAt = Instant.now()
        )
      )
    else
      None
  }
}
