package com.example

import cats.effect.{Async, IO, IOApp}
import cats.effect.kernel.Resource
import cats.effect.std.Console
import com.example.config.DatabaseConfig
import com.example.persistence.{DatabasePool, FlywayMigration}
import com.example.stream.TransactionStream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

object Main extends IOApp.Simple {

  type F[A] = IO[A]
  implicit val console: Console[F] = IO.consoleForIO
  implicit val async: Async[F]     = IO.asyncForIO

  private val databaseConfig = DatabaseConfig()

  def run: IO[Unit] = {

    val resources = for {
      logger <- Resource.eval(Slf4jLogger.create[IO])
      pool   <- DatabasePool(databaseConfig)
    } yield (logger, pool)
    resources.use { case (logger, pool) =>
      implicit val _logger: Logger[F] = logger

      val streams = for {
        _      <- Resource.eval(FlywayMigration.migrate(databaseConfig))
        stream <- TransactionStream(1.second, pool.sessionResource, 5.seconds, 100)
      } yield new Streams(stream)

      streams.use(_.run)
    }
  }

  private class Streams(transactions: TransactionStream[F]) {

    def run: F[Unit] = transactions.stream.compile.drain
  }
}
