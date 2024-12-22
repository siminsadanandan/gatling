package oraloadgen

import java.sql.{Connection, SQLException}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.gatling.core.Predef._
import io.gatling.commons.stats.{KO, OK}
import io.gatling.core.structure.ChainBuilder
import io.gatling.core.stats.StatsEngine
import scala.concurrent.duration._


object Database {
  private val hikariConfig = new HikariConfig()
  hikariConfig.setJdbcUrl("jdbc:oracle:thin:@localhost:1521/FREEPDB1")
  hikariConfig.setUsername("test_user")
  hikariConfig.setPassword("test_password")
  hikariConfig.setMaximumPoolSize(10)
  hikariConfig.setDriverClassName("oracle.jdbc.OracleDriver")

  private val dataSource = new HikariDataSource(hikariConfig)

  def getConnection: Connection = dataSource.getConnection

  def close(): Unit = dataSource.close()
}

class OracleDMLSimulation extends Simulation {


  // Execute a query
  def executeQuery(query: String, params: Map[String, Any]): Either[String, Long] = {
    val startTime = System.currentTimeMillis()
    try {
      val connection = Database.getConnection
      try {
        val preparedStatement = connection.prepareStatement(query)
        params.zipWithIndex.foreach { case ((_, value), index) =>
          preparedStatement.setObject(index + 1, value)
        }
        preparedStatement.executeUpdate()
        preparedStatement.close()
        val endTime = System.currentTimeMillis()
        Right(endTime - startTime)
      } finally {
        connection.close()
      }
    } catch {
      case e: SQLException => Left(e.getMessage)
    }
  }

  // Log and track database queries
  def trackQuery(query: String, params: Map[String, Any], operationName: String): ChainBuilder = {
    exec { session =>
      val startTime = System.currentTimeMillis()

      // Resolve parameters from the session
      val resolvedParams = params.map {
        case (key, value: String) if value.startsWith("${") && value.endsWith("}") =>
          val sessionKey = value.substring(2, value.length - 1)
          key -> session(sessionKey).validate[Any].toOption.getOrElse(
            throw new RuntimeException(s"Session key '$sessionKey' not found")
          )
        case (key, value) => key -> value
      }

      // Execute the query
      val result = executeQuery(query, resolvedParams)
      val endTime = System.currentTimeMillis()

      result match {
        case Right(execTime) =>
          session("statsEngine").asOption[StatsEngine].foreach { statsEngine =>
            statsEngine.logResponse(
              session.scenario,
              session.groups,
              operationName,
              startTime,
              endTime,
              OK,
              None,
              None
            )
          }
          println(s"$operationName succeeded in $execTime ms")
          session.markAsSucceeded
        case Left(errorMessage) =>
          session("statsEngine").asOption[StatsEngine].foreach { statsEngine =>
            statsEngine.logResponse(
              session.scenario,
              session.groups,
              operationName,
              startTime,
              endTime,
              KO,
              None,
              Some(errorMessage)
            )
          }
          println(s"$operationName failed: $errorMessage")
          session.markAsFailed
      }
      session
    }
  }

  // Feeder for unique IDs
  val feeder = Iterator.from(1).map(i => Map("id" -> i))

  // Define operations
  val insertOperation = trackQuery(
    "INSERT INTO test_table (id, name, age) VALUES (?, ?, ?)",
    Map("1" -> "${id}", "2" -> "John Doe", "3" -> 30),
    "Insert"
  )

  val updateOperation = trackQuery(
    "UPDATE test_table SET name = ? WHERE id = ?",
    Map("1" -> "Jane Doe", "2" -> "${id}"),
    "Update"
  )

  val deleteOperation = trackQuery(
    "DELETE FROM test_table WHERE id = ?",
    Map("1" -> "${id}"),
    "Delete"
  )

  // Scenario definition
  val scn = scenario("Oracle DML Operations")
    .feed(feeder)
    .exec(insertOperation)
    .pause(1)
    .exec(updateOperation)
    .pause(1)
    .exec(deleteOperation)

  // Simulation setup
  setUp(
    scn.inject(
      constantUsersPerSec(10).during(1.minutes) // Simulate 10 users per second for 5 minutes
    )
  )

  after {
    println("Shutting down the connection pool...")
    Database.close()
  }
}
