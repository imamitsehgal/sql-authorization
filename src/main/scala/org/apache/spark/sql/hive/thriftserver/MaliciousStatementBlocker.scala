package org.apache.spark.sql.hive.thriftserver

import org.apache.commons.logging.LogFactory
import org.apache.hive.service.cli.HiveSQLException

import scala.io.Source

object MaliciousStatementBlocker {

  val log = LogFactory.getLog("MaliciousStatementBlocker")

  private var validCommands: Map[String, Boolean] = _

  def loadValidCommands = {

    validCommands = {
      val pathofValidCmdFile = Option(System.getProperty("valid.statement.config.path"))
      if (pathofValidCmdFile.isDefined)
        Source.fromFile(pathofValidCmdFile.get).getLines().map(cmd => (cmd -> true)).toMap
      else
        throw new IllegalStateException("Unable to start the ThriftServer set valid.statement.config.path=<path to list of valid commands> ")
    }
  }
  loadValidCommands

  def error(sql:String, user:String)={
    log.error(s"Malicious SQL: ${sql}")
    throw new HiveSQLException(s"User $user is not allowed to run this query. Only SELECT queries are permitted","NOT_PERMITTED_QUERY",403)
  }

  def validate(user:String,sql:String):String={
    val sqlToMatch = sql.trim.toLowerCase
    if(validCommands.contains(sqlToMatch))
      sql
    else if(validCommands.contains(sqlToMatch.split(" ")(0)))
      sql
    else
      error(sql,user)
  }

}
