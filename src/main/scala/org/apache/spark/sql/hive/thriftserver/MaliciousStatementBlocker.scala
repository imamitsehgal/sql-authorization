package org.apache.spark.sql.hive.thriftserver

import org.apache.commons.logging.LogFactory
import org.apache.hive.service.cli.HiveSQLException

object MaliciousStatementBlocker {

  val log = LogFactory.getLog("MaliciousStatementBlocker")

  def error(sql:String, user:String)={
    log.error(s"Malicious SQL: ${sql}")
    throw new HiveSQLException(s"User $user is not allowed to run this query. Only SELECT queries are permitted","NOT_PERMITTED_QUERY",403)
  }

  def validate(user:String,sql:String):String={
     sql.toLowerCase.trim.split(" ")(0) match {
       case "select" | "describe" | "show" | "use" => sql;
       case "set" => if(sql.trim.equalsIgnoreCase("set -v")) sql else error(sql,user)
       case x =>
         error(sql,user)
     }
  }

}
