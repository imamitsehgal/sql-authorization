package org.apache.spark.sql.hive.thriftserver

import org.apache.hive.service.cli.{HiveSQLException, OperationState, OperationStatus}

object SQLValidator {

  def validate(user:String,sql:String):String={
     sql.trim.split(" ")(0) match {
       case "select" | "describe" | "show" => sql;
       case _ => throw new HiveSQLException(s"User $user is not allowed to run this query. Only SELECT queries are permitted","NOT_PERMITTED_QUERY",403)
     }

  }
}
