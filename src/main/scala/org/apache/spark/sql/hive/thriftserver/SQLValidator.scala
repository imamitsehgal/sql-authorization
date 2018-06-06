package org.apache.spark.sql.hive.thriftserver

object SQLValidator {

  def validate(user:String,sql:String):String={
    if(sql.trim.toLowerCase.startsWith("select")){
      return sql;
    }else{
      throw new RuntimeException(s"User $user is not allowed to run this query. You are only allowed to run a select query")
    }
  }
}
