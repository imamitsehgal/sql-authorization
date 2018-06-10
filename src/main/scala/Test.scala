import org.apache.spark.sql.hive.thriftserver.MaliciousStatementBlocker

object Test extends  App{


  MaliciousStatementBlocker.validate("a","select")
}
