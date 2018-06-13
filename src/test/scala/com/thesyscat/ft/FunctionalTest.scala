package com.thesyscat.ft

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.util.Try

class FunctionalTest  extends WordSpec with Matchers with BeforeAndAfterAll {

  System.setProperty("valid.statement.config.path",getClass.getClassLoader.getResource("testvalidcmd.txt").getPath)

  val session = SparkSession.builder().appName("ThesysThrifServerTest")
      .master("local[2]").enableHiveSupport().config("hive.server2.thrift.port","9876")
      .getOrCreate()
  Try(session.sql("drop table data_tbl"))
  session.read.text(System.getProperty("valid.statement.config.path")).write.saveAsTable("data_tbl")

  HiveThriftServer2.startWithContext(session.sqlContext)

  Class.forName("org.apache.hive.jdbc.HiveDriver")
  val con = DriverManager.getConnection("jdbc:hive2://localhost:9876/default","a","a")

  "Functional test for JDBC connectivity" should{

    "throw exception for delete statement " in {
      val stmt = con.createStatement();
      val thrown = intercept[Exception] {
        stmt.execute("drop table events")
      }
      assert(thrown.getMessage === s"User a is not allowed to run this query. Only SELECT queries are permitted")
      stmt.close()
    }
    "select count must return correct results" in{

      val stmt = con.createStatement();
      val res = stmt.executeQuery("select count(*) from data_tbl")
      while(res.next()){
        assert(res.getLong(1) === 5)
      }
      res.close()
      stmt.close()
    }
  }

  override def afterAll() = {
    con.close()
    session.stop()
  }
}
