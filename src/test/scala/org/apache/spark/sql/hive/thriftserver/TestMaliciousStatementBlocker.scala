package org.apache.spark.sql.hive.thriftserver

import org.scalatest.{Matchers, WordSpec}


class TestMaliciousStatementBlocker extends WordSpec with Matchers{

val user = "amit"

  "SQL Validator" should{
    "throw exception for delete statement " in {
      val thrown = intercept[Exception] {
        MaliciousStatementBlocker.validate(user, "delete table sample")
      }
      assert(thrown.getMessage === s"User $user is not allowed to run this query. Only SELECT queries are permitted")
      }

    "throw exception for update statement " in {
      val thrown = intercept[Exception] {
        MaliciousStatementBlocker.validate(user, "update table sample set data=1")
      }
      assert(thrown.getMessage === s"User $user is not allowed to run this query. Only SELECT queries are permitted")
    }

    " does not throw exception for select statement " in {
      val query = "select * from abcd"
      val x = MaliciousStatementBlocker.validate(user, query)
      assert(x === query)
    }

    " does not throw exception for describe statement " in {
      val query = "describe abcd"
      val x = MaliciousStatementBlocker.validate(user, query)
      assert(x === query)
    }

    " throws exception if you try to set a property " in {
      val query = "set spark.sql.cbo.enable=true"
      val thrown = intercept[Exception]{MaliciousStatementBlocker.validate(user, query)}
      assert(thrown.getMessage === s"User $user is not allowed to run this query. Only SELECT queries are permitted")
    }

    " case sensitivity is also handled" in {
      val query = "DEScriBE test"
      val x = MaliciousStatementBlocker.validate(user, query)
      assert(x === query)
    }
    " case listing the config properties works fine " in {
      val query = "SET -v"
      val x = MaliciousStatementBlocker.validate(user, query)
      assert(x === query)
    }
    " case system property set " in {
      System.setProperty("valid.statement.config.path",getClass.getClassLoader.getResource("testvalidcmd.txt").getPath)
      val query = "refresh"
      MaliciousStatementBlocker.loadValidCommands
      val x = MaliciousStatementBlocker.validate(user, query)
      assert(x === query)
    }




    }
}
