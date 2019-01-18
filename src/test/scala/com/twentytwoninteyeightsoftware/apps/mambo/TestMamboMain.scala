package com.twentytwoninteyeightsoftware.apps.mambo

import java.io.File

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.scalatest.Assertions.assert


class TestMamboMain extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeAll()  {
    import java.sql.DriverManager
    val jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS auto"

    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(jdbcUrl, "sa", "sa")
    conn.createStatement().execute("create table auto.vehicles (id int, make varchar(50))")
    conn.createStatement().execute("insert into auto.vehicles values (1, 'Ford')")
    conn.createStatement().execute("insert into auto.vehicles values (2, 'Chevrolet')")

    conn.createStatement().execute("create table auto.vehicles_delta (id int, make varchar(50))")
    conn.createStatement().execute("insert into auto.vehicles_delta values (3, 'GMC')")
    conn.createStatement().execute("insert into auto.vehicles_delta values (2, 'Chevy')")
  }

  test("testGoodConfig"){
    val p: Path = Files.createTempFile("TestMamboMain", null ) ;
    Files.write(p,"application {name=test\nmaster=local\n}\nsteps{}".getBytes()) ;
    p.toFile.deleteOnExit()
    val conf: Array[String] = new Array[String](1)
    conf(0) = p.toString
    MamboMain.main(conf)
  }

  test("testGoodExample"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/file-ingest-local-csv.conf"
    conf(1) = "examples/environment.conf"
    MamboMain.main(conf)
  }

  test("testGenerateData"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/generate-data.conf"
    conf(1) = "examples/environment.conf"
    MamboMain.main(conf)
  }

  test("testRemoteHttpCsv"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/file-ingest-remote-csv.conf"
    conf(1) = "examples/environment.conf"
    MamboMain.main(conf)
  }

  test("testRdbmsExample"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/rdbms-ingest.conf"
    conf(1) = "examples/environment.conf"
    MamboMain.main(conf)
  }

  test("testRdbmsIngestAndDistribute"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/rdbms-ingest-and-distribute.conf"
    conf(1) = "examples/environment.conf"
    MamboMain.main(conf)

  }

  test("testExecuteSqlEvaluationExample"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/execute-sql-evaluation-example.conf"
    conf(1) = "examples/environment.conf"
    MamboMain.main(conf)
  }

  test("testExecuteSqlEvaluationFailExample"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/execute-sql-evaluation-fail-example.conf"
    conf(1) = "examples/environment.conf"

    try{
      MamboMain.main(conf)
    } catch {
      case e: Exception =>
        assert(e.getMessage == "ExecuteSqlEvaluation query (select if(count(*) " +
          "!= 2, 'pass', 'fail') as result from vehicles) failed the evaluation by returning fail")
    }
  }

  test("testExecuteCommand"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/execute-command-example.conf"
    conf(1) = "examples/environment.conf"
    MamboMain.main(conf)
  }

  test("testExecuteCdc"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/rdbms-ingest-cdc.conf"
    conf(1) = "examples/environment.conf"
    MamboMain.main(conf)
  }


}
