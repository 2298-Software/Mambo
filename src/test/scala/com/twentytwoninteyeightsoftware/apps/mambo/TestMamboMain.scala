package com.twenty298.apps.mambo

import java.io.File

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import java.nio.file.{Files, Path}

import org.slf4j.{Logger, LoggerFactory}

class TestMamboMain extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
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

  def getDefaultConf(confFile: String): Array[String] ={
    Array(confFile,"examples/environment.conf")
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
    MamboMain.main(getDefaultConf("examples/file-ingest-local-csv.conf"))
  }


  test("testGenerateData"){
    MamboMain.main(getDefaultConf("examples/generate-data.conf"))
  }

  test("testRemoteHttpCsv"){
    MamboMain.main(getDefaultConf("examples/file-ingest-remote-csv.conf"))
  }

  test("testRdbmsExample"){
    MamboMain.main(getDefaultConf("examples/rdbms-ingest.conf"))
  }

  test("testRdbmsIngestAndDistribute"){
    MamboMain.main(getDefaultConf("examples/generate-data.conf"))
  }

  test("testExecuteSqlEvaluationExample"){
    MamboMain.main(getDefaultConf("examples/execute-sql-evaluation-example.conf"))
  }

  test("testExecuteSqlEvaluationFailExample"){
    try{
      MamboMain.main(getDefaultConf("examples/execute-sql-evaluation-fail-example.conf"))
    } catch {
      case e: Exception =>
        assert(e.getMessage == "ExecuteSqlEvaluation query (select if(count(*) " +
          "!= 2, 'pass', 'fail') as result from vehicles) failed the evaluation by returning fail")
    }
  }

  test("testExecuteCommand"){
    MamboMain.main(getDefaultConf("examples/execute-command-example.conf"))
  }

  test("testExecuteCdc"){
    MamboMain.main(getDefaultConf("examples/rdbms-ingest-cdc.conf"))
  }


}
