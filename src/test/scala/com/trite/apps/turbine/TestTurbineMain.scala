package com.trite.apps.turbine

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import java.nio.file.{Files, Path}

class TestTurbineMain extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  test("testGoodConfig"){
    val p: Path = Files.createTempFile("TestEnvelopeMain", null ) ;
    Files.write(p,"application {name=test\nmaster=local\n}\nsteps{}".getBytes()) ;
    p.toFile.deleteOnExit()
    val conf: Array[String] = new Array[String](1)
    conf(0) = p.toString
    TurbineMain.main(conf)
  }

  test("testGoodExample"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/file-ingest-local-csv.conf"
    conf(1) = "examples/environment.conf"
    TurbineMain.main(conf)
  }

  test("testRemoteHttpCsv"){
    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/file-ingest-remote-csv.conf"
    conf(1) = "examples/environment.conf"
    TurbineMain.main(conf)
  }

  test("testRdbmsExample"){
    import java.sql.DriverManager
    val jdbcUrl = "jdbc:h2:mem:test;INIT=CREATE SCHEMA IF NOT EXISTS auto"

    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(jdbcUrl, "sa", "sa")
    conn.createStatement().execute("create table auto.vehicles (id int, make varchar(50))")
    conn.createStatement().execute("insert into auto.vehicles values (1, 'Ford')")
    conn.createStatement().execute("insert into auto.vehicles values (2, 'Chevrolet')")

    val conf: Array[String] = new Array[String](2)
    conf(0) = "examples/rdbms-ingest.conf"
    conf(1) = "examples/environment.conf"
    TurbineMain.main(conf)
  }
}
