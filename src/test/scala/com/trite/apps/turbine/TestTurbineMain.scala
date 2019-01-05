package com.trite.apps.turbine

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import java.nio.file.{Files, Path}

import com.trite.apps.turbine
import com.trite.apps.turbine.TurbineMain

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
    conf(0) = "examples/file-ingest.conf"
    conf(1) = "examples/environment.conf"
    TurbineMain.main(conf)
  }
}
