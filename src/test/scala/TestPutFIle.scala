import com.typesafe.config.{Config, ConfigFactory}

import java.util.Properties

class TestPutFIle extends TestBase {

  test("TestPutFile") {
    val props: Properties = new Properties()
    props.setProperty("name", "Generate Vehicle Data")
    props.setProperty("description", "Example that generates vehicle data.")
    props.setProperty("enabled", "true")
    props.setProperty("type", "com.ttne.mambo.processors.PutFile")
    props.setProperty("path", "target/com.ttne.mambo.processors.PutFile/TestPutFile/")
    props.setProperty("format", "parquet")
    props.setProperty("parallelWrites", "10")
    props.setProperty("saveMode", "overwrite")
    props.setProperty("query", "select 'test1', 'test2'")

    val config: Config = ConfigFactory.parseProperties(props)
    val p: PutFile = new PutFile(spark, config)
    p.run()
  }
}
