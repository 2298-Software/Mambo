import com.typesafe.config.{Config, ConfigFactory}

import java.util.Properties

class TestExecuteSql extends TestBase {

  test("TestExecuteSql") {
    val prop1: Properties = new Properties()
    prop1.setProperty("name", "Generate Vehicle Data")
    prop1.setProperty("description", "Example that generates vehicle data.")
    prop1.setProperty("enabled", "true")
    prop1.setProperty("type", "com.ttne.mambo.processors.ExecuteSql")
    prop1.setProperty("outputType", "memory")
    prop1.setProperty("outputName", "vehicle_ids")
    prop1.setProperty("numRows", "10")
    prop1.setProperty("show", "true")

    val config1: Config = ConfigFactory.parseProperties(prop1)
    val gd: GenerateDataset = new GenerateDataset(spark, config1)
    gd.run()

    val prop2: Properties = new Properties()
    prop2.setProperty("name", "Generate Vehicle Data")
    prop2.setProperty("description", "Example that generates vehicle data.")
    prop2.setProperty("enabled", "true")
    prop2.setProperty("type", "com.ttne.mambo.processors.ExecuteSql")
    prop2.setProperty("outputType", "memory")
    prop2.setProperty("outputName", "vehicle_ids_summary")
    prop2.setProperty("query", "select sum(id) from vehicle_ids")
    prop2.setProperty("show", "true")

    val config2: Config = ConfigFactory.parseProperties(prop2)
    val es: ExecuteSql = new ExecuteSql(spark, config2)
    es.run()

    val df = spark.sql("select * from vehicle_ids_summary")
    assert(df.count() == 1)
  }
}
