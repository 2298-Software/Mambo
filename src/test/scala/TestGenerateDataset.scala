import com.typesafe.config.{Config, ConfigFactory}

import java.util.Properties

class TestGenerateDataset extends TestBase {

  test("TestGenerateDataset") {
    val props: Properties = new Properties()
    props.setProperty("name", "Generate Vehicle Data")
    props.setProperty("description", "Example that generates vehicle data.")
    props.setProperty("enabled", "true")
    props.setProperty("type", "com.ttne.mambo.processors.GenerateDataset")
    props.setProperty("outputType", "memory")
    props.setProperty("outputName", "vehicle_ids")
    props.setProperty("numRows", "10")
    props.setProperty("show", "true")

    val config: Config = ConfigFactory.parseProperties(props)
    val gd: GenerateDataset = new GenerateDataset(spark, config)
    gd.run()

    val df = spark.sql("select * from vehicle_ids")
    assert(df.count() == 10)
  }
}
