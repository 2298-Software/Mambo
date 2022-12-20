import com.typesafe.config.{Config, ConfigFactory}

import java.util.Properties

class TestRunner extends TestBase {

  test("TestGenerateDataset") {
    val props: Properties = new Properties()

    props.setProperty("spark.master","local[2]")
    props.setProperty("spark.name", "Vehicle Data")
    props.setProperty("spark.executor.memory","1g")
    props.setProperty("spark.executor.cores","1")

    props.setProperty("steps.step1.name", "Generate Vehicle Data")
    props.setProperty("steps.step1.description", "Example that generates vehicle data.")
    props.setProperty("steps.step1.enabled", "true")
    props.setProperty("steps.step1.type", "com.ttne.mambo.processors.GenerateDataset")
    props.setProperty("steps.step1.outputType", "memory")
    props.setProperty("steps.step1.outputName", "vehicle_ids")
    props.setProperty("steps.step1.numRows", "10")
    props.setProperty("steps.step1.show", "true")

    val config: Config = ConfigFactory.parseProperties(props)
    val runner: Runner = new Runner()
    runner.run(config)
  }
}
