import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

class TestBase  extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val sparkConf = new SparkConf()
  val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .master(master="local[2]")
    .getOrCreate()
}
