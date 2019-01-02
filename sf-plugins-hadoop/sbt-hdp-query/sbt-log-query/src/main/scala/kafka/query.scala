package maplelabs.org.hadoop

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object KafkaQuery{
    def main(args: Array[String]){

        val spark = SparkSession
             .builder()
             .appName("Kafka Query")
             .master("yarn")
             .config("spark.sql.orc.impl", "native")
             .config("spark.sql.streaming.fileSink.log.cleanupDelay", 60000)
             .getOrCreate()

        val defaultFSPath = spark.sparkContext.hadoopConfiguration.get("fs.defaultFS")

        val startTime = args(0).toLong
        val endTime = args(1).toLong
        val tagName = args(2)
        val plugin = args(3)
        val logLevel = args(4)
        val docType = args(5)
        val fileName = args(6)
        val inputDirectory = args(7)
        val year = args(7)
        val month = args(8)
        val days = args(9)

        import spark.implicits._

        // Year, month, and day assume the Hadoop wildcard format, with {26, 27} for example being used.

        val values = spark.read.format("orc").load(defaultFSPath + inputDirectory + "/" + year + "/" + month + "/" + days + "/")

        values.printSchema

        var decoded = values.select("data.time", "data._documentType", "data.message", "data._tag_appName", "data._plugin", "data.level")
                      .withColumn("timestamp", $"time".cast("long"))
                      .filter($"timestamp" > startTime)
                      .filter($"timestamp" < endTime)
                      .filter($"_tag_appName" === tagName)
                      .filter($"_plugin" === plugin)
                      .filter($"level" === logLevel)

        if (docType != "*") {
            decoded = decoded.filter($"_documentType" === docType)
        }
        
        val indexed = decoded.withColumn("index", row_number().over(Window.orderBy("timestamp")))

        indexed.show(10)
        
        indexed.write.mode(SaveMode.Overwrite).format("orc").save(defaultFSPath + fileName)

    }
}
