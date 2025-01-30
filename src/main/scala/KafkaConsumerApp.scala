
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, current_timestamp, from_json, lit}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object KafkaConsumerApp extends App {
  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("POC")
      .setMaster("local[2]")
      .set("spark.sql.streaming.checkpointLocation","checkpoint")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("auto.offset.reset", "latest")
      .option("subscribe", "kafkaTopic")
      .load()

    val schema = StructType(
      List(
        StructField("EmpId", IntegerType, true),
        StructField("EmpName", StringType, true),
        StructField("DeptName", StringType, true),
        StructField("Salary", IntegerType, true)
      )
    )

    val FlattenKafkaStream = kafkaStream.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).alias("tmp"))
      .select("tmp.*")

    val aggregation = FlattenKafkaStream.groupBy(col("DeptName")).agg(sum("Salary").alias("Salary"))

    def saveToMySql = (df: Dataset[Row], batchId: Long) => {
      val url = """jdbc:mysql://localhost:3306/mydb"""

      df
        .withColumn("batchId", lit(batchId))
        .write.format("jdbc")
        .option("url", url)
        .option("dbtable", "kafka")
        .option("user", "root")
        .option("password", "rootpassword")
        .mode("append")
        .save()
    }
    val kafkaToMySql = aggregation
      .writeStream
      .outputMode("complete")
      .foreachBatch(saveToMySql)
      .start()
      .awaitTermination()
  }
}
