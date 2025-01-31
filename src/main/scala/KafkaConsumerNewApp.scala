import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, from_json, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object KafkaConsumerNewApp extends App {
  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("POC")
      .setMaster("local[2]")
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    import spark.implicits._

    // Define schema for Kafka messages
    val schema = StructType(List(
      StructField("EmpId", IntegerType, true),
      StructField("EmpName", StringType, true),
      StructField("DeptName", StringType, true),
      StructField("Salary", IntegerType, true)
    ))

    // Read department table from MySQL
    val deptTable = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mydb")
      .option("dbtable", "department")
      .option("user", "root")
      .option("password", "rootpassword")
      .load()
      .select(col("id").alias("DeptId"), col("name").alias("DeptName"))

    // Read Kafka stream
    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("auto.offset.reset", "latest")
      .option("subscribe", "kafkaTopic")
      .load()

    // Parse Kafka JSON messages
    val FlattenKafkaStream = kafkaStream.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), schema).alias("tmp"))
      .select("tmp.*")

    // Join streamed data with department table to fetch Department ID
    val joinedStream = FlattenKafkaStream
      .join(deptTable, Seq("DeptName"), "inner") // Ensures only valid departments are considered
      .select(col("DeptId"), col("Salary"))

    // Aggregate data by Department ID
    val aggregation = joinedStream
      .groupBy(col("DeptId"))
      .agg(sum("Salary").alias("TotalSalary"))

    // Save to MySQL (MySQL will auto-generate the primary key)
    def saveToMySql = (df: Dataset[Row], batchId: Long) => {
      val url = "jdbc:mysql://localhost:3306/mydb"

      df
        .withColumn("updated_at", current_timestamp()) // Timestamp column
        .write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", "aggregated_data") // Auto-increment primary key in MySQL
        .option("user", "root")
        .option("password", "rootpassword")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("rewriteBatchedStatements", "true")
        .option("batchsize", "10000")
        .mode("append") // Ensures each batch appends new records
        .save()
    }

    // Write the stream to MySQL
    val kafkaToMySql = aggregation
      .writeStream
      .outputMode("complete") // Ensures all data is stored correctly
      .foreachBatch(saveToMySql)
      .start()
      .awaitTermination()
  }
}
