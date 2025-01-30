import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.util.Random

object KafkaProducerApp extends App {
  override def main(args: Array[String]): Unit = {
    val kafkaTopic = "kafkaTopic"
    val bootstrapServers = "localhost:9092"

    // Kafka Producer Configuration
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Generate Sample Data
    val departments = List("HR", "Engineering", "Finance", "Marketing")
    val employees = List("Alice", "Bob", "Charlie", "Diana", "Eve", "Frank")

    def generateRandomMessage: String = {
      val empId = Random.nextInt(1000) + 1
      val empName = employees(Random.nextInt(employees.length))
      val deptName = departments(Random.nextInt(departments.length))
      val salary = Random.nextInt(5000) + 3000

      s"""{
         |  "EmpId": $empId,
         |  "EmpName": "$empName",
         |  "DeptName": "$deptName",
         |  "Salary": $salary
         |}""".stripMargin
    }

    // Send Messages to Kafka Topic
    for (_ <- 1 to 100) { // Adjust the number of messages as needed
      val message = generateRandomMessage
      val record = new ProducerRecord[String, String](kafkaTopic, null, message)
      producer.send(record)
      println(s"Sent: $message")
//      Thread.sleep(500) // Delay between messages (adjust as needed)
    }

    producer.close()
  }
}
