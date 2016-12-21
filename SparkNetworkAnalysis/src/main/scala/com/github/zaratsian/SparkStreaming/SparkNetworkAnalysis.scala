

/**********************************************************************************************************************

Usage (Docker):
/apache-maven-3.3.9/bin/mvn clean package
/spark/bin/spark-submit --master local[*] --class "SparkNetworkAnalysis" --jars /phoenix-spark-4.8.1-HBase-1.1.jar target/SparkStreaming-0.0.1.jar phoenix.dev:2181 mytestgroup dztopic1 1 kafka.dev:9092


Kafka Stream - Here's an example msg:
24.93.67.200|United States|NC|Charlotte|-80.8439|35.2277|Time Warner Cable|Time Warner Cable|4|80.1|10.1

**********************************************************************************************************************/

import java.util.HashMap
import java.util.Arrays
import java.sql.DriverManager
import scala.collection.mutable.ListBuffer

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,FloatType}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.HashPartitioner

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import _root_.kafka.serializer.StringDecoder

import org.apache.phoenix.spark._

object SparkNetworkAnalysis {

   def main(args: Array[String]) {
      if (args.length < 5) {
         System.err.println("Usage: SparkNetworkAnalysis <zkQuorum> <group> <topics> <numThreads> <kafkabroker>")
         System.exit(1)
      }

      val batchIntervalSeconds = 10              // 10  seconds
      val slidingInterval = Duration(2000L)      // 2   seconds
      val windowSize = Duration(10000L)          // 10  seconds
      val checkpointInterval = Duration(120000L) // 120 seconds

      val Array(zkQuorum, group, topics, numThreads, kafkabroker) = args
      val sparkConf = new SparkConf().setAppName("cxStream")
      val sc  = new SparkContext(sparkConf)
      val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
      ssc.checkpoint(".")

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Approach 2: Kafka Direct Approach
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkabroker)
      val events = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)

      /********************************************************************************************
      *
      *  Parse each Kafka (DStream)
      *
      *********************************************************************************************/

      //INPUT:  ip_address|country|region|city|longitude|latitude|isp|org|level|signal_strength|signal_noise
      //OUTPUT: ip_address|country|region|city|longitude|latitude|isp|org|level|signal_strength|signal_noise|device_health

      val event = events.map(_.split("\\|")).map(p =>   
          (p(0), (p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8).toInt,p(9).toFloat,p(10).toFloat,

          // Add default value of 0 for device health (meaning health is ok). Device health status will be calculated below. 
          0
          ))
      )


      /******************************************************************
      *  Write Enriched Household Records to HBase via Phoenix
      *******************************************************************/

      event.map(x => x._2 ).print()

      event.map(x => x._2 ).foreachRDD { rdd =>
            rdd.foreachPartition { rddpartition =>
                //val thinUrl = "jdbc:phoenix:thin:url=http://phoenix.dev:8765;serialization=PROTOBUF"
                val thinUrl = "jdbc:phoenix:phoenix.dev:2181:/hbase"
                val conn = DriverManager.getConnection(thinUrl)
                rddpartition.foreach { record =>
                     conn.createStatement().execute("upsert into DEVICE_INFO values ('" + record._1 + "', '" + record._2 + "', '" + record._3 + "', '" + record._4 + "', '" + record._5 + "', '" + record._6 + "', '" + record._7 + "', '" + record._8 + "', " + record._9 + ", " + record._10 + ", " + record._11 + ", " + record._12 + ")" )
                     
                     //conn.createStatement().execute("upsert into DEVICE_INFO (IP,COUNTRY,REGION,CITY,LONGITUDE,LATITUDE,ISP,ORG,LEVEL,SIGNAL_STRENGTH,SIGNAL_NOISE,DEVICE_HEALTH) values ('" + record._1 + "', '" + record._2 + "', '" + record._3 + "', '" + record._4 + "', '" + record._5 + "', '" + record._6 + "', '" + record._7 + "', '" + record._8 + "', " + record._9 + ", " + record._10 + ", " + record._11 + ", " + record._12 + ")" )
                }
                conn.commit()
            }
      }


      /********************************************************************************************
      *
      *  Join DStream (Data from Kafka) with Static Data (Topology Map)
      *
      *********************************************************************************************/
      
      val topology_map = sc.textFile("/traceroute_google_mapped.txt").map(x => x.split("\\|")).map(x => (x(0).toString, (x) ) ).cache()
      
      /********************************************************************************************
      *  
      *  State Mapping Function
      *
      *********************************************************************************************/
      
      // Key:    ip_address
      // Value:  ip_address, signal_strength, signal_noise, device_health
      // State:  ip_address, avg3_signal_strength, signal_strength_array, avg3_signal_noise, signal_noise_array, device_health
      // Output: ip_address, 
      
      def trackStateFunc(batchTime: Time, key: String, value: Option[(String, Float, Float, Int)], state: State[(String, Float, ListBuffer[Float], Float, ListBuffer[Float], Int)] ): Option[(String, Float, ListBuffer[Float], Float, ListBuffer[Float], Int)] = {
         
         val currentState = state.getOption.getOrElse( ("ip_address", (99.0).toFloat, ListBuffer[Float](), (1.0).toFloat, ListBuffer[Float](), 0) )
         val signal_strength_array = currentState._3
         val signal_noise_array    = currentState._5
         
         // Calculate avg for last 3 signal strength readings:
         signal_strength_array += value.get._2.toFloat
         if (signal_strength_array.length > 3) { signal_strength_array -= signal_strength_array(0) }
         
         // Calculate avg for last 3 signal noise readings:
         signal_noise_array += value.get._3.toFloat
         if (signal_noise_array.length > 3) { signal_noise_array -= signal_noise_array(0) }
         
         val avg3_signal_strength = signal_strength_array.sum / signal_strength_array.length
         val avg3_signal_noise    = signal_noise_array.sum / signal_noise_array.length
         
         // Calculate device health status based on avg signal strength and avg noise (for the past 3 signals):
         val device_health = if (avg3_signal_strength < 25 && avg3_signal_noise > 65) 1 else 0
         
         val output = (value.get._1, avg3_signal_strength, signal_strength_array, avg3_signal_noise, signal_noise_array, device_health) 
         state.update(output)
         Some(output)
      }
      
      val stateSpec = StateSpec.function(trackStateFunc _)
                         //.initialState(initialRDD)
                         //.numPartitions(2)
                         //.timeout(Seconds(60))
      
      /********************************************************************************************
      *  
      *  Level 1
      *
      *********************************************************************************************/      
      
      // (ip_address, (ip_address, signal_strength, signal_noise, device_health))
      val eventlevel1 = event.map(x => (x._1, (x._1, x._2._10, x._2._11, x._2._12)) )
      
      val eventStateLevel1 = eventlevel1.mapWithState(stateSpec)
      eventStateLevel1.print()

      /*********************************************************
      *  Level 1 - Write State SnapShot to Phoenix
      **********************************************************/

      eventStateLevel1.foreachRDD { rdd =>
            rdd.foreachPartition { rddpartition =>
                val thinUrl = "jdbc:phoenix:phoenix.dev:2181:/hbase"
                val conn = DriverManager.getConnection(thinUrl)
                rddpartition.foreach { record =>
                     conn.createStatement().execute("upsert into DEVICE_INFO (IP,HEALTH_STATUS) values ('" + record._1 + "', " + record._6 + ")" )
                     //conn.createStatement().execute("UPSERT INTO DEVICE_INFO (IP,HEALTH_STATUS) VALUES ('192.168.0.1',1)")
                }
                conn.commit()
            }
      }

      /********************************************************************************************
      *  
      *  LEVEL 2
      *
      *********************************************************************************************/
/*
      val eventLevel2 = eventStateSnapshot1.map(x => (x._2._2._6, (x._2._2._6, x._2._2._2, x._2._2._3, x._2._2._4, x._1, x._2._2._7(2), x._2._2._7, 1 )) )

      val eventStateLevel2 = eventLevel2.mapWithState(stateSpec)

      // Snapshot of the state for the current batch - This DStream contains one entry per key.
      val eventStateSnapshot2 = eventStateLevel2.stateSnapshots()
*/
      /*********************************************************
      *  Level 2 - Write State SnapShot to Phoenix
      **********************************************************/
/*
      eventStateSnapshot2.map(x => (x._2._2._9, x._2._2._2, x._2._2._8) ).foreachRDD { rdd =>
            rdd.foreachPartition { rddpartition =>
                //val thinUrl = "jdbc:phoenix:thin:url=http://phoenix.dev:8765;serialization=PROTOBUF"
                val thinUrl = "jdbc:phoenix:phoenix.dev:2181:/hbase"
                val conn = DriverManager.getConnection(thinUrl)
                rddpartition.foreach { record =>
                     conn.createStatement().execute("upsert into CX_LOOKUP (ID,MER_FLAG,TOPOLOGY_LEVEL) values ('" + record._1 + "', " + record._2 + ", " + record._3 + ")" )
                }
                conn.commit()
            }
      }
*/


      ssc.start()
      ssc.awaitTermination()
   
   }

}


//ZEND
