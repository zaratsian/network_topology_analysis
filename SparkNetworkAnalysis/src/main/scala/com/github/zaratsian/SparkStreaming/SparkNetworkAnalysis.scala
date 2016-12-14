

/**********************************************************************************************************************

Input Data source = Kafka stream as a pipe-delimited string
Here's an examples input event:
"000E5XE99XD8|895|85086090|D|1|Y|2150250860|250860|PM007|34.2|48.7|-12.3|26-OCT-16 13:02:27|06676XJ98908|PTMOXMTK98|220|DATA_D2D1|DGI5120|201461|2.0531|0|6.4|-17.0982|-25.4403|4"

Usage:
spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 --jars /usr/hdp/current/phoenix-client/phoenix-client.jar --class "cxStream" --master yarn-client ./target/SparkStreaming-0.0.1.jar sandbox.hortonworks.com:2181 mytestgroup dztopic1 1

Usage (Docker):
/apache-maven-3.3.9/bin/mvn clean package
/spark/bin/spark-submit --master local[*] --class "cxStream" --jars /phoenix-spark-4.8.1-HBase-1.1.jar target/SparkStreaming-0.0.1.jar phoenix.dev:2181 mytestgroup dztopic1 1 kafka.dev:9092


Create HBase / Phoenix table:

echo "CREATE TABLE IF NOT EXISTS CX_TOPOLOGY (
DEVICE_OF_INTEREST CHAR(30) NOT NULL PRIMARY KEY,
MER_FLAG FLOAT,
NEXT_DEVICE_OF_INTEREST CHAR(30),  
TOPOLOGY_LEVEL INTEGER);" > /tmp/create_topology.sql

/usr/hdp/current/phoenix-client/bin/sqlline.py localhost:2181:/hbase-unsecure /tmp/create_topology.sql 

**********************************************************************************************************************/


import java.util.HashMap
import java.util.Arrays
import java.sql.DriverManager

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import _root_.kafka.serializer.StringDecoder

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,FloatType}

import org.apache.phoenix.spark._

import org.apache.spark.HashPartitioner


object SparkNetworkAnalysis {

   case class stateCase(sum: Double, red: Double, seen_keys: List[String])

   def main(args: Array[String]) {
      if (args.length < 5) {
         System.err.println("Usage: cxStream <zkQuorum> <group> <topics> <numThreads> <kafkabroker>")
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
      //val sc = ssc.sparkContext
      ssc.checkpoint(".")

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Approach 1: Kafka Receiver-based Approach (http://spark.apache.org/docs/1.6.0/streaming-kafka-integration.html)
      //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      //val events = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

      // Approach 2: Kafka Direct Approach
      val topicsSet = topics.split(",").toSet
      //val kafkaParams = Map[String, String]("metadata.broker.list" -> "sandbox.hortonworks.com:6667")
      val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkabroker)
      val events = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)

      /********************************************************************************************
      *
      *  Parse each Kafka (DStream) and enrich raw events
      *
      *********************************************************************************************/

      val event = events.map(_.split("\\|")).map(p =>   
          (p(0), (p(0),p(1),p(2).toInt,

          // Rule #1: Bad Node/Device
          if ( (p(2).toFloat < 25 ) ) 1 else 0
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
                     conn.createStatement().execute("upsert into CX_HOUSEHOLD_INFO values ('" + record._1 + "', '" + record._2 + "', '" + record._3 + "', '" + record._4 + "', " + record._5 + ", '" + record._6 + "', " + record._7 + ", " + record._8 + ", " + record._9 + ", " + record._10 + ", " + record._11 + ", " + record._12 + ", " + record._13 + ")" )
                     //conn.createStatement().execute("UPSERT INTO CX_TOPOLOGY VALUES('mydevice2',1.0,'next_device2',0)")
                }
                conn.commit()
            }
      } 


      /********************************************************************************************
      *
      *  Join DStream (Data from Kafka) with Static Data (Topology Map)
      *
      *********************************************************************************************/
      val partitioner = new HashPartitioner(2)

      val topology_map = sc.textFile("/topology_mapped.csv").map(x => x.split(",")).map(x => (x(1).toString, (x) ) ).cache()

      val eventJoined= event.transform(rdd => topology_map.join(rdd, partitioner))


      /********************************************************************************************
      *  
      *  LEVEL 0
      *
      *********************************************************************************************/
 
      val eventLevel0 = eventJoined.map(x => (x._1, (x._1, x._2._2._13.toDouble, 1.0, 0.0, x._2._2._1, x._2._1(2), x._2._1, 0, x._2._1(0))) )


      // INPUTS: (DEVICE_OF_INTEREST, (DEVICE_OF_INTEREST, MER_FLAG, SUM, RED, PREVIOUS_DEVICE, NEXT_DEVICE_OF_INTEREST, DEVICE_TOPOLOGY, TOPOLOGY_LEVEL, MAC) 
      // OUTPUT: (DEVICE_OF_INTEREST, (DEVICE_OF_INTEREST, MER_FLAG, SUM, RED, PREVIOUS_DEVICE, NEXT_DEVICE_OF_INTEREST, DEVICE_TOPOLOGY, TOPOLOGY_LEVEL, MAC))
    
      def trackStateFunc(batchTime: Time, key: String, value: Option[(String, Double, Double, Double, String, String, Array[String], Int, String)], state: State[(Map[String,Double],(String,Double,Double,Double,String,String,Array[String],Int,String))] ): Option[(String, (String, Double, Double, Double, String, String, Array[String], Int, String))] = {
         
          val currentState = state.getOption.getOrElse( (Map("aaa" -> 0.0), ("key", 0.0, 0.0, 0.0, "previous_key", "next_key", Array("temp"), 0, "mac")) )
          val seen_devices = currentState._1
          val previous_device = value.get._5

          if (state.exists()) {
              if (seen_devices.contains(previous_device)) {
                  val sum = currentState._2._3

                  val red_old_value = seen_devices.get(previous_device).get.toDouble
                  val red_new_value = value.get._2
                  
                  val red = if (red_old_value == red_new_value) currentState._2._4 else if (red_old_value > red_new_value) currentState._2._4 - 1.0 else currentState._2._4 + 1.0

                  val red_percentage = red / sum
                  
                  val device_mer_flag = if (red_percentage >= 0.50) 1.0 else 0.0
                  state.update( (seen_devices + (previous_device -> value.get._2) , (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8, value.get._9)) )
                  val output = (key, (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8, value.get._9))
                  Some(output)
              } else {
                  val sum = currentState._2._3 + value.get._3
                  val red = currentState._2._4 + value.get._4
                  val red_percentage = red / sum
                  val device_mer_flag = if (red_percentage >= 0.50) 1.0 else 0.0
                  state.update( (seen_devices + (previous_device -> value.get._2) , (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8, value.get._9)) )
                  val output = (key, (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8, value.get._9))
                  Some(output)
              }
          } else {
              val sum = value.get._3
              //val red = value.get._2
              val red = if (value.get._8 == 0) value.get._2 else value.get._4
              val red_percentage = red / sum
              val device_mer_flag = if (red_percentage >= 0.50) 1.0 else 0.0
              state.update( ( Map(previous_device -> value.get._2) , (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8, value.get._9)) )
              val output = (key, (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8, value.get._9))
              Some(output)
          }

      }

      val initialRDD = sc.parallelize( Array((0.0, (0.0, List("xyz")))) )
 
      val stateSpec = StateSpec.function(trackStateFunc _)
                         //.initialState(initialRDD)
                         //.numPartitions(2)
                         //.timeout(Seconds(60))

      val eventStateLevel0 = eventLevel0.mapWithState(stateSpec)
      eventStateLevel0.print()

      // Snapshot of the state for the current batch - This DStream contains one entry per key.
      val eventStateSnapshot0 = eventStateLevel0.stateSnapshots() 
      eventStateSnapshot0.print(10)


      /*********************************************************
      *  Write State SnapShot to Phoenix
      **********************************************************/

      eventStateSnapshot0.map(x => (x._2._2._1, x._2._2._2, x._2._2._6, x._2._2._8, x._2._2._9) ).print(15)
      
      eventStateSnapshot0.map(x => (x._2._2._9, x._2._2._2, x._2._2._8) ).foreachRDD { rdd =>
            rdd.foreachPartition { rddpartition =>
                //val thinUrl = "jdbc:phoenix:thin:url=http://phoenix.dev:8765;serialization=PROTOBUF"
                val thinUrl = "jdbc:phoenix:phoenix.dev:2181:/hbase"
                val conn = DriverManager.getConnection(thinUrl)
                rddpartition.foreach { record =>
                     conn.createStatement().execute("upsert into CX_LOOKUP (ID,MER_FLAG,TOPOLOGY_LEVEL) values ('" + record._1 + "', " + record._2 + ", " + record._3 + ")" )
                     //conn.createStatement().execute("UPSERT INTO CX_TOPOLOGY VALUES('mydevice2',1.0,'next_device2',0)")
                }
                conn.commit()
            }
      } 


      /********************************************************************************************
      *  
      *  LEVEL 1
      *
      *********************************************************************************************/

      val eventLevel1 = eventStateSnapshot0.map(x => (x._2._2._6, (x._2._2._6, x._2._2._2, x._2._2._3, x._2._2._4, x._1, x._2._2._7(2), x._2._2._7, 1, x._2._2._7(0))) )

      val eventStateLevel1 = eventLevel1.mapWithState(stateSpec)
      eventStateLevel1.print()

      // Snapshot of the state for the current batch - This DStream contains one entry per key.
      val eventStateSnapshot1 = eventStateLevel1.stateSnapshots()
      eventStateSnapshot1.print(10)

      /*********************************************************
      *  Write State SnapShot to Phoenix
      **********************************************************/

      eventStateSnapshot1.map(x => (x._2._2._9, x._2._2._2, x._2._2._8) ).foreachRDD { rdd =>
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


      /********************************************************************************************
      *
      *  LEVEL 2
      *
      *********************************************************************************************/

      val eventLevel2 = eventStateSnapshot1.map(x => (x._2._2._6, (x._2._2._6, x._2._2._2, x._2._2._3, x._2._2._4, x._1, x._2._2._7(3), x._2._2._7, 2, x._2._2._7(0) )) )

      val eventStateLevel2 = eventLevel2.mapWithState(stateSpec)
      eventStateLevel2.print()

      // Snapshot of the state for the current batch - This DStream contains one entry per key.
      val eventStateSnapshot2 = eventStateLevel2.stateSnapshots()
      eventStateSnapshot2.print(10)

      /*********************************************************
      *  Write State SnapShot to Phoenix
      **********************************************************/

      eventStateSnapshot2.map(x => (x._1.split("_")(0) , x._2._2._2, x._2._2._8) ).foreachRDD { rdd =>
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



      ssc.start()
      ssc.awaitTermination()
   
   }

}


//ZEND
