

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
      *  Parse each Kafka (DStream) and enrich raw events
      *
      *********************************************************************************************/

      //INPUT:  ip_address|country|region|city|longitude|latitude|isp|level|org|signal_strength|signal_noise
      //OUTPUT: ip_address|country|region|city|longitude|latitude|isp|level|org|signal_strength|signal_noise|device_health

      val event = events.map(_.split("\\|")).map(p =>   
          (p(0), (p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8).toInt,p(9).toFloat,p(10).toFloat,

          // Rule #1: Bad Node/Device
          // If signal_strength is less than 25 AND signal noise is greater than 65, then device_health = 1 (1 = bad health)
          if ( (p(9).toFloat < 25) && (p(10).toFloat > 65) ) 1 else 0
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
                     
                     //conn.createStatement().execute("upsert into DEVICE_TOPOLOGY (IP,COUNTRY,REGION,CITY,LONGITUDE,LATITUDE,ISP,ORG,LEVEL,SIGNAL_STRENGTH,SIGNAL_NOISE,DEVICE_HEALTH) values ('" + record._1 + "', '" + record._2 + "', '" + record._3 + "', '" + record._4 + "', '" + record._5 + "', '" + record._6 + "', '" + record._7 + "', '" + record._8 + "', " + record._9 + ", " + record._10 + ", " + record._11 + ", " + record._12 + ")" )
                }
                conn.commit()
            }
      }


      /********************************************************************************************
      *
      *  Join DStream (Data from Kafka) with Static Data (Topology Map)
      *
      *********************************************************************************************/
/*
      val partitioner = new HashPartitioner(2)

      val topology_map = sc.textFile("/traceroute_google_mapped.txt").map(x => x.split("\\|")).map(x => (x(0).toString, (x) ) ).cache()

      // Data for join:
      // traceroute_google_mapped.txt: localhost|ipaddress2|ipaddress3|ipaddress4|...
      // event:                        ip_address|country|region|city|longitude|latitude|isp|org|level|signal_strength|signal_noise|device_health

      val eventJoined= event.transform(rdd => topology_map.join(rdd, partitioner))

      eventJoined.print()
      // (localhost,([Ljava.lang.String;@6ae20f43,(localhost,United States,NC,Raleigh,-78.6382,35.7796,Time Warner Cable,Time Warner Cable,1,80.1,10.1,0)))
*/

      /********************************************************************************************
      *  
      *  LEVEL 0
      *
      *********************************************************************************************/
/*
      val eventLevel0 = eventJoined.map(x => (x._1, (x._1, x._2._2._12.toDouble, 1.0, 0.0, x._2._2._1, x._2._1(1), x._2._1, x._2._2._9.toInt  )) )

      // INPUTS: (DEVICE_OF_INTEREST, (DEVICE_OF_INTEREST, DEVICE_HEALTH, CHILD_NODE_COUNT, BAD_CHILD_NODE_COUNT, CHILD_DEVICE, PARENT_DEVICE, DEVICE_TOPOLOGY, TOPOLOGY_LEVEL)) 
      // OUTPUT: (DEVICE_OF_INTEREST, (DEVICE_OF_INTEREST, DEVICE_HEALTH, CHILD_NODE_COUNT, BAD_CHILD_NODE_COUNT, CHILD_DEVICE, PARENT_DEVICE, DEVICE_TOPOLOGY, TOPOLOGY_LEVEL))
    
      def trackStateFunc(batchTime: Time, key: String, value: Option[(String, Double, Double, Double, String, String, Array[String], Int)], state: State[(Map[String,Double],(String,Double,Double,Double,String,String,Array[String],Int))] ): Option[(String, (String, Double, Double, Double, String, String, Array[String], Int))] = {
         
          val currentState = state.getOption.getOrElse( (Map("aaa" -> 0.0), ("key", 0.0, 0.0, 0.0, "previous_key", "next_key", Array("temp"), 0 )) )
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
                  state.update( (seen_devices + (previous_device -> value.get._2) , (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8)) )
                  val output = (key, (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8))
                  Some(output)
              } else {
                  val sum = currentState._2._3 + value.get._3
                  val red = currentState._2._4 + value.get._4
                  val red_percentage = red / sum
                  val device_mer_flag = if (red_percentage >= 0.50) 1.0 else 0.0
                  state.update( (seen_devices + (previous_device -> value.get._2) , (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8)) )
                  val output = (key, (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8))
                  Some(output)
              }
          } else {
              val sum = value.get._3
              //val red = value.get._2
              val red = if (value.get._8 == 0) value.get._2 else value.get._4
              val red_percentage = red / sum
              val device_mer_flag = if (red_percentage >= 0.50) 1.0 else 0.0
              state.update( ( Map(previous_device -> value.get._2) , (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8)) )
              val output = (key, (key, device_mer_flag, sum, red, value.get._5, value.get._6, value.get._7, value.get._8))
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
*/

      /*********************************************************
      *  Write State SnapShot to Phoenix
      **********************************************************/
/*
      eventStateSnapshot0.map(x => (x._2._2._1, x._2._2._2, x._2._2._6, x._2._2._7.mkString("|")) ).print()
      // (localhost,0.0,174.111.102.226,localhost|174.111.102.226|24.25.62.50|24.93.64.186|24.93.67.202|66.109.6.82|205.197.180.41|205.197.180.54|216.239.51.53|64.233.175.94|216.58.193.142) 

      eventStateSnapshot0.map(x => (x._2._2._1, x._2._2._2, x._2._2._6, x._2._2._7.mkString("|") )).foreachRDD { rdd =>
            rdd.foreachPartition { rddpartition =>
                //val thinUrl = "jdbc:phoenix:thin:url=http://phoenix.dev:8765;serialization=PROTOBUF"
                val thinUrl = "jdbc:phoenix:phoenix.dev:2181:/hbase"
                val conn = DriverManager.getConnection(thinUrl)
                rddpartition.foreach { record =>
                     conn.createStatement().execute("upsert into DEVICE_TOPOLOGY (IP,DEVICE_HEALTH,UPSTREAM_DEVICE,NODE_PATH) values ('" + record._1 + "', " + record._2 + ", '" + record._3 + "', '" + record._4 + "')" )
                     //conn.createStatement().execute("UPSERT INTO CX_LOOKUP VALUES('mydevice2',1.0,'next_device2',0)")
                }
                conn.commit()
            }
      }
*/
      /********************************************************************************************
      *  
      *  LEVEL 1
      *
      *********************************************************************************************/
/*
      val eventLevel1 = eventStateSnapshot0.map(x => (x._2._2._6, (x._2._2._6, x._2._2._2, x._2._2._3, x._2._2._4, x._1, x._2._2._7(2), x._2._2._7, 1 )) )

      val eventStateLevel1 = eventLevel1.mapWithState(stateSpec)
      eventStateLevel1.print()

      // Snapshot of the state for the current batch - This DStream contains one entry per key.
      val eventStateSnapshot1 = eventStateLevel1.stateSnapshots()
      eventStateSnapshot1.print(10)
*/
      /*********************************************************
      *  Write State SnapShot to Phoenix
      **********************************************************/
/*
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

*/
      /********************************************************************************************
      *
      *  LEVEL 2
      *
      *********************************************************************************************/
/*
      val eventLevel2 = eventStateSnapshot1.map(x => (x._2._2._6, (x._2._2._6, x._2._2._2, x._2._2._3, x._2._2._4, x._1, x._2._2._7(3), x._2._2._7, 2, x._2._2._7(0) )) )

      val eventStateLevel2 = eventLevel2.mapWithState(stateSpec)
      eventStateLevel2.print()

      // Snapshot of the state for the current batch - This DStream contains one entry per key.
      val eventStateSnapshot2 = eventStateLevel2.stateSnapshots()
      eventStateSnapshot2.print(10)
*/
      /*********************************************************
      *  Write State SnapShot to Phoenix
      **********************************************************/
/*
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
*/


      ssc.start()
      ssc.awaitTermination()
   
   }

}


//ZEND
