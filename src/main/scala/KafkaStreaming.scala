

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SQLContext}

object KafkaStreaming {

 def main(args: Array[String]) {
// val checkpointDir = "popularity-data-checkpoint"
   val processingInterval = "20"
   val inputTopic = "house_test" 
   val zkQuorum = "localhost:2181"
   val group = "spark_stream_topic"
   val inputTopics = "house_test"
   val numThreads = "1"
   val topicMap = inputTopics.split(",").map((_, numThreads.toInt)).toMap

   val conf = new SparkConf().setAppName("KafkaStreaming")
   val ssc = new StreamingContext(conf, Seconds(processingInterval.toInt))
//   ssc.checkpoint("checkpoint")

   val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, group , topicMap).map(x => x._2.split(" ", -1))
   
   case class Data( house_id:Int,household_id: Int,timestamp: String,value:Double)
   val a1 = kafkaStream.map(_._2).map(s => s.split(",")).map(s=> Data(s(0).toInt,s(1).toInt,s(2),s(3).toDouble))
   a1.foreachRDD { rdd =>
   rdd.toDF().registerTempTable("house")
   val house = sqlContext.sql("select house_id,household_id,from_unixtime(timestamp,'dd-MM-YYYY')as date1,hour(timestamp(from_unixtime(timestamp))) as hour1,minute(timestamp(from_unixtime(timestamp))) as minute1,round(float(value),2) as value from house")
   house.coalesce(2)
   .write
   .mode("append")
   .parquet("/data123/scala")
   val d1 = house.where("minute1=59") 
   if(d1.count > 0){
   d1.show()
   val stored = sqlContext.read.parquet("/data123/scala")
   d1.registerTempTable("d1_t1")
   stored.registerTempTable("stored_dt")
   val f1 = sqlContext.sql("select a.house_id,a.household_id,a.date1,a.hour1,round(sum(a.value)) as value from stored_dt a join d1_t1 b on a.house_id = b.house_id and a.household_id = b.household_id and a.date1= b.date1 and a.hour1= b.hour1 group by a.house_id,a.household_id,a.date1,a.hour1")
   val conf = sc.hadoopConfiguration
   val fs = org.apache.hadoop.fs.FileSystem.get(conf)
   val exists = fs.exists(new org.apache.hadoop.fs.Path("/final123/scala/_SUCCESS"))
   if(exists) {
   val l1 = sqlContext.read.parquet("/final123/scala")
   l1.registerTempTable("agg2")
   val temp1 = sqlContext.sql("select house_id,household_id,hour1,avg(value)+stddev(value) as range_value from agg2 group by house_id,household_id,hour1")
   val temp2 = sqlContext.sql("select date1,hour1,avg(value)+stddev(value) as range_value from agg2 group by date1,hour1")
   temp1.registerTempTable("agg1")
   temp2.registerTempTable("agg1_alert2")
    }
    f1.coalesce(1)
   .write
   .mode("append")
   .parquet("/final123/scala")
    f1.registerTempTable("hourly_tab")
    
    if(exists) {    
    val f11 = sqlContext.sql("""select CONCAT(a.house_id,'_',a.household_id,'_',b.date1,'_',a.hour1) as id, case when ( b.value > a.range_value ) 
         then 1 else 0 end as alert_type  
         from agg1 a join hourly_tab b on a.house_id = b.house_id and a.household_id = b.household_id and a.hour1= b.hour1 """)
    val f12 = sqlContext.sql("""select CONCAT(a.house_id,'_',a.household_id,'_',b.date1,'_',a.hour1) as id, case when ( a.value > b.range_value ) 
         then 1 else 0 end as alert_type  
         from agg1_alert2 b join hourly_tab a on a.date1 = b.date1 and a.hour1= b.hour1 """)
    f11.coalesce(1)
   .write
   .mode("append")
   .parquet("/final/result1")

   f12.coalesce(1)
   .write
   .mode("append")
   .parquet("/final/result2")
    }    
  }

}

ssc.start()
ssc.awaitTermination()

    
  }
  
  
}
 
