package sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import java.util.Properties

//Socket Text Stream
object lab04 extends App{
  
  //step-1
    val spark = SparkSession.builder().appName("Streaming-lab04").master("local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    
    //Step-2 create dstream
    val dstr = ssc.socketTextStream("localhost",9412)
    dstr.print()
    val dstr1 = dstr.map(x => x.split(","))
    val dstr2 = dstr1.map(x => (x(7),(x(3).toFloat,1)))
    val dstr3 = dstr2.reduceByKey((a,b) => (a._1 + b._1,a._2 + b._2))
    val dstr4 = dstr3.map(x => x._1 + "," + x._2._1 + "," + x._2._2)
    
    dstr4.foreachRDD(rdd =>
      {
        import spark.implicits._
        if (!rdd.isEmpty())
        {
          val df = rdd.toDF("State","Salesamt","transcount")
          df.show
          val prop = new Properties()
          prop.put("user","root")
          prop.put("password","root")
          prop.put("driver","com.mysql.jdbc.Driver")
          df.write.mode("append").jdbc("jdbc:mysql://localhost/retail","streamtrans",prop)
          println("Data written into MysqlDB")
        }
      })
    
    
}
