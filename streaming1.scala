package sparkstreaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._

//Text File Stream
object lab01 {
  def main(args:Array[String])=
  {
    //step-1
    val conf = new SparkConf().setAppName("Streaming-labo1").setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(10))
    ssc.sparkContext.setLogLevel("ERROR")
    
    //Step-2 create dstream
    val dstr = ssc.textFileStream("file:/home/hduser/streamdata")
    println("Hello World") //executed by driver prints only once
    dstr.print()
    
    //step-3 
    ssc.start()
    ssc.awaitTermination()
    
  }
}
