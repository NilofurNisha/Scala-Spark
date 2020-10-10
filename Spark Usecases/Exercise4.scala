//Task 7
package sparkex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ex04 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Ex04").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/hive/data/txns")
    val rdd1 = rdd.map(x => x.split(","))
    
    //totalsales by state wise and sort by state
    val rdd2 = rdd1.map(x => ((x(7)),(x(3).toFloat,1)))
    val rdd3 = rdd2.reduceByKey((a,b) => ((a._1,a._2 + b._2)))
    val rdd4 = rdd3.sortByKey()
    rdd4.foreach(println)
    
    //transactioncount by date and sort by transactioncount
    val rdd5 = rdd1.map(x => (x(1),1))
    val rdd6 = rdd5.reduceByKey((a,b) => (a + b))
    val rdd7 = rdd6.sortByKey()
    rdd7.foreach(println)
    
    //the date having maximum transactioncount
    val rdd8 = rdd6.sortByKey(false,1)
    rdd8.foreach(println)  
    
  }
}
