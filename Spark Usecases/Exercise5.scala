//Task 8
package sparkex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ex05 {
  def main(args:Array[String])=
  {
    val conf = new  SparkConf().setAppName("Ex05").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/spark6/user.csv")
    println("Input data")
    println("==========")
    rdd.foreach(println)
    
    val header = rdd.first()
    
    val rdd1 = rdd.filter(_(0) != header(0))
    println()
    println("Data after removing header")
    println("==========================")
    rdd1.foreach(println)
    
    val rdd2 = rdd1.map(x => x.split(",").toList)
    val rdd3 = rdd2.filter( x => x(0) == "myself")
    println()
    println("Filtering row having id as myself is ")
    rdd3.foreach(println)

    
  }
  
}
