//Task 9
package sparkex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable._
import org.apache.spark.rdd.RDD

object ex06 {
  def main(args:Array[String])=
  {
    val conf = new  SparkConf().setAppName("Ex06").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/hdpcd/tax.csv")
    
    val header = rdd.first()
    
    val rdd1 = rdd.filter(_(0) != header(0))
    println("Input data")
    rdd1.foreach(println)
    
    val rdd2 = rdd1.map(x => x.split(",").toList).filter( x => x.length == 3)
    println("length filter")
    rdd2.foreach(println)
    val rdd3 = rdd2.map (x => (x(0),x(1).toFloat,x(2).toFloat))
    rdd3.foreach(println)
    val rdd4 = rdd3.map( x => (x._1,x._2,x._3,(x._2 + (x._3 /100) * x._2)))
    rdd4.foreach(println)
  }
}
