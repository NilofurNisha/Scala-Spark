//Task 11
package sparkex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ex12 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex12").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/movies.csv")
    val rdd1 = rdd.map(x => x.split(","))
    
    val rdd2 = rdd1.map(x => (x(1),x(3).toFloat))
    val rdd3 = rdd2.filter(x => (x._2 > 4 || x._2 == 4))
    println("The movies that have rating greater than or equal to 4 is ",rdd3)
    
    val rdd4 = rdd1.map(x => (x(1),x(2).toFloat)).filter(x => (x._2 > 1980))
    println("The movies that are released after 1980 are")
    rdd4.foreach(println)
    
    val rdd5 = rdd1.map(x => (x(1),x(2))).sortBy(_._2)
    println("List of movies released by year")
    rdd5.foreach(println)
    rdd5.saveAsTextFile("file:/home/hduser/movieslist") 
    
  }
}
