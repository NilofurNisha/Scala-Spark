//Task 5

package sparkex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ex02 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex01").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/hive/data/txns")
    val rdd1 = rdd.map(x => x.split(","))
    
    val rdd2  = rdd1.filter(x => x(7) == "Texas")
    
    //Total Sales  by payment type
    val rdd3 = rdd2.map(x => (x(8),(x(3).toFloat,1)))
    val rdd4 = rdd3.reduceByKey((a,b) => (a._1 + b._1,a._2 + b._2))
    val rdd5 = rdd4.sortByKey()
    
    println("Total Sales  by payment type")
    rdd5.foreach(println)
   
    //Sum of sales for texas state
    val rdd6 = rdd2.map(x => (x(3).toFloat))
    val rdd7 = rdd6.sum().round
    println("Sum of sales for texas state")
    println(rdd7)
    
    //Max number of product sales in texas
    val rdd9 = rdd2.map(x => (x(5),1))  
    val rdd10 = rdd9.reduceByKey((a,b) => (a + b))
    val rdd11 = rdd10.sortBy(_._2,false)
    println("Max number of product sales in texas")
    rdd11.foreach(println)
  }

}
