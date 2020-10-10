//Task 10
package sparkex

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import java.io._
import java.io.PrintWriter
import scala.io.Source

object ex07 {
  def main(args:Array[String])=
  {
    val conf = new  SparkConf().setAppName("Ex07").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd = sc.textFile("file:/home/hduser/auction.txt")
    val header = rdd.first()
    val rdd1 = rdd.filter(_(0) != header)
    
    val rdd2 = rdd1.map(x => x.split("~"))
    val rdd3 = rdd2.count()
    println("No of auctions ", rdd3)

    val rdd4 = rdd2.filter(x => x.length == 9)
    val rdd5 = rdd4.map( x => ((x(7)),(x(1),1)))
    val rdd6 = rdd5.reduceByKey((a,b) => (a._1 , a._2 + b._2))
    val rdd7 = rdd6.sortByKey()
    println("No of bids per item")
    rdd7.foreach(println)
    val bid = rdd7.collect()
    val lbid = bid.toList
    
    
    val rdd8 = rdd4.map(x => x(1))
    val max = rdd8.max()
    val min = rdd8.min()
    println("Maximum number of bids",max)
    println("Minimum number of bids" , min)
   
    val rdd9 = rdd4.map(x => (x(1).toFloat,1)).reduce((x,y) => (x._1 + y._1, x._2 + y._2))
    val avg = (rdd9._1 /rdd9._2)
    println("Average of bids",avg)
    
    println("==========Auction details=========")
    //writing into file
    val file = new File("/home/hduser/auctions1.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    
    bw.write(s"Totalnumber of auctions - $rdd3")
    bw.newLine()
    bw.write(s"No of bids per Item - $lbid")
    bw.newLine()
    bw.write(s"Maximum number of bids - $max ")
    bw.newLine()
    bw.write(s"Minimum number of bids - $min ")
    bw.newLine()
    bw.write(s"Average of bids - $avg")
    bw.close()
    
    Source.fromFile("/home/hduser/auctions1.txt").foreach {x => print(x)} 
    
  }
}
