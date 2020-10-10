//Task 16
package sparkex

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

case class emp(id:Int,ename:String)

object ex17 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex15").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    //one -way
    val rdd = sc.textFile("file:/home/hduser/empname.csv",1)
    val rdd1 = rdd.map(x => x.split(",").toList)
    val rdd3 = rdd1.sortBy(_(1))
    rdd3.foreach(println)
    rdd3.saveAsTextFile("file:/home/hduser/empresults")
    
    //using dataframe
    val sql = new SQLContext(sc)
    
    import sql.implicits._
    
    val edf = sql.read.format("csv").option("inferschema",true).load("file:/home/hduser/empname.csv").toDF("eid","ename")
    edf.sort(col("ename")).show()
    
    val erdd = edf.rdd.map(_.toString().replace("[","").replace("]","")).saveAsTextFile("file:/home/hduser/empresults1")
    println("written into text file")
  }
}
