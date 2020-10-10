//Task 6
package sparkex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object ex03 {
  def main(args:Array[String])=
  {
    val conf =new SparkConf().setAppName("Ex03").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd1 = sc.textFile("hdfs://localhost:54310/user/hduser/cricket.txt")
    val rdd2 = sc.textFile("hdfs://localhost:54310/user/hduser/hockey.txt")
    val rdd3 = sc.textFile("hdfs://localhost:54310/user/hduser/football.txt")
    
    //import sql.implicits._
    
    //players who are in cricket and football
    val cfdd = rdd1.intersection(rdd3)
    println("Players who are in cricket and football")
    cfdd.foreach(println)
    
    //players who are in all 3 sports
    val alld = rdd1.intersection(rdd2).intersection(rdd3)
    println("Players who are in all 3 sports")
    alld.foreach(println)
    
    //the distinct players
    val jdd = rdd1.union(rdd2).union(rdd3)
    val ddd = jdd.distinct
    println("the distinct players")
    ddd.foreach(println)
    
    //the distinct players with sports
    val cdd = rdd1.map( x => x + ",cricket")
    val hdd = rdd2.map(x => x + ",hockey")
    val fdd = rdd3.map(x => x + ",football")
    val result = cdd.union(hdd).union(fdd)
   
    val r1 = result.map(x => x.split(","))
    val r2 = r1.map(x => Row(x(0),x(1).toInt,x(2)))

    val sql = new SQLContext(sc)
    
    import sql.implicits._    
    
    
    val schema = StructType(List(StructField("Name",StringType,true),
        StructField("Age",IntegerType,true),
        StructField("Sport",StringType,true)))
   
    val df = sql.createDataFrame(r2,schema)

    val dropdf = df.dropDuplicates(Array("Name","Age"))
    println("The distinct players with sports")
    dropdf.show()
    

    
  }
}
