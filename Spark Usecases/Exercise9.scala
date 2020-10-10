//Task 12
package sparkex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ex13 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex12").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd  = sc.textFile("file:/home/hduser/Downloads/context.txt",1)
    val rdd1 = sc.textFile("file:/home/hduser/Downloads/remove.txt",1)
    
    
    
    val con = rdd.flatMap(x => x.split(" "))
    //con.foreach(println)
    
    
    val rem = rdd1.flatMap(x => x.split(" "))
    //rem.foreach(println)
    
    
    println("=====join=======")
    val join = rdd.union(rdd1)
    //join.foreach(println)
        
    
    
    println("=====subtract======")
    val result = con.subtract(rem).collect
    result.foreach(println)
    
    println("===================")
    val r1 = result.map(x => (x,1))
    val r2 = r1.reduce((a,b) => (a._1 ,a._2 + b._2))
    println(r2)
        
  }
   

 }
   
 
