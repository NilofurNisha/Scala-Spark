//Task 19
package sparkex

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object ex20 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex15").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val a = sc.parallelize(List("dog","tiger","lion","cat","panther","eagle"))
    
    val b = a.flatMap(x => x.split(",").toSeq)
    
    val c = b.map(t => (t,(t.length)))
    //c.foreach(println) 
    
   /***
    (dog,3)
    (tiger,5)
    (lion,4)
    (cat,3)
    (panther,7)
    (eagle,5)
    *
    * 
    */
    
    val d = c.groupBy(_._2).map{case (k,v) => k -> v.map {_._1}}
    d.foreach(println)
    
    //output
    /**
     * 
     (4,List(lion))
     (3,List(dog, cat))
     (7,List(panther))
     (5,List(tiger, eagle))*
     * 
     */
  }
}
