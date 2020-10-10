//Task 18
package sparkex

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


//combine by key
object ex19 {
  
  def main(args:Array[String])=
  {
    
    val conf = new SparkConf().setAppName("ex15").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val lst2 = Seq(("Deepak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female", 2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
    val rdd =sc.parallelize(lst2)
    
    //defining createcombiner,mergevalue and merge combiner functions
       //(4000,1) (2000,1) (2000,1)
    def createCombiner(tuple:(Int)) =
      (tuple.toDouble,1)

      //if key matches -----(2000,1),(3000) =  (2000+3000) , 1 + 1
    def mergeValue(accumulator: (Double,Int), element: (Int)) =  
      (accumulator._1 + element , accumulator._2 + 1)
      
      //merging combiners of single key
    def mergeCombiner(accumulator1: (Double,Int), accumulator2: (Double,Int)) =
      (accumulator1._1 + accumulator2._1 , accumulator1._2 + accumulator2._2)
      
    val combrdd = rdd.map(x => ((x._1,x._2),x._3))
                     .combineByKey(createCombiner,mergeValue,mergeCombiner)
    combrdd.foreach(println) 
    
 
  }
}

