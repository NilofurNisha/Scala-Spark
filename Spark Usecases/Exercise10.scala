//Task 13
package sparkex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ex14 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex14").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val rdd1 = sc.textFile("file:/home/hduser/spw/file1.txt")
    val rdd2 = sc.textFile("file:/home/hduser/spw/file2.txt")
    val rdd3 = sc.textFile("file:/home/hduser/spw/file3.txt")
    
    val rdd = rdd1.union(rdd2).union(rdd3)
    //rdd.foreach(println)
    
    val lst = List("a","the","an","as","a","with","this","these","is","are","in","for","to","and","The","of")
    val lst1 = sc.parallelize(lst)
    println("=====list of words=======")
    lst1.foreach(println)
    
    
    val rdd4 = rdd.flatMap(x => x.split(" ").toList)
    println("====Input=====")
    rdd4.foreach(println)
   
    
    //intersection = filter matching words between 2 RDD's but without duplicates
    //Double subtraction - filter matching words between 2 RDD's with duplicates
    
    println("==========filtered===========")
    val diff = rdd4.subtract(lst1)
    val rdd5 = rdd4.subtract(diff)
    rdd5.foreach(println)
    val wc = rdd5.count
    println("words count " ,wc)
    
  }
}
