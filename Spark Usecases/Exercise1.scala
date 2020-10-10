package sparkex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object ex01 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex01").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    //Task 1
    val rdd = sc.textFile("hdfs://localhost:54310/user/hduser/Sparkexercise/Inceptez1.txt")
    println("Row count ",rdd.count)
    
    //Task 2
    val rdd1 = sc.textFile("hdfs://localhost:54310/user/hduser/Sparkexercise/Inceptez2.txt")
    val idd = rdd1.filter( x => x.contains("Inceptez"))
    println("Rows that contains Inceptez ",idd.count)
    
    val ndd = rdd1.filter(x => !(x.contains("Inceptez")))
    println("Rows that does not contain Inceptez ",ndd.count)
    
    //Task 3
    val lst = List("We", "Are" ,"Learning" , "Hadoop" , "From" , "Inceptez" , "We", "Are" ,"Learning" , "Spark" , "From" , "Inceptez.com" , "hadoop" , "HADOOP")
    val pdd = sc.parallelize(lst)
    val cpdd = pdd.map(x => x.split(","))
    println("Total no of words in list ",cpdd.count)
    val hdd = pdd.filter(x => x.toLowerCase() != "hadoop")
    println("Words that doesn't contain Hadoop ",hdd.count)
    
    //Task 4
    val jdd1 = sc.textFile("file:/home/hduser/hdpcd/Inceptez4A.txt")
    val jdd2 = sc.textFile("file:/home/hduser/hdpcd/Inceptez4B.txt")
    val jdd3 = sc.textFile("file:/home/hduser/hdpcd/Inceptez4C.txt")

    val join = jdd1.union(jdd2).union(jdd3)
    val result = join.flatMap(x => x.split(" ").toList)
    println("No of words in all 3 files ",result.count)
  }
}
