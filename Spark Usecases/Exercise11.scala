//Task 14
package sparkex

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

case class name(id:Int,sname:String)
case class mark(id:Int,m1:Int,m2:Int,m3:Int)

object ex15 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex15").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val data = sc.textFile("file:/home/hduser/spw/data.csv",1)
    val marks = sc.textFile("file:/home/hduser/spw/marks.csv",1)
    
    
    val pair1 = data.map(x => x.split(",")).map(x => (x(0).toInt,x(1)))
    //pair1.foreach(println)
    
    //keys
    //pair1.keys.foreach(println)
    
    //values
    //pair1.values.foreach(println)
    
    //====================================Join============================================================//
    val splt_data = data.map(x => x.split(",")).map(x => name(x(0).toInt,x(1).toString))
    val splt_marks = marks.map(x => x.split("~")).map(x => mark(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toInt))
   
  
    val data_items = splt_data.map(x => (x.id,x))
    val mark_items = splt_marks.map(x => (x.id,x))
   
    val joins = data_items.join(mark_items)
    //joins.foreach(println)
    
   //=====================================DataFrame========================================================//
   
   val sql = new SQLContext(sc)
   import sql.implicits._ 
   
   val datadf = data.map(x => x.split(",")).map(x => name(x(0).toInt,x(1).toString)).toDF()
   
   datadf.createOrReplaceTempView("Name")
   
   val marksdf = marks.map(x => x.split("~")).map(x => mark(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toInt)).toDF()
   
   marksdf.createOrReplaceTempView("Marks")
   
   val student = sql.sql("select a.id,a.sname,b.m1,b.m2,b.m3 from Name a join Marks b where a.id = b.id")
   //student.show()
 
   val std1 = student.groupBy(col("id"),col("sname"),col("m1"),col("m2"),col("m3"))
                    .agg(sum(col("m1")+col("m2")+col("m3")).alias("Totalmarks"))
                
   
   //std1.show()
   //student.printSchema()
   
   val std2 = student.join(std1,"id")
   std2.show()
   
   val std3 = std2.agg(max("Totalmarks").alias("MaximumMark"))
   std3.show()
  }
}
