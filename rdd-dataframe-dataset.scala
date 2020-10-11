package sparksql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

case class customerinfo(custid:Int,fname:String,lname:String,age:Int,profession:String)
object Lab11 {
 
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("Inceptez-SQLLab-11").setMaster("local[*]")
    val sc = new SparkContext(conf)    
    sc.setLogLevel("ERROR")
    
    val sql = new SQLContext(sc)    
    
    import sql.implicits._
    
    val custdf = sql.read.format("csv")
    .option("inferSchema",true)    
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","profession")
    
     //compile-time safety
    
    val custdf1 = custdf.filter(col("age") > 30)
    
    custdf1.show()
    custdf1.printSchema() 
    
    val custds = sql.read.format("csv")
    .option("inferSchema",true)    
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","fname","lname","age","profession").as[customerinfo]
    
    val custds1 = custds.filter(x => x.age > 30)
    
    //or
    val custds2 = custds.filter(col("age") > 30)
    //custds2.show()    
    
    
    //Convert dataframe to dataset
    val ds = custdf.as[customerinfo]
    
    
    //Convert dataset to dataframe
    val df = custds.toDF
    
    //Convert dataframe to RDD
    val rdd = df.rdd    
    val rdd1 = rdd.map(x => (x.getInt(0),x.getString(1),x.getString(2)))
    
    //Convert dataset to RDD
    
    val rdd2 = ds.rdd    
    val rdd3 = rdd2.map(x => (x.custid,x.fname,x.lname))
    
    
  }
  
}
