package sparksql
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import java.util.Properties

object lab15 {
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("sql-hive").master("local[*]")
                .config("hive.metastore.uris","thrift://localhost:9083").enableHiveSupport()
                .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    
    //reading from hive
    val df = spark.sql("select * from retaildb.txnrecords limit 10")
    df.show()
   
    //writing into hive from file 
    val df1 = spark.read.format("csv").option("header",true).option("inferschema",true)   
              .load("file:/home/hduser/hive/data/custs").toDF()
              
    //df1.write.saveAsTable("custldb.sparkcust")
    println("Data written into hive")
    
    //writing into hive from mysql
    val df2 = spark.read.format("jdbc")
                   .option("url","jdbc:mysql://localhost/custdb")
                   .option("user","root")
                   .option("password","root")
                   .option("dbtable","tblcustomer")
                   .option("driver","com.mysql.jdbc.Driver")
                   .load()
    //df2.write.saveAsTable("custdb.mysqlcust")
    println("Data written into hive from mysql")
    
    //writing into hive from postgresql
    val df3 = spark.read.format("jdbc")
                   .option("url","jdbc:postgresql://localhost/retail")
                   .option("user","hduser")
                   .option("password","hduser")
                   .option("dbtable","sparktrans")
                   .option("driver","org.postgresql.Driver")
                   .load()
    //df3.write.saveAsTable("custdb.postcust")
    println("Data written into hive from postgresql")

    
  }
}
