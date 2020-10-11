package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import java.util.Properties

//writing data into database from file
object lab12 {
  def main(args:Array[String])=
  {
    val conf = new  SparkConf().setAppName("SQL-Lab12").setMaster("local[*]")
    val sc  = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val sql = new SQLContext(sc)
    
    val transdf = sql.read.format("csv")
                     .option("inferschema",true)
                     .load("file:/home/hduser/hive/data/txns")
                     .toDF("txnid","txndate","custid","amount","category","product","city","state","paymenttype")
              
                     
    //writing into mysql
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","root")
    prop.put("driver","com.mysql.jdbc.Driver")
    
    transdf.write.mode("overwrite").jdbc("jdbc:mysql://localhost/retail","sparktrans",prop)
    
    println("Data written into MysqlDB")
    
    //writing into Postgresql
    transdf.write.format("jdbc").mode("overwrite")
           .option("url","jdbc:postgresql://localhost/retail")
           .option("user","hduser")
           .option("password","hduser")
           .option("dbtable","sparktrans")
           .option("driver","org.postgresql.Driver")
           .save()
    
    println("Data written into postgresql")
  }
}
