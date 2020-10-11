package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

//Reading Data from RDBMS and write into CSV file
object lab11 {
def main(args:Array[String])=
 {  
     val conf = new SparkConf().setAppName("SparkSql-Lab11").setMaster("local[*]")
     val sc = new SparkContext(conf)
     sc.setLogLevel("ERROR")
  
     val sql = new SQLContext(sc)
  
     val df = sql.read.format("jdbc")
              .option("url","jdbc:mysql://localhost/custdb")
              .option("user","root")
              .option("password","root")
              .option("dbtable","tblcustomer")
              .option("driver","com.mysql.jdbc.Driver")
              .load()
     
     df.createOrReplaceTempView("customer")
     
     sql.sql("select * from customer where age > 50 and age is not null").show()
     
     val df1 = df.filter(col("age") > 50 && col("age").isNotNull)
     df1.show()
     
     val df2 = sql.read.format("jdbc")
              .option("url","jdbc:postgresql://localhost/retail")
              .option("user","hduser")
              .option("password","hduser")
              .option("dbtable","weather")
              .option("driver","org.postgresql.Driver")
              .load()
     df2.show()
     
     df2.write.format("csv").save("file:/home/hduser/postgresqlweather")
     
     
 }  
}
