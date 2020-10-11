package sparksql

import org.apache.spark.SparkConf


import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object lab04 {
  def main(args:Array[String])=
  {
    val conf = new  SparkConf().setAppName("Spark-SQL-Lab04")
               .setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val sql = new SQLContext(sc)
    
    import sql.implicits._
    val df = sql.read.format("csv")
                .option("inferschema",true)
                .load("file:/home/hduser/hive/data/txns")
                .toDF("txnid","txndate","custid","amount","category","product","state","city","paymenttype")
                
     df.printSchema()
     df.show()           
     val df1 = df.filter(df("city") === "Texas" && df("paymenttype") === "credit")
      
     
     val df2 = df.where("city = 'Texas' and paymenttype = 'credit'")
     
     val df3 = df2.select("txnid","custid","city").limit(10)
     df3.show()
     
     val df4 = df.groupBy("state","city").agg(sum("amount").alias("Totalsales"),
                 max("amount").alias("Maxamount"))
                 
     df4.show()            
     
    //Renaming column
     val df5 = df.withColumnRenamed("paymenttype","paytype")            
     df5.show()
     
    //Adding new column
     val df6 = df.withColumn("country",lit("USA"))
     df6.show() 
     df6.describe().show()
     
  }
    
  
}
