package sparksql

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object lab09 {
  def main(args:Array[String])=
  {
    val conf = new  SparkConf().setAppName("Spark-fileformats-Lab09").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val sql = new SQLContext(sc)
    
    import sql.implicits._
    val custdf = sql.read.format("csv")
                 .option("inferschema",true)
                 .load("file:/home/hduser/hive/data/custs")
                 .toDF("custid","fname","lname","age","profession")
    
    custdf.createOrReplaceTempView("customer")
    
    val transdf = sql.read.format("csv")
                .option("inferschema",true)
                .load("file:/home/hduser/hive/data/txns")
                .toDF("txnid","txndate","custid","amount","category","product","state","city","paymenttype")
    
    transdf.createOrReplaceTempView("txns")
                 
    val df = sql.sql("""select profession,state,round(sum(amount),2) as totalsales from txns inner join customer
                 on txns.custid = customer.custid group by profession,state order by state""")
    
    df.show()             
    val df1 = df.filter(col("totalsales") > 2000)
    df1.show()
   
    //writing in different formats
    
    df1.write.format("csv").mode("overwrite").save("file:/home/hduser/custsalesreportcsv")
    
    df1.write.format("orc").mode("overwrite").save("file:/home/hduser/custsalesreportorc")
    
    df1.write.format("parquet").mode("overwrite").save("file:/home/hduser/custsalesreportparquet")
    
    df1.write.format("json").mode("overwrite").save("file:/home/hduser/custsalesreportjson")
    
  
  }
  
}
