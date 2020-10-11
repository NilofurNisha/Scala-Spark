package sparksql

import org.apache.spark.sql.SparkSession

object lab06 {
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Spark-Session-Lab06")
                     .config("hive.metastore.uris","thrift://localhost:9083")
                     .enableHiveSupport()
                     .master("local")
                     .getOrCreate()
       
    spark.sparkContext.setLogLevel("ERROR")
    
    //Reading from hive table
    val df = spark.sql("select * from projectdb.department")
    df.show()

  
    //Reading data from csv file and writing data to hive 
    val transdf = spark.read.format("csv")
                .option("inferschema",true)
                .load("file:/home/hduser/hive/data/txns")
                .toDF("txnid","txndate","custid","amount","category","product","state","city","paymenttype")
                
     transdf.write.saveAsTable("retaildb.sparktrans")
     println("Table created in Hive")
  }
}
