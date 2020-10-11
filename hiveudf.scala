package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object lab08 {
  def main(args:Array[String])=
  {
    val spark = SparkSession.builder().appName("Spark-Session-Lab08")
                     .config("hive.metastore.uris","thrift://localhost:9083")
                     .enableHiveSupport()
                     .master("local")
                     .getOrCreate()
       
    spark.sparkContext.setLogLevel("ERROR")
    
    val df = spark.sql("select * from custdb.customer")
    
    
    //way 1 
    val custcategory = udf(getcustage)
    
    val df1 = df.withColumn("Category",custcategory(col("age")))
    
    df1.show()
    
    //way 2
    //To use UDF function in sql query we have to register it
    spark.udf.register("custageudf",getcustage)
    
    spark.sql("select custageudf(age) from custdb.customer").show()
  }
      val getcustage = (age:Int) =>
    {
      if (age < 30 )
        "Young"
      else if (age < 50)
        "Middle age"
      else if (age > 60)
        "Senior Citizen"
      else
        "NA"
    }
  
}
