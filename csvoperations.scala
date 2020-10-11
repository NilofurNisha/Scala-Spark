package sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._


object lab03 {
  def main(args:Array[String])=
  {
    val conf = new  SparkConf().setAppName("Spark-SQL-Lab01")
               .setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val sql = new SQLContext(sc)
    
    import sql.implicits._
    
    val schema = StructType(List(StructField("CustId",StringType,true),
        StructField("FName",StringType,true),
        StructField("LName",StringType,true),
        StructField("Age",IntegerType,true),
        StructField("Profession",StringType,true)))
        
    val df = sql.read.format("csv")
                .option("header",true)
//              .option("delimiter",",")
                .schema(schema)
                .option("inferschema",true)
                .load("file:/home/hduser/hive/data/custs")
//                .toDF("column names") if no header
    df.show()
    
    df.printSchema()
//  println(df.schema)    
                
  }
}
