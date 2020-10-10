//Task 17
package sparkex

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object ex18 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex15").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val sql = new SQLContext(sc)
    
    import sql.implicits._
    
    val df = sql.read.format("csv").option("inferschema",true).option("header",true)
                 .load("file:/home/hduser/sales.csv").toDF("Department","Designation","costToCompany","State")
                 
    val df1 = df.groupBy(col("Department"),col("Designation"),col("State"))
                .agg(sum(col("costToCompany")).alias("TotalCTC"),count("*").alias("EmployeeCount"))
                
    df1.show()
                    
  }
  
}
