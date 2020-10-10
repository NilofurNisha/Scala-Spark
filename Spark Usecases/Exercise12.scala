//Task 15
package sparkex

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object ex16 {
  def main(args:Array[String])=
  {
    val conf = new SparkConf().setAppName("ex15").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val sql = new SQLContext(sc)
    
    import sql.implicits._
    
    val empmanager = sql.read.format("csv").option("inferschema",true)
                    .load("file:/home/hduser/empmanager.csv")toDF("eid","emname")
       
    empmanager.createOrReplaceTempView("EmpManager")
    
    val empname = sql.read.format("csv").option("inferschema",true)
                    .load("file:/home/hduser/empname.csv")toDF("eid","ename")
    
    empname.createOrReplaceTempView("EmpName")
                    
    val empsalary = sql.read.format("csv").option("inferschema",true)
                    .load("file:/home/hduser/empsalary.csv")toDF("eid","esal")
                
    empsalary.createOrReplaceTempView("EmpSalary")
    
    val employee = sql.sql("select e.eid,e.ename,s.esal,m.emname from EmpName e inner join EmpManager m on e.eid = m.eid inner join EmpSalary s on m.eid = s.eid")
    
    //employee.show()
  
    //writing data to text file
    
    //it encloses data with [ ] -replace that
    val rdd = employee.rdd.map(_.toString().replace("[","").replace("]","")).saveAsTextFile("file:/home/hduser/employeedata")
    println("written to text file")
   
  }
}
