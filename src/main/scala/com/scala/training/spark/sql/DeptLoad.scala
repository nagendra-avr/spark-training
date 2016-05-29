package com.scala.training.spark.sql

import com.scala.training.spark.sql.EmployeeLoad.Employee
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Nagendra Amalakanta on 5/29/16.
 */
object DeptLoad {

  case class Dept(deptno:Int,dname:String,loc:String)

  def main(args: Array[String]) {


    if(args.length < 1) {
      println("Usage DeptLoad : <Text File>")
    }
    //Suppress Spark output
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sconf = new SparkConf();
    sconf.setAppName("Spark-Sql-DeptLoad")
    val sc = new SparkContext(sconf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    // Create an RDD of Employee objects and register it as a table.
    val dept = sc.textFile(args(0)).map(_.split(",")).map(p => Dept(p(0).trim.toInt, p(1), p(2))).toDF();

    dept.registerTempTable("dept")

    val depttable = sqlContext.sql("SELECT * from dept")

    depttable.show()
  }
}
