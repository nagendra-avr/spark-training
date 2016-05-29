package com.scala.training.spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by Nagendra Amalakanta on 5/29/16.
 */
object EmployeeLoad {

  case class Employee(empno:Int ,ename: String ,job: String ,mgr: String , hire: String , sal:String , comm: String , deptno:Int);

  def main(args: Array[String]) {

    if(args.length < 1) {
      println("Usage EmployeeLoad : <Text File>")
    }
    //Suppress Spark output
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sconf = new SparkConf();
    sconf.setAppName("Spark-Sql-EmployeeLoad")
    val sc = new SparkContext(sconf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    // Create an RDD of Employee objects and register it as a table.


    val employees = sc.textFile(args(0)).map(_.split(",")).map(p => Employee(p(0).trim.toInt, p(1), p(2), p(3), p(4),p(5),p(6),p(7).trim.toInt)).toDF();

    employees.registerTempTable("emp")

    val emptable = sqlContext.sql("SELECT empno, ename, sal,mgr from emp")

    emptable.show()

    // Print the DF schema
    emptable.printSchema()

    // Select customer name column
    emptable.select("empno").show()

    // Select customer name and city columns
    emptable.select("ename", "sal").show()

    // Select a customer by id
    emptable.filter(emptable("empno").equalTo(7839)).show()

    // Count the customers by zip code
    emptable.groupBy("mgr").count().show()


  }
}
