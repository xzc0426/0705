/**
  * Created by Xu on 2018/12/5.
  */
package com.xzc.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Xu on 2018/12/3.
  */
class MyFirstSql {

  def main(args: Array[String]): Unit = {

    //创建和spark连接的配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql1")

    //创建SparkSession
    val spark1 = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark1.sparkContext

    //必须加载隐式转换
    import spark1.implicits._

    //val rdd = sc。。。。。
    //rdd -> df

    val df = spark1.read.json("")

    // DSL风格操作
    df.show
    df.filter($"salary" > 3500).show(10)

    // SQL风格操作
    df.createOrReplaceTempView("employee")
    val df2 = spark1.sql("select * from employee")

    df2.show()

    //关闭spark session
    spark1.stop()
  }
}