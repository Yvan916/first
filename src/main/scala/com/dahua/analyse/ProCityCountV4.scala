package com.dahua.analyse

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ProCityCountV4 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |inputPath
        """.stripMargin)
      sys.exit()
    }
    val Array(inputpath) = args
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val df: DataFrame = spark.read.parquet(inputpath)
    df.createTempView("log")

    val sql = "select" +
        " provincename,cityname," +
        "row_number() over(partition by provincename order by cityname) as pcsum " +
        "from log group by provincename,cityname"
    spark.sql(sql).show(100)
    // 需求1： 统计各个省份分布情况，并排序。

    // 需求2： 统计各个省市分布情况，并排序。

    // 需求3： 使用RDD方式，完成按照省分区，省内有序。

    // 需求4： 将项目打包，上传到linux。使用yarn_cluster 模式进行提交，并查看UI。

    // 需求5： 使用azkaban ，对两个脚本进行调度。

    spark.stop()
    sc.stop()


  }
}
