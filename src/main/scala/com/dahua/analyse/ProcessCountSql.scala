package com.dahua.analyse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProcessCountSql {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |inputPath
          |outputPath
        """.stripMargin)
      sys.exit()
    }
    val Array(inputpath) = args
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val df: DataFrame = spark.read.parquet(inputpath)
    df.createTempView("log")
    val sql =
      """
        |select
        |  provincename
        |  ,cityname
        |  ,sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end)as allreq
        |  ,sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end)as validreq
        |from
        |  log
        |group by
        |  provincename,cityname
      """.stripMargin

    spark.sql(sql).show(100)
  }

}
