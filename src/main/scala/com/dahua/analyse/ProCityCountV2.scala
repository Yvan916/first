package com.dahua.analyse

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ProCityCountV2 {
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
    val conf: SparkConf = new SparkConf().setAppName("Bz2Parquet").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val df: DataFrame = spark.read.parquet(inputpath)
    df.createTempView("log")

    val sql = "select provincename,cityname,count(*) as pcsum from log group by provincename,cityname"
    val procityCount: DataFrame = spark.sql(sql)

    //输出mysql
    val load: Config = ConfigFactory.load()
    val properties = new Properties()
    properties.setProperty("driver",load.getString("jdbc.driver"))
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))

    procityCount.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)

    spark.stop()
    sc.stop()


  }
}
