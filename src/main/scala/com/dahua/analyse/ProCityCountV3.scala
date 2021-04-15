package com.dahua.analyse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ProCityCountV3 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |inputPath
          |outputPath
        """.stripMargin)
      sys.exit()
    }
    val Array(inputpath,outputpath) = args
    val conf: SparkConf = new SparkConf().setAppName("Bz2Parquet").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile(inputpath)
    val mf: RDD[Array[String]] = rdd.map(line => {
      line.split(",", -1)
    }).filter(line => {
      line.length >= 85
    })

    val proCIty: RDD[((String, String), Int)] = mf.map(field => {
      val pro = field(24)
      val city = field(25)
      ((pro, city), 1)
    })
    val reduceByKey: RDD[((String, String), Int)] = proCIty.reduceByKey(_+_)
    val res: RDD[String] = reduceByKey.map(res => {
      res._1._1 + "\t" + res._1._2 + "\t" + res._2
    })
    res.saveAsTextFile(outputpath)

    spark.stop()
    sc.stop()


  }
}
