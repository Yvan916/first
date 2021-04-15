package com.dahua.bz2parquet

import com.dahua.bean.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Bz2ParquetV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |loginputPath
          |logoutputPath
        """.stripMargin)
      sys.exit()
    }
    val Array(loginputpath,logoutputpath) = args
    val conf: SparkConf = new SparkConf().setAppName("Bz2Parquet").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    conf.registerKryoClasses(Array(classOf[Log]))

    val log: RDD[String] = sc.textFile(loginputpath)

    val arr: RDD[Array[String]] = log.map(_.split(",",-1))
    val filter: RDD[Array[String]] = arr.filter(_.length>=85)
    val logBean: RDD[Log] = filter.map(Log(_))
    val df: DataFrame = spark.createDataFrame(logBean)
    df.write.parquet(logoutputpath)

    spark.stop()
    sc.stop()


  }

}
