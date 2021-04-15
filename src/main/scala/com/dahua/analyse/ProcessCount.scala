package com.dahua.analyse

import com.dahua.bean.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object ProcessCount {
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

    val rdd: RDD[String] = sc.textFile(inputpath)
    val rdd2: RDD[((String, String), (Int, Int))] = rdd.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85).map(Log(_)).map(line => {
      ((line.provincename, line.cityname), (line.requestmode, line.processnode))
    })
    val rdd3: RDD[((String, String), (Int, Int))] = rdd2.filter(_._2._1==1).filter(_._2._2>=1)
    val rdd4: RDD[((String, String), Int)] = rdd3.map(line => {
      (line._1, 1)
    }).reduceByKey(_ + _)

    rdd4.foreach(println)


    spark.stop()
    sc.stop()

  }

}
