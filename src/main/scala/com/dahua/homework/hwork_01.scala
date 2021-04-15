package com.dahua.homework

import com.dahua.bean.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object hwork_01 {
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
    
    val rdd: RDD[String] = sc.textFile(inputpath)

    val rdd4: RDD[(String, List[(String, Int)])] = rdd.map(_.split(",", -1)).filter(x => {
      x.length >= 85
    }).map(Log(_)).map(line => {
      ((line.provincename, line.cityname), 1)
    }).reduceByKey(_ + _).groupBy(x => x._1._1).map(line => {
      (line._1, line._2.toMap.map(x => {
        (x._1._2, x._2)
      }).toList.sortBy(_._2).reverse)
    })


    rdd4.foreach(println)


    
  }

}
