package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.TerritoryTool
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |inputPath、
          |mappingPath、
          |outputPath
        """.stripMargin)
      sys.exit()
    }
    val Array(inputpath,mappingPath,outputPath) = args
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val mapping: RDD[String] = sc.textFile(mappingPath)
    val apps: Map[String, String] = mapping.map(line => {
      val strings: Array[String] = line.split("[:]", -1)
      (strings(0), strings(1))
    }).collect.toMap
    val bc: Broadcast[Map[String, String]] = sc.broadcast(apps)

    val rdd: RDD[String] = sc.textFile(inputpath)
    val res: RDD[Log] = rdd.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85).map(Log(_)).filter(t => !t.appid.isEmpty || !t.appname.isEmpty)
    val res1: RDD[(String, List[Double])] = res.map(line => {
      val qqs: List[Double] = TerritoryTool.qqsRtp(line.requestmode, line.processnode)
      var appname: String = line.appname
      if (appname.isEmpty || appname == "") {
        appname = bc.value.getOrElse(line.appid, "未知")
      }
      (appname, qqs)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => {
        t._1 + t._2
      })
    })

    res1.saveAsTextFile(outputPath)
  }

}
