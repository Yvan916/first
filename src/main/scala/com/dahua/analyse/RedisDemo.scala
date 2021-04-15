package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.{AppIntoRedis, JedisUtil, TerritoryTool}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object RedisDemo {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |inputPath、
          |outputPath
        """.stripMargin)
      sys.exit()
    }
    val Array(inputpath,outputPath) = args
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile(inputpath)
    val res: RDD[Log] = rdd.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85).map(Log(_)).filter(x=> !x.appid.isEmpty || !x.appid.isEmpty)

    res.mapPartitions(line=>{
      val resoure: Jedis = JedisUtil.resource
      val strings = new ListBuffer[(String,List[Double])]
      line.foreach(line=>{
        var appname: String = line.appname
        if(appname.isEmpty){
          appname = resoure.get(line.appid)
        }
        val doubles: List[Double] = TerritoryTool.qqsRtp(line.requestmode, line.processnode)
        strings += ((appname,doubles))
      })
      resoure.close()
      strings.iterator

    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => {
        t._1 + t._2
      })
    }).saveAsTextFile(outputPath)
  }

}
