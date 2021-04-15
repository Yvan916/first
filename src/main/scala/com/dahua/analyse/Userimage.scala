package com.dahua.analyse

import com.dahua.analyse.SqlDemo.apptype
import com.dahua.bean.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}

object Userimage {
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
    import spark.implicits._

    val rdd: RDD[String] = sc.textFile(inputpath)
    val res: RDD[Log] = rdd.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85).map(Log(_)).filter(t => !t.appid.isEmpty || !t.appname.isEmpty)

    val rdd3: Dataset[String] = spark.read.textFile(mappingPath)
    val rdd1: Dataset[apptype] = rdd3.map(line => {
      val strings: Array[String] = line.split("[:]")
      apptype(strings(0), strings(1))
    })
    rdd1.createTempView("apptype")

    //sql
    res.toDS().createTempView("log")

    val sql =
      """
        |
        |select
        |  logid
        |  ,concat("LC",case when adspacetype < 10 then concat("0",adspacetype) else adspacetype end , "->" ,count(adspacetype)) as adspacetype
        |  ,concat("LN",adspacetypename,"->" ,count(adspacetypename)) as adspacetypename
        |  ,concat("CN",adplatformproviderid,"->" ,count(adplatformproviderid)) as adplatformproviderid
        |  ,newname
        |  ,provincename
        |  ,cityname
        |  ,concat(clienttype,"->" ,count(clienttype)) as clienttype
        |  ,sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end)as allreq
        |  ,sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end)as validreq
        |from
        |(select
        |  case when imei != "" then imei
        |  when mac != "" then mac
        |  when idfa != "" then idfa
        |  when androidid != "" then androidid
        |  when openudid != "" then openudid else "未知" end as logid
        |  ,case when adplatformproviderid = 1 then "D00010001"
        |  when adplatformproviderid = 2 then "D00010002"
        |  when adplatformproviderid = 3 then "D00010003" else "D00010004" end as clienttype
        |  ,case when l.appname = "其他" then a.appname else l.appname end as newname
        |  ,l.*
        |from
        |  log l
        |left join
        |  apptype a
        |on l.appid = a.appid
        |)
        |where
        |  logid != "未知"
        |group by
        |  logid,adspacetype,adspacetypename,newname,adplatformproviderid,provincename,cityname,clienttype
        |
      """.stripMargin

    spark.sql(sql).show(2000)
//    spark.sql(sql).rdd.repartition(1).saveAsTextFile(outputPath)




    //rdd
//    val res1: RDD[(String, ((String, String), Int))] = res.map(log => {
//      (getKey(log), ((getAdType(log), getAdTypeName(log)), 1))
//    }).filter(line => {
//      line._1 != "未知"
//    })
//    val res2: RDD[(String, ((String, String), Int))] = res1.reduceByKey((x, y) => {
//      (x._1, x._2 + y._2)
//    })
//    val res3: RDD[(String, String, String)] = res2.map(line => {
//      (line._1, line._2._1._1 + "->" + line._2._2, line._2._1._2 + "->" + line._2._2)
//    })
//
//    res3.saveAsTextFile(outputPath)



  }


  //获取用户画像关键字
  def getKey(log: Log):String={
    if(!log.imei.isEmpty){
      log.imei
    }else if(!log.mac.isEmpty){
      log.mac
    }else if(!log.idfa.isEmpty){
      log.idfa
    }else if(!log.androidid.isEmpty){
      log.androidid
    }else if(!log.openudid.isEmpty){
      log.openudid
    }else {
     "未知"
    }
  }

  //获取广告位类型
  def getAdType(log: Log):String={
    var res = ""
    val adspacetype: Int = log.adspacetype
    if (adspacetype<10){
      res = "LC0"+adspacetype.toString
    }else{
      res = "LC"+adspacetype.toString
    }
    res
  }
  //获取广告位类型名称
  def getAdTypeName(log: Log):String={
    var res = ""
    val adspacetype= log.adspacetypename
    res = "LN"+adspacetype
    res
  }

}
