package com.dahua.analyse

import com.dahua.bean.Log
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}



object SqlDemo {
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
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.debug.maxToStringFields", "100")
      .set("spark.sql.crossJoin.enabled","true")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val rdd: Dataset[String] = spark.read.textFile(mappingPath)
    val rdd1: Dataset[apptype] = rdd.map(line => {
      val strings: Array[String] = line.split("[:]")
      apptype(strings(0), strings(1))
    })
    rdd1.createTempView("apptype")

    val rdd2: Dataset[String] = spark.read.textFile(inputpath)
    val apps: Dataset[Log] = rdd2.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85).map(Log(_))
    apps.createTempView("apps")

    val sql =
      """
        |
        |select
        |   allreq
        |   ,validreq
        |   ,appid
        |   ,appname
        |   ,apptype
        |  from
        |(select
        |   sum(allreq) as allreq
        |   ,sum(validreq) as validreq
        |   ,first(appid) as appid
        |   ,first(appname) as appname
        |   ,first(apptype) as apptype
        |from
        |(select
        |    a.provincename
        |    ,a.appname
        |    ,a.appid
        |    ,a.cityname
        |    ,a.allreq
        |    ,a.validreq
        |    ,b.appname as apptype
        |  from
        |((select
        |    appid
        |    ,appname
        |    ,provincename
        |    ,cityname
        |    ,(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as allreq
        |    ,(case when requestmode = 1 and processnode >= 2 then 1 else 0 end)as validreq
        |  from
        |    apps)a
        |full join
        |(select
        |    appid
        |    ,appname
        |  from
        |    apptype)b
        |on a.appid = b.appid
        |))
        |group by provincename,cityname)
      """.stripMargin


    val sql1 =
      """
        |select
        |  aid
        |  ,case when aname = "" then bname else aname end as appname
        |  from
        |(select
        |  apps.appid as aid
        |  ,apps.appname as aname
        |   ,apptype.appid as bid
        |   ,apptype.appname as bname
        |   from
        |   apps,apptype
        |   where
        |   apps.appid = apptype.appid)
      """.stripMargin

    val sql2 =
      """
         |selecto
         |   d.appid
         |   ,case when d.newname = "null" then "未知" else d.newname end as appname
         |   ,d.allreq
         |   ,d.validreq
         |   from
         |(select
         |   c.appidk
         |   ,c.newname
         |   ,sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end)as allreq
         |   ,sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end)as validreq
         |  from
         |(select
         |    a.*
         |   	,case when a.appname = "其他" then b.appname else a.appname end as newname
         |  from
         |    apps a
         |left join
         | apptype b
         |on a.appid = b.appid) c
         |group by c.appid,c.newname)d
      """.stripMargin



    spark.sql(sql2).show(2000)
//spark.sql("select count(*) from apptype").show()


  }

  case class apptype(appid:String,appname:String){
    def apply(appid: String, appname: String): apptype = new apptype(appid, appname)
  }

}
