package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.TerritoryTool
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProcessCount2 {
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
    val rdd2: RDD[Log] = rdd.map(line => {
      line.split(",", -1)
    }).filter(_.length >= 85).map(Log(_))
    val rdd3: RDD[((String, String), List[Double])] = rdd2.map(line => {
      val qqs: List[Double] = TerritoryTool.qqsRtp(line.requestmode, line.processnode)
      val jingjia: List[Double] = TerritoryTool.jingjiaRtp(line.iseffective, line.isbilling, line.isbid, line.iswin, line.adorderid)
      val ggz: List[Double] = TerritoryTool.ggzjRtp(line.requestmode, line.iseffective)
      val mj: List[Double] = TerritoryTool.mjjRtp(line.requestmode, line.iseffective, line.isbilling)
      val ggc: List[Double] = TerritoryTool.ggcbRtp(line.iseffective, line.isbilling, line.iswin, line.winprice, line.adpayment)

      // 从广播变量中获取值。如果为空。替换成新的appname
      ((line.provincename,line.cityname), qqs ++ jingjia ++ ggz ++ mj ++ ggc)

    })
    rdd3.reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    })foreach(println)

  }

}
