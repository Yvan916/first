package com.dahua.analyse



import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityCount {
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
    val conf: SparkConf = new SparkConf().setAppName("Bz2Parquet").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val df: DataFrame = spark.read.parquet(inputpath)

    df.createTempView("log")
    val sql = "select provincename,cityname,count(*) from log group by provincename,cityname";
    val procityCount: DataFrame = spark.sql(sql)

    val config: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(config)
    val path = new Path(outputpath)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    procityCount.write.partitionBy("provincename","cityname").json(outputpath)

    spark.stop()
    sc.stop()


  }
}
