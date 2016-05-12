package com.caishi.etl

import java.text.SimpleDateFormat
import java.util.Calendar

import com.mongodb.hadoop.MongoInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject

/**
 * 将mongodb中的数据抽取到hdfs
 * Created by root on 15-11-3.
 */
object MongoToHdfs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("etl-mongodb to hdfs").setMaster("local")
    val sc = new SparkContext(conf)

    // 创建mongodb配置
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.uri", "mongodb://host:27017/news.newsContent")

    val mongo = sc.newAPIHadoopRDD(hadoopConf,classOf[MongoInputFormat],classOf[Object],classOf[BSONObject])
    val c = mongo.repartition(10).map(news => news._2).saveAsTextFile("hdfs://host:9000/test/mongo")
    mongo.partitionBy().map(news => news._2).foreachPartition()
    println("-------------------"+c)
  }


  def saveToParquet(rdd: RDD[String], dataType: String): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    sqlContext.setConf("parquet.enable.summary-metadata", "false")

    // Loads an `JavaRDD[String]` storing JSON objects (one object per record)
    val df = sqlContext.read.json(rdd)
    if(df.count() > 0){
      val dirname = {
        dataType match {
          case "topic_comment_event" => dirName.getDirName("topic_comment_event")
          case "topic_news_behavior" => dirName.getDirName("topic_news_behavior")
          case "topic_news_social" => dirName.getDirName("topic_news_social")
          case "topic_common_event" => dirName.getDirName("topic_common_event")
          case "topic_scene_behavior" => dirName.getDirName("topic_scene_behavior")
        }
      }
      try {
        df.write.format("parquet").mode(SaveMode.Append).save(dirname)
      }catch {
        case e: Throwable =>
          println("ERROR: Save to parquet error\n" + e.toString + "\n" + rdd.collect())
      }
    }
  }
}


/* 计算文件存放目录 */
object dirName {
  def getDirName(dataType: String): String = {
    val time = Calendar.getInstance().getTime
    val year = new SimpleDateFormat("yyyy").format(time)
    val month = new SimpleDateFormat("MM").format(time)
    val day = new SimpleDateFormat("dd").format(time)
    val hour = new SimpleDateFormat("HH").format(time)
    val minute = new SimpleDateFormat("mm").format(time)
    val filename = ("hdfs://10.4.1.4:9000/test/dw/%s/%s/%s/" + dataType + "/%s").format(year, month, day, hour)
    filename
  }
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
      instance.setConf("spark.sql.parquet.compression.codec", "snappy")
    }
    instance
  }
}
