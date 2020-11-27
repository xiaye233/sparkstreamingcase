package com.atguigu.app

import java.util.Properties

import com.atguigu.handler.{BlackListHandler, DateAreaCityAdCountHandler, LastHourAdCountHandler}
import com.atguigu.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author xystart
 * @create 2020-11-25 11:44
 *
 *         读取kafka的数据，进行黑名单比对，记录更新，添加黑名单
 */
object RealTimeApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

    val ssc = new StreamingContext(conf, Seconds(3))

    //读取数据
    val config: Properties = PropertiesUtil.load("config.properties")
    val topic: String = config.getProperty("kafka.topic")

    // 获取数据DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(_.value())
      .map(
        str => {
          val arr: Array[String] = str.split(" ")
          Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
        }
      )

    // 根据需求1.1：过滤数据中处在黑名单的用户的数据
    val filterDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream)

    // 需求1.2：将满足要求的用户写进黑名单
    BlackListHandler.addBlackList(filterDStream)

    // 测试打印
    filterDStream.count().print()

    // 需求2 ：时间，地区城市广告，点击量排名
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterDStream)


    // 需求3 ：最近两分钟的点击量
    val twoMinuteCount: DStream[(String, List[(String, Int)])] = LastHourAdCountHandler.getAdTwoMinToCount(filterDStream)
    twoMinuteCount.print()

    ssc.start()
    ssc.awaitTermination()



  }

}

//样例类获取的用户访问数据
case class Ads_log(timestamp: Long, area: String, city: String, userid: String, adid: String)
