package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

/**
 * @author xystart
 * @create 2020-11-25 17:40
 */
object LastHourAdCountHandler {
  private val sdf = new SimpleDateFormat("HH:mm")



  def getAdTwoMinToCount(filterDStream: DStream[Ads_log]) = {

    // 第一步先给数据流开窗，窗口给2分钟，步长是默认的时间间隔3s
    val windowDStream: DStream[Ads_log] = filterDStream.window(Minutes(2))

    val adHmToCountDStream: DStream[((String, String), Int)] = windowDStream.map(
      adsLog => {
        val hm: String = sdf.format(new Date(adsLog.timestamp))
        ((adsLog.adid, hm), 1)
      }
    ).reduceByKey(_ + _)



    adHmToCountDStream.map{
      case ((adid,hm),count) =>{
        (adid,(hm,count))
      }
    }.groupByKey()
      .mapValues(
        iter =>{
          iter.toList.sortBy(_._1)
        }
      )


  }

}
