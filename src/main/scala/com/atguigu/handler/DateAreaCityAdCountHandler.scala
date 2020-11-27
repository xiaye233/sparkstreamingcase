package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * @author xystart
 * @create 2020-11-25 15:50
 */
object DateAreaCityAdCountHandler {

  // 日期格式化
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")
  def saveDateAreaCityAdCountToMysql(filterDStream: DStream[Ads_log]): Unit = {

    val dtAreaCityAdToSum: DStream[((String, String, String, String), Int)] = filterDStream.map(adsLog => {
      val dt: String = sdf.format(new Date(adsLog.timestamp))
      ((dt, adsLog.area, adsLog.city, adsLog.adid), 1)
    }).reduceByKey(_ + _)

    dtAreaCityAdToSum.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter =>{
            val connection: Connection = JDBCUtil.getConnection

            iter.foreach{
              case ((dt,area,city,adid),count) => {
                JDBCUtil.executeUpdate(
                  connection,
                  """
                    |insert into  area_city_ad_count(dt,area,city,adid,count)
                    |values (?,?,?,?,?)
                    |on duplicate key
                    |update count=count+?
                    |""".stripMargin,
                  Array(dt,area,city,adid,count,count)
                )
              }
            }

            connection.close()

          }
        )
      }
    )

  }


}
