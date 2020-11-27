package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.time.format.{DateTimeFormatter, ResolverStyle}
import java.util.Date

import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
 * @author xystart
 * @create 2020-11-25 11:55
 */
object BlackListHandler {
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def addBlackList(filterDStream: DStream[Ads_log]) = {

    //统计当前批次中单日每个用户点击每个广告的总次数
    val dateUserAdToCount: DStream[((String, String, String), Long)] = filterDStream.map(
      adsLog => {
        // 将时间转化成字符串
        val date: String = sdf.format(new Date(adsLog.timestamp))
        //b.返回值
        ((date, adsLog.userid, adsLog.adid), 1L)
      }
    ).reduceByKey(_ + _)

    //写出
    dateUserAdToCount.foreachRDD(
      rdd => {
        // 每个分区数据写出一次

        rdd.foreachPartition(
          iter => {
            val connection: Connection = JDBCUtil.getConnection
            iter.foreach {
              case ((dt, user, adid), count) => {
                // 向mysql中的表添加更新数据
                JDBCUtil.executeUpdate(
                  connection,
                  """
                    |insert into  user_ad_count(dt,userid,adid,count)
                    |values(?,?,?,?)
                    |on duplicate key
                    |update count=count+?
                    |""".stripMargin,
                  Array(dt, user, adid, count, count)
                )

                // 查询mysql的表，看看有没有需要加入黑名单的人
                val ct: Long = JDBCUtil.getDataFromMysql(
                  connection,
                  """
                    |select count from user_ad_count
                    |where dt =? and userid =? and adid=?
                    |""".stripMargin,
                  Array(dt, user, adid)
                )
                // 如果大于了30，加入黑名单
                if (ct > 30000) {
                  JDBCUtil.executeUpdate(
                    connection,
                    """
                      |insert into black_list (userid) values (?) on DUPLICATE KEY update userid=?
                      |""".stripMargin,
                    Array(user, user)
                  )
                }
              }
            }
            connection.close()
          }
        )
      }
    )


  }


  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] = {

    adsLogDStream.filter(
      adsLog => {
        // 获取连接
        val connection: Connection = JDBCUtil.getConnection
        // 判断是否存在黑名单中
        val bool: Boolean = JDBCUtil.isExist(connection,
          """
            |select * from black_list where userid = ?
            |""".stripMargin,
          Array(adsLog.userid))

        connection.close()
        // 存在久不要这条数据
        !bool
      }
    )


  }


}



