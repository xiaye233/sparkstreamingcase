package com.atguigu.mock

import java.util.Properties

import com.atguigu.util.PropertiesUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * @author xystart
 * @create 2020-11-24 21:06
 */
object MockerRealTime {

  /**
   * 模拟的数据
   * 格式 ：timestamp area city userid adid
   * 某个时间点 某个地区 某个城市 某个用户 某个广告
   * 1604229363531 华北 北京 3 3
   */

  def generateMockData: Array[String] = {
    val mockdataString = new ArrayBuffer[String]()

    val cityRandomOptions: RandomOptions[CityInfo] = RandomOptions(
      RanOpt(CityInfo(1, "北京", "华北"), 30),
      RanOpt(CityInfo(2, "上海", "华东"), 30),
      RanOpt(CityInfo(3, "广州", "华南"), 10),
      RanOpt(CityInfo(4, "深圳", "华南"), 20),
      RanOpt(CityInfo(5, "天津", "华北"), 10)
    )
    val random = new Random()

    // 模拟实时数据：
    // timestamp area city userid adid
    for (i <- 1 to 50) {
      // 实时时间戳
      val timeStamp: Long = System.currentTimeMillis()
      // 随机获取的随机城市
      val randomCity: CityInfo = cityRandomOptions.getRandomOpt
      val area: String = randomCity.area
      val city_name: String = randomCity.city_name
      val userId: Int = 1 + random.nextInt(6)
      val adid: Int = 1 + random.nextInt(6)

//      mockdataString.addString(new StringBuilder().append(timeStamp).append(" ").append(area).append(" ").append(city_name).append(" ").append(userId).append(" ").append(adid))
      mockdataString += timeStamp + " " + area + " " + city_name + " " + userId + " " + adid

    }

    mockdataString.toArray
  }

  def main(args: Array[String]): Unit = {

    // 读取配置文件
    val config: Properties = PropertiesUtil.load("config.properties")
    val brokers: String = config.getProperty("kafka.broker.list")
    val topic: String = config.getProperty("kafka.topic")


    val kafkaProperties = new Properties()
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])

    val kafkaProducer = new KafkaProducer[String, String](kafkaProperties)

    val data: Array[String] = generateMockData
    while(true){
      //随机生产的实时数据通过kafka生产者发送到kafka集群中
      for (line <- data){
        kafkaProducer.send(new ProducerRecord[String,String](topic,line))
        println(line)
      }
      Thread.sleep(2000)
    }

  }


}

//城市信息表： city_id :城市id  city_name：城市名称   area：城市所在大区
case class CityInfo(city_id: Long, city_name: String, area: String)

