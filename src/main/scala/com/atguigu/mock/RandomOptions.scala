package com.atguigu.mock

import java.util.Random

import scala.collection.mutable.ListBuffer

/**
 * @author xystart
 * @create 2020-11-24 20:48
 *         根据权重，生成对应随机数
 */
object RandomOptions {
  def apply[T](opts: RanOpt[T]*): RandomOptions[T] = {
    val randomOptions = new RandomOptions[T]()

    for (opt <- opts) {
      randomOptions.totalWeight += opt.weight

      // 根据每个元素的自己的权重，向buffer中存储数据。权重越多存储的越多
      for (i <- 1 to opt.weight) {
        // 男 男 男 男 男 男 男 男 女 女
        randomOptions.optsBuffer += opt.value
      }
    }
    randomOptions

  }

//  def main(args: Array[String]): Unit = {
//    val randomOptions: RandomOptions[String] = RandomOptions(RanOpt("男", 7), RanOpt("女", 3))
//    for (i <- 1 to 10){
//      println(randomOptions.getRandomOpt)
//    }
//  }

}


// value值出现的比例，例如：(男，8) (女:2)
case class RanOpt[T](value: T, weight: Int)

class RandomOptions[T](opts: RanOpt[T]*) {
  var totalWeight = 0
  var optsBuffer = new ListBuffer[T]

  def getRandomOpt: T = {
    //随机取：0-9
    val randomNum: Int = new Random().nextInt(totalWeight)

    optsBuffer(randomNum)
  }
}