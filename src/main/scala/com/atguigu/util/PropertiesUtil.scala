package com.atguigu.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * @author xystart
 * @create 2020-11-24 20:23
 *         编写读取资源文件工具类
 *
 */
object PropertiesUtil {

  def load(propertiesName: String): Properties = {
    val prop = new Properties()

    prop.load(new InputStreamReader(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(propertiesName), StandardCharsets.UTF_8))
    prop
  }

}
