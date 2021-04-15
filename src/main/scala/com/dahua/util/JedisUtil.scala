package com.dahua.util


import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object JedisUtil {

  val jedisTool = new JedisPool(new GenericObjectPoolConfig,"192.168.137.151",6379,300000,null,8)

  def resource  = jedisTool.getResource

  def main(args: Array[String]): Unit = {
    println(resource)
  }


}
