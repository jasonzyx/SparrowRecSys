package com.sparrowrecsys.online.factory;

import redis.clients.jedis.Jedis;


public class JedisFactory {

  public Jedis createRedisClient(String redisEndpoint, int redisPort) {
    return new Jedis(redisEndpoint, redisPort);
  }

}
