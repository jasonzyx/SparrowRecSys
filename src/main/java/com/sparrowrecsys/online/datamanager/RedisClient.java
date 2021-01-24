package com.sparrowrecsys.online.datamanager;

import redis.clients.jedis.Jedis;

import static com.sparrowrecsys.online.util.Config.*;


public class RedisClient {
    //singleton Jedis
    private static volatile Jedis redisClient;

    private RedisClient(){
        redisClient = new Jedis(REDIS_ENDPOINT, REDIS_PORT);
    }

    public static Jedis getInstance(){
        if (null == redisClient){
            synchronized (RedisClient.class){
                if (null == redisClient){
                    redisClient = new Jedis(REDIS_ENDPOINT, REDIS_PORT);
                }
            }
        }
        return redisClient;
    }
}
