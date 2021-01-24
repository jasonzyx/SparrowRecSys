package com.sparrowrecsys.online.util;


public class Config {
    public static final String DATA_SOURCE_REDIS = "redis";
    public static final String DATA_SOURCE_FILE = "file";

    public static final int REDIS_PORT = 6379;
    public static final String REDIS_ENDPOINT = "localhost";

    public static final int DEFAULT_REC_SYS_PORT = 6010;

    public static String EMB_DATA_SOURCE = Config.DATA_SOURCE_REDIS;
    public static boolean IS_LOAD_USER_FEATURE_FROM_REDIS = true;
    public static boolean IS_LOAD_ITEM_FEATURE_FROM_REDIS = true;

    public static boolean IS_ENABLE_AB_TEST = false;

}
