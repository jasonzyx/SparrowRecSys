package com.sparrowrecsys.online.util;

import static com.sparrowrecsys.online.util.Constants.*;

public class ABTest {
    final static int trafficSplitNumber = 5;

    final static String bucketAModel = EMBEDDING_MLP;
    final static String bucketBModel = NEURAL_CF;
    final static String bucketCModel = WIDE_N_DEEP;
    final static String defaultModel = EMBEDDING;

    public static String getConfigByUserId(String userId){
        if (null == userId || userId.isEmpty()){
            return defaultModel;
        }

        if (userId.hashCode() % trafficSplitNumber == 0){
            System.out.println(userId + " is in bucketA.");
            return bucketAModel;
        } else if(userId.hashCode() % trafficSplitNumber == 1){
            System.out.println(userId + " is in bucketB.");
            return bucketBModel;
        } else if(userId.hashCode() % trafficSplitNumber == 2){
            System.out.println(userId + " is in bucketC.");
            return bucketCModel;
        } else{
            System.out.println(userId + " is in control bucket");
            return defaultModel;
        }
    }
}
