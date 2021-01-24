package com.sparrowrecsys.online.recprocess;

import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.Movie;
import com.sparrowrecsys.online.datamanager.RedisClient;
import com.sparrowrecsys.online.datamanager.User;
import com.sparrowrecsys.online.util.Config;
import com.sparrowrecsys.online.util.Utility;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.sparrowrecsys.online.util.Constants.*;
import static com.sparrowrecsys.online.util.HttpClient.*;

/**
 * Recommendation process of similar movies
 */

public class RecForYouProcess {

    /**
     * get recommendation movie list
     * @param userId input user id
     * @param size  size of similar items
     * @param model model used for calculating similarity
     * @return  list of similar movies
     */
    public static List<Movie> getRecList(int userId, int size, String model){
        User user = DataManager.getInstance().getUserById(userId);
        if (null == user){
            return new ArrayList<>();
        }
        final int CANDIDATE_SIZE = 800;
        List<Movie> candidates = DataManager.getInstance().getMovies(CANDIDATE_SIZE, "rating");

        //load user emb from redis if data source is redis
        if (Config.EMB_DATA_SOURCE.equals(Config.DATA_SOURCE_REDIS)) {
            String userEmbKey = "uEmb:" + userId;
            String userEmb = RedisClient.getInstance().get(userEmbKey);
            if (null != userEmb){
                user.setEmb(Utility.parseEmbStr(userEmb));
            }
        }

        if (Config.IS_LOAD_USER_FEATURE_FROM_REDIS) {
            String userFeaturesKey = USER_FEATURE_PREFIX + userId;
            Map<String, String> userFeatures = RedisClient.getInstance().hgetAll(userFeaturesKey);
            if (null != userFeatures){
                user.setUserFeatures(userFeatures);
            }
        }

        List<Movie> rankedList = ranker(user, candidates, model);

        if (rankedList.size() > size){
            return rankedList.subList(0, size);
        }
        return rankedList;
    }

    /**
     * rank candidates
     * @param user    input user
     * @param candidates    movie candidates
     * @param model     model name used for ranking
     * @return  ranked movie list
     */
    public static List<Movie> ranker(User user, List<Movie> candidates, String model){
        HashMap<Movie, Double> candidateScoreMap = new HashMap<>();
        HashSet<String> modelSet = new HashSet<>(Arrays.asList(NEURAL_CF, WIDE_N_DEEP, EMBEDDING_MLP));

        System.out.println("[DEBUG]: the requested model is: " + model);
        if (EMBEDDING.equals(model)) {
            System.out.println("[DEBUG] now using model: " + EMBEDDING);
            for (Movie candidate : candidates){
                double similarity = calculateEmbSimilarScore(user, candidate);
                candidateScoreMap.put(candidate, similarity);
            }
        } else if (modelSet.contains(model)) {
            callModelServing(user, candidates, candidateScoreMap, model);
        } else {
            // default ranking in candidate set
            System.out.println("[DEBUG] now using model: fallback");
            for (int i = 0 ; i < candidates.size(); i++){
                candidateScoreMap.put(candidates.get(i), (double)(candidates.size() - i));
            }
        }

        List<Movie> rankedList = new ArrayList<>();
        candidateScoreMap.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(m -> rankedList.add(m.getKey()));
        return rankedList;
    }

    /**
     * function to calculate similarity score based on embedding
     * @param user     input user
     * @param candidate candidate movie
     * @return  similarity score
     */
    public static double calculateEmbSimilarScore(User user, Movie candidate){
        if (null == user || null == candidate || null == user.getEmb()){
            return -1;
        }
        return user.getEmb().calculateSimilarity(candidate.getEmb());
    }

    /**
     * call TenserFlow serving to get model inference result
     * @param user              input user
     * @param candidates        candidate movies
     * @param candidateScoreMap save prediction score into the score map
     * @param model             model name
     */
    public static void callModelServing(User user, List<Movie> candidates, HashMap<Movie, Double> candidateScoreMap, String model){
        if (null == user || null == candidates || candidates.size() == 0){
            return;
        }

        JSONArray instances = new JSONArray();
        for (Movie m : candidates){
            JSONObject instance = new JSONObject();
            instance.put(USER_ID, user.getUserId());
            instance.put(MOVIE_ID, m.getMovieId());
            if (EMBEDDING_MLP.equals(model) || WIDE_N_DEEP.equals(model)) {
                // movie features
                instance.put(FEATURE_MOVIE_GENRE_1,m.getMovieFeatures().get(FEATURE_MOVIE_GENRE_1));
                instance.put(FEATURE_MOVIE_GENRE_2,m.getMovieFeatures().get(FEATURE_MOVIE_GENRE_2));
                instance.put(FEATURE_MOVIE_GENRE_3,m.getMovieFeatures().get(FEATURE_MOVIE_GENRE_3));
                instance.put(FEATURE_MOVIE_RELEASE_YEAR, Integer.parseInt(m.getMovieFeatures().get(FEATURE_MOVIE_RELEASE_YEAR)));
                instance.put(FEATURE_MOVIE_RATING_COUNT, Integer.parseInt(m.getMovieFeatures().get(FEATURE_MOVIE_RATING_COUNT)));
                instance.put(FEATURE_MOVIE_AVG_RATING, Float.parseFloat(m.getMovieFeatures().get(FEATURE_MOVIE_AVG_RATING)));
                instance.put(FEATURE_MOVIE_RATING_STDDEV, Float.parseFloat(m.getMovieFeatures().get(FEATURE_MOVIE_RATING_STDDEV)));
                // user features
                instance.put(FEATURE_USER_RATING_COUNT, Integer.parseInt(user.getUserFeatures().get(FEATURE_USER_RATING_COUNT)));
                instance.put(FEATURE_USER_AVG_RATING, Float.parseFloat(user.getUserFeatures().get(FEATURE_USER_AVG_RATING)));
                instance.put(FEATURE_USER_GENRE_1, user.getUserFeatures().get(FEATURE_USER_GENRE_1));
                instance.put(FEATURE_USER_GENRE_2, user.getUserFeatures().get(FEATURE_USER_GENRE_2));
                instance.put(FEATURE_USER_GENRE_3, user.getUserFeatures().get(FEATURE_USER_GENRE_3));
                instance.put(FEATURE_USER_GENRE_4, user.getUserFeatures().get(FEATURE_USER_GENRE_4));
                instance.put(FEATURE_USER_GENRE_5, user.getUserFeatures().get(FEATURE_USER_GENRE_5));
                instance.put(FEATURE_USER_RATING_STDDEV, Float.parseFloat(user.getUserFeatures().get(FEATURE_USER_RATING_STDDEV)));
            }
            if (WIDE_N_DEEP.equals(model)) {
                instance.put(FEATURE_USER_RATED_MOVIE_1, Integer.parseInt(user.getUserFeatures().get(FEATURE_USER_RATED_MOVIE_1)));
            }

            instances.put(instance);
        }

        JSONObject instancesRoot = new JSONObject();
        instancesRoot.put("instances", instances);

        //need to confirm the tf serving end point
        String host = "http://localhost:8501/v1/models/" + model + ":predict";
        String predictionScores = asyncSinglePostRequest(host, instancesRoot.toString());
        System.out.println("[DEBUG] now using model: " + model);
        System.out.println("[DEBUG] send user" + user.getUserId() + " request to tf serving.");
        System.out.println("[DEBUG] instancesRoot string is: " + instancesRoot.toString().substring(0, 200) + "...");
        System.out.println("[DEBUG] prediction score is: " + predictionScores);

        JSONObject predictionsObject = new JSONObject(predictionScores);
        JSONArray scores = predictionsObject.getJSONArray("predictions");
        for (int i = 0 ; i < candidates.size(); i++){
            candidateScoreMap.put(candidates.get(i), scores.getJSONArray(i).getDouble(0));
        }
    }

}
