package com.sparrowrecsys.online.util;

public class Constants {
  // movie and user features
  public static final String USER_ID = "userId";
  public static final String MOVIE_ID = "movieId";
  public static final String USER_FEATURE_PREFIX= "uf:";
  public static final String REDIS_KEY_PREFIX_USER_EMBEDDING = "uEmb";
  public static final String REDIS_KEY_PREFIX_ITEM2VEC_EMBEDDING = "i2vEmb";

  public static final String FEATURE_MOVIE_GENRE_1 = "movieGenre1";
  public static final String FEATURE_MOVIE_GENRE_2 = "movieGenre2";
  public static final String FEATURE_MOVIE_GENRE_3 = "movieGenre3";
  public static final String FEATURE_MOVIE_RATING_COUNT= "movieRatingCount";
  public static final String FEATURE_MOVIE_AVG_RATING = "movieAvgRating";
  public static final String FEATURE_MOVIE_RATING_STDDEV= "movieRatingStddev";
  public static final String FEATURE_MOVIE_RELEASE_YEAR = "releaseYear";

  public static final String FEATURE_USER_GENRE_1 = "userGenre1";
  public static final String FEATURE_USER_GENRE_2 = "userGenre2";
  public static final String FEATURE_USER_GENRE_3 = "userGenre3";
  public static final String FEATURE_USER_GENRE_4 = "userGenre4";
  public static final String FEATURE_USER_GENRE_5 = "userGenre5";
  public static final String FEATURE_USER_RATING_COUNT = "userRatingCount";
  public static final String FEATURE_USER_AVG_RATING = "userAvgRating";
  public static final String FEATURE_USER_RATING_STDDEV = "userRatingStddev";
  public static final String FEATURE_USER_RATED_MOVIE_1 = "userRatedMovie1";
  public static final String FEATURE_USER_RATED_MOVIE_2 = "userRatedMovie2";
  public static final String FEATURE_USER_RATED_MOVIE_3 = "userRatedMovie3";
  public static final String FEATURE_USER_RATED_MOVIE_4 = "userRatedMovie4";
  public static final String FEATURE_USER_RATED_MOVIE_5 = "userRatedMovie5";

  public static final String FEATURE_USER_RELEASE_YEAR = "userAvgReleaseYear";
  public static final String FEATURE_USER_RELEASE_YEAR_STDDEV = "userReleaseYearStddev";

  // model names
  public static final String EMBEDDING = "emb";
  public static final String WIDE_N_DEEP = "widendeep";
  public static final String NEURAL_CF = "neuralcf";















}
