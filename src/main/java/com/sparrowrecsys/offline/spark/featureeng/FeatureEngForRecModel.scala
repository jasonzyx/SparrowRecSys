package com.sparrowrecsys.offline.spark.featureeng

import com.sparrowrecsys.online.factory.JedisFactory
import com.sparrowrecsys.online.util.Config._
import com.sparrowrecsys.online.util.Constants._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{format_number, _}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

import scala.collection.immutable.ListMap
import scala.collection.{JavaConversions, mutable}

object FeatureEngForRecModel {

  val NUMBER_PRECISION = 2

  def addSampleLabel(ratingSamples:DataFrame): DataFrame ={
    ratingSamples.show(10, truncate = false)
    ratingSamples.printSchema()
    val sampleCount = ratingSamples.count()
    ratingSamples.groupBy(col("rating")).count().orderBy(col("rating"))
      .withColumn("percentage", col("count")/sampleCount).show(100,truncate = false)

    ratingSamples.withColumn("label", when(col("rating") >= 3.5, 1).otherwise(0))
  }

  def addMovieFeatures(movieSamples:DataFrame, ratingSamples:DataFrame): DataFrame ={

    //add movie basic features
    val samplesWithMovies1 = ratingSamples.join(movieSamples, Seq(MOVIE_ID), "left")
    //add release year
    val extractReleaseYearUdf = udf({(title: String) => {
      if (null == title || title.trim.length < 6) {
        1990 // default value
      }
      else {
        val yearString = title.trim.substring(title.length - 5, title.length - 1)
        yearString.toInt
      }
    }})

    //add title
    val extractTitleUdf = udf({(title: String) => {title.trim.substring(0, title.trim.length - 6).trim}})

    val samplesWithMovies2 = samplesWithMovies1.withColumn(FEATURE_MOVIE_RELEASE_YEAR, extractReleaseYearUdf(col("title")))
      .withColumn("title", extractTitleUdf(col("title")))
      .drop("title")  //title is useless currently

    //split genres
    val samplesWithMovies3 = samplesWithMovies2.withColumn(FEATURE_MOVIE_GENRE_1,split(col("genres"),"\\|").getItem(0))
      .withColumn(FEATURE_MOVIE_GENRE_2,split(col("genres"),"\\|").getItem(1))
      .withColumn(FEATURE_MOVIE_GENRE_3,split(col("genres"),"\\|").getItem(2))

    //add rating features
    val movieRatingFeatures = samplesWithMovies3.groupBy(col(MOVIE_ID))
      .agg(count(lit(1)).as("movieRatingCount"),
        format_number(avg(col("rating")), NUMBER_PRECISION).as(FEATURE_MOVIE_AVG_RATING),
        stddev(col("rating")).as(FEATURE_MOVIE_RATING_STDDEV))
    .na.fill(0).withColumn(FEATURE_MOVIE_RATING_STDDEV,format_number(col(FEATURE_MOVIE_RATING_STDDEV), NUMBER_PRECISION))


    //join movie rating features
    val samplesWithMovies4 = samplesWithMovies3.join(movieRatingFeatures, Seq(MOVIE_ID), "left")
    samplesWithMovies4.printSchema()
    samplesWithMovies4.show(10, truncate = false)

    samplesWithMovies4
  }

  val extractGenres: UserDefinedFunction = udf { (genreArray: Seq[String]) => {
    val genreMap = mutable.Map[String, Int]()
    genreArray.foreach((element:String) => {
      val genres = element.split("\\|")
      genres.foreach((oneGenre:String) => {
        genreMap(oneGenre) = genreMap.getOrElse[Int](oneGenre, 0)  + 1
      })
    })
    val sortedGenres = ListMap(genreMap.toSeq.sortWith(_._2 > _._2):_*)
    sortedGenres.keys.toSeq
  }}

  def addUserFeatures(ratingSamples:DataFrame): DataFrame ={
    val samplesWithUserFeatures = ratingSamples
      .withColumn("userPositiveHistory", collect_list(when(col("label") === 1, col(MOVIE_ID)).otherwise(lit(null)))
        .over(Window.partitionBy(USER_ID)
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userPositiveHistory", reverse(col("userPositiveHistory")))
      .withColumn(FEATURE_USER_RATED_MOVIE_1,col("userPositiveHistory").getItem(0))
      .withColumn(FEATURE_USER_RATED_MOVIE_2,col("userPositiveHistory").getItem(1))
      .withColumn(FEATURE_USER_RATED_MOVIE_3,col("userPositiveHistory").getItem(2))
      .withColumn(FEATURE_USER_RATED_MOVIE_4,col("userPositiveHistory").getItem(3))
      .withColumn(FEATURE_USER_RATED_MOVIE_5,col("userPositiveHistory").getItem(4))
      .withColumn(FEATURE_USER_RATING_COUNT, count(lit(1))
        .over(Window.partitionBy(USER_ID)
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn(FEATURE_USER_RELEASE_YEAR, avg(col(FEATURE_MOVIE_RELEASE_YEAR))
        .over(Window.partitionBy(USER_ID)
          .orderBy(col("timestamp")).rowsBetween(-100, -1)).cast(IntegerType))
      .withColumn(FEATURE_USER_RELEASE_YEAR_STDDEV, stddev(col(FEATURE_MOVIE_RELEASE_YEAR))
        .over(Window.partitionBy(USER_ID)
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn(FEATURE_USER_AVG_RATING, format_number(avg(col("rating"))
        .over(Window.partitionBy(USER_ID)
          .orderBy(col("timestamp")).rowsBetween(-100, -1)), NUMBER_PRECISION))
      .withColumn(FEATURE_USER_RATING_STDDEV, stddev(col("rating"))
        .over(Window.partitionBy(USER_ID)
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      .withColumn("userGenres", extractGenres(collect_list(when(col("label") === 1, col("genres")).otherwise(lit(null)))
        .over(Window.partitionBy(USER_ID)
          .orderBy(col("timestamp")).rowsBetween(-100, -1))))
      .na.fill(0)
      .withColumn(FEATURE_USER_RATING_STDDEV,format_number(col(FEATURE_USER_RATING_STDDEV), NUMBER_PRECISION))
      .withColumn(FEATURE_USER_RELEASE_YEAR_STDDEV,format_number(col(FEATURE_USER_RELEASE_YEAR_STDDEV), NUMBER_PRECISION))
      .withColumn(FEATURE_USER_GENRE_1,col("userGenres").getItem(0))
      .withColumn(FEATURE_USER_GENRE_2,col("userGenres").getItem(1))
      .withColumn(FEATURE_USER_GENRE_3,col("userGenres").getItem(2))
      .withColumn(FEATURE_USER_GENRE_4,col("userGenres").getItem(3))
      .withColumn(FEATURE_USER_GENRE_5,col("userGenres").getItem(4))
      .drop("genres", "userGenres", "userPositiveHistory")
      .filter(col(FEATURE_USER_RATING_COUNT) > 1)

    samplesWithUserFeatures.printSchema()
    samplesWithUserFeatures.show(100, truncate = false)

    samplesWithUserFeatures
  }

  def extractAndSaveMovieFeaturesToRedis(samples:DataFrame): DataFrame = {
    val movieLatestSamples = samples.withColumn("movieRowNum", row_number()
      .over(Window.partitionBy(MOVIE_ID)
        .orderBy(col("timestamp").desc)))
      .filter(col("movieRowNum") === 1)
      .select(MOVIE_ID,FEATURE_MOVIE_RELEASE_YEAR, FEATURE_MOVIE_GENRE_1,FEATURE_MOVIE_GENRE_2,FEATURE_MOVIE_GENRE_3,"movieRatingCount",
        FEATURE_MOVIE_AVG_RATING, FEATURE_MOVIE_RATING_STDDEV)
      .na.fill("")

    movieLatestSamples.printSchema()
    movieLatestSamples.show(100, truncate = false)

    val movieFeaturePrefix = "mf:"

    val jedisFactory = new JedisFactory();
    val redisClient = jedisFactory.createRedisClient(REDIS_ENDPOINT, REDIS_PORT);

    val params = SetParams.setParams()
    //set ttl to 24hs * 30
    params.ex(60 * 60 * 24 * 30)
    val sampleArray = movieLatestSamples.collect()
    println("total movie size:" + sampleArray.length)
    var insertedMovieNumber = 0
    val movieCount = sampleArray.length
    for (sample <- sampleArray){
      val movieKey = movieFeaturePrefix + sample.getAs[String](MOVIE_ID)
      val valueMap = mutable.Map[String, String]()
      valueMap(FEATURE_MOVIE_GENRE_1) = sample.getAs[String](FEATURE_MOVIE_GENRE_1)
      valueMap(FEATURE_MOVIE_GENRE_2) = sample.getAs[String](FEATURE_MOVIE_GENRE_2)
      valueMap(FEATURE_MOVIE_GENRE_3) = sample.getAs[String](FEATURE_MOVIE_GENRE_3)
      valueMap(FEATURE_MOVIE_RATING_COUNT) = sample.getAs[Long](FEATURE_MOVIE_RATING_COUNT).toString
      valueMap(FEATURE_MOVIE_RELEASE_YEAR) = sample.getAs[Int](FEATURE_MOVIE_RELEASE_YEAR).toString
      valueMap(FEATURE_MOVIE_AVG_RATING) = sample.getAs[String](FEATURE_MOVIE_AVG_RATING)
      valueMap(FEATURE_MOVIE_RATING_STDDEV) = sample.getAs[String](FEATURE_MOVIE_RATING_STDDEV)

      redisClient.hset(movieKey, JavaConversions.mapAsJavaMap(valueMap))
      insertedMovieNumber += 1
      if (insertedMovieNumber % 100 ==0){
        println(insertedMovieNumber + "/" + movieCount + "...")
      }
    }

    redisClient.close()
    movieLatestSamples
  }

  def splitAndSaveTrainingTestSamples(samples:DataFrame, savePath:String)={
    //generate a smaller sample set for demo
    val smallSamples = samples.sample(0.1)

    //split training and test set by 8:2
    val Array(training, test) = smallSamples.randomSplit(Array(0.8, 0.2))

    val sampleResourcesPath = this.getClass.getResource(savePath)
    training.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/trainingSamples")
    test.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/testSamples")
  }

  def splitAndSaveTrainingTestSamplesByTimeStamp(samples:DataFrame, savePath:String)={
    //generate a smaller sample set for demo
    val smallSamples = samples.sample(0.1).withColumn("timestampLong", col("timestamp").cast(LongType))

    val quantile = smallSamples.stat.approxQuantile("timestampLong", Array(0.8), 0.05)
    val splitTimestamp = quantile.apply(0)

    val training = smallSamples.where(col("timestampLong") <= splitTimestamp).drop("timestampLong")
    val test = smallSamples.where(col("timestampLong") > splitTimestamp).drop("timestampLong")

    val sampleResourcesPath = this.getClass.getResource(savePath)
    training.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/trainingSamples")
    test.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/testSamples")
  }


  def extractAndSaveUserFeaturesToRedis(samples:DataFrame): DataFrame = {
    val userLatestSamples = samples.withColumn("userRowNum", row_number()
      .over(Window.partitionBy(USER_ID)
        .orderBy(col("timestamp").desc)))
      .filter(col("userRowNum") === 1)
      .select(USER_ID,FEATURE_USER_RATED_MOVIE_1, FEATURE_USER_RATED_MOVIE_2, FEATURE_USER_RATED_MOVIE_3,
        FEATURE_USER_RATED_MOVIE_4,FEATURE_USER_RATED_MOVIE_5, FEATURE_USER_RATING_COUNT, FEATURE_USER_RELEASE_YEAR,
        FEATURE_USER_RELEASE_YEAR_STDDEV, FEATURE_USER_AVG_RATING, FEATURE_USER_RATING_STDDEV, FEATURE_USER_GENRE_1,
        FEATURE_USER_GENRE_2, FEATURE_USER_GENRE_3, FEATURE_USER_GENRE_4, FEATURE_USER_GENRE_5)
      .na.fill("")

    userLatestSamples.printSchema()
    userLatestSamples.show(100, truncate = false)

    val redisClient = new Jedis(REDIS_ENDPOINT, REDIS_PORT)
    val params = SetParams.setParams()
    //set ttl to 24hs * 30
    params.ex(60 * 60 * 24 * 30)
    val sampleArray = userLatestSamples.collect()
    println("total user size:" + sampleArray.length)
    var insertedUserNumber = 0
    val userCount = sampleArray.length
    for (sample <- sampleArray){
      val userKey = USER_FEATURE_PREFIX + sample.getAs[String](USER_ID)
      val valueMap = mutable.Map[String, String]()
      valueMap(FEATURE_USER_RATED_MOVIE_1) = sample.getAs[String](FEATURE_USER_RATED_MOVIE_1)
      valueMap(FEATURE_USER_RATED_MOVIE_2) = sample.getAs[String](FEATURE_USER_RATED_MOVIE_2)
      valueMap(FEATURE_USER_RATED_MOVIE_3) = sample.getAs[String](FEATURE_USER_RATED_MOVIE_3)
      valueMap(FEATURE_USER_RATED_MOVIE_4) = sample.getAs[String](FEATURE_USER_RATED_MOVIE_4)
      valueMap(FEATURE_USER_RATED_MOVIE_5) = sample.getAs[String](FEATURE_USER_RATED_MOVIE_5)
      valueMap(FEATURE_USER_GENRE_1) = sample.getAs[String](FEATURE_USER_GENRE_1)
      valueMap(FEATURE_USER_GENRE_2) = sample.getAs[String](FEATURE_USER_GENRE_2)
      valueMap(FEATURE_USER_GENRE_3) = sample.getAs[String](FEATURE_USER_GENRE_3)
      valueMap(FEATURE_USER_GENRE_4) = sample.getAs[String](FEATURE_USER_GENRE_4)
      valueMap(FEATURE_USER_GENRE_5) = sample.getAs[String](FEATURE_USER_GENRE_5)
      valueMap(FEATURE_USER_RATING_COUNT) = sample.getAs[Long](FEATURE_USER_RATING_COUNT).toString
      valueMap(FEATURE_USER_RELEASE_YEAR) = sample.getAs[Int](FEATURE_USER_RELEASE_YEAR).toString
      valueMap(FEATURE_USER_RELEASE_YEAR_STDDEV) = sample.getAs[String](FEATURE_USER_RELEASE_YEAR_STDDEV)
      valueMap(FEATURE_USER_AVG_RATING) = sample.getAs[String](FEATURE_USER_AVG_RATING)
      valueMap(FEATURE_USER_RATING_STDDEV) = sample.getAs[String](FEATURE_USER_RATING_STDDEV)

      redisClient.hset(userKey, JavaConversions.mapAsJavaMap(valueMap))
      insertedUserNumber += 1
      if (insertedUserNumber % 100 ==0){
        println(insertedUserNumber + "/" + userCount + "...")
      }
    }

    redisClient.close()
    userLatestSamples
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("featureEngineering")
      .set("spark.submit.deployMode", "client")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    val movieResourcesPath = this.getClass.getResource("/webroot/sampledata/movies.csv")
    val movieSamples = spark.read.format("csv").option("header", "true").load(movieResourcesPath.getPath)

    val ratingsResourcesPath = this.getClass.getResource("/webroot/sampledata/ratings.csv")
    val ratingSamples = spark.read.format("csv").option("header", "true").load(ratingsResourcesPath.getPath)

    val ratingSamplesWithLabel = addSampleLabel(ratingSamples)
    ratingSamplesWithLabel.show(10, truncate = false)

    val samplesWithMovieFeatures = addMovieFeatures(movieSamples, ratingSamplesWithLabel)
    val samplesWithUserFeatures = addUserFeatures(samplesWithMovieFeatures)


    //save samples as csv format
    splitAndSaveTrainingTestSamples(samplesWithUserFeatures, "/webroot/sampledata")

    //save user features and item features to redis for online inference
    extractAndSaveUserFeaturesToRedis(samplesWithUserFeatures)
    extractAndSaveMovieFeaturesToRedis(samplesWithUserFeatures)

    println("Feature-eng finished.")
    spark.close()
  }

}
