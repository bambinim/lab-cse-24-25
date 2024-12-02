package lab104

import lab104.MovieLens.{path_ml_movies, path_ml_ratings}
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Commons

object MovieLensRating {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("MovieLens job").getOrCreate()
    val sqlContext = spark.sqlContext // needed to save as CSV
    import sqlContext.implicits._

    val path_output_avgRatPerGenre = "/output/avgRatPerGenre"

    if(args.length < 1){
      println("The first parameter should indicate the deployment mode (\"local\" or \"remote\")")
      return
    }

    val deploymentMode = args(0)
    var writeMode = deploymentMode
    if(deploymentMode == "sharedRemote"){
      writeMode = "remote"
    }

    val rddMovies = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_ml_movies)).
      flatMap(MovieLensParser.parseMovieLine)
    val rddRatings = spark.sparkContext.
      textFile(Commons.getDatasetPath(deploymentMode, path_ml_ratings)).
      flatMap(MovieLensParser.parseRatingLine)

    // job 1
    rddRatings
      .map({case (_, movieId, rating, _) => (movieId, (rating, 1))})
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .join(rddMovies.map({case (movieId, title, genres) => ((movieId, genres))}).flatMapValues(_.split("""\|""")))
      .map({case (movieId, ((rating, count), genre)) => (genre, (rating, count))})
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map({case (genre, (rating, count)) => (genre, rating / count)})
      .coalesce(1)
      .toDF().write.format("csv").mode(SaveMode.Overwrite)
      .save(Commons.getDatasetPath(writeMode, path_output_avgRatPerGenre))

    // job 2
    rddRatings
      .map({case (_, movieId, rating, _) => (movieId, rating)})
      .join(rddMovies.map({case (movieId, title, genres) => ((movieId, genres))}))
      .flatMap({case (movieId, (rating, genres)) => genres.split("""\|""").map(g => (g, (rating, 1)))})
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map({case (genre, (rating, count)) => (genre, rating / count)})
      .coalesce(1)
      .toDF().write.format("csv").mode(SaveMode.Overwrite)
      .save(Commons.getDatasetPath(writeMode, path_output_avgRatPerGenre))
  }
}
