// Author: Arnaud Patrick Elias Dhaene

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd._


object Assignment1 {
//------------------------------------------------------------------------------
    def main(args: Array[String]) = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        val spark = SparkSession.builder
                                .appName("KNN").getOrCreate()

        val start = System.nanoTime

        val result = find10Closest(185544, loadData(spark))

        val duration = (System.nanoTime - start) / 1e9d

        println(s"The 10 nearest neighbours of user 185544 [${duration} sec]:")
        result.foreach(println)
        
        spark.stop()
    }

//------------------------------------------------------------------------------
    case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Int)

    def find10Closest(user: Int,
                      ratings: RDD[Rating]): Array[(Int, Double)] = {
        
        val likedMovies = ratings.map(r => (r.userId, (r.movieId, r.rating)))
            // remove all ratings of 3 or lower
            .filter { case (u, (i, r)) => r > 3}
            // remove ratings as we don't need them
            .map { case (u, (i, r)) => (u, i)}
            // group by user
            .groupByKey()
            // sanity check -- there should be no empty `li`
            // remove all users who have never given a score greater than 3
            .filter { case (u, li) => !li.isEmpty }
        
        val similarities = likedMovies.filter { case (u, lir) => u == user }
            // compute the cartesian product of one user with all other users
            .cartesian(likedMovies.filter { case (u, lir) => u != user })
            // compute the jaccard similarity metric for each pair of users
            .map { case ((u, uli), (v, vli)) => (v, jaccard(uli.toSet, vli.toSet)) }
            // sort by the similarity in a descending fashion
            .sortBy(_._2, ascending = false)

        // edge case - if user is not found in the `likedMovies` RDD
        // the resulting cartesian product will be empty
        // no errors will be thrown and no result will be printed
            
        return similarities
            // select the top 10 similarities in as an Array of
            // (neighbourId, jaccardSimilarity) tuples
            .take(10)
        
    } 

//------------------------------------------------------------------------------
    def jaccard(a: Set[Int], b: Set[Int]): Double = {
        // edge case - division by zero
        // this will never occur as we filter out empty `li` in line 46
        // just in case, we check here
        val in = (a intersect b).size.toDouble
        val un = (a union b).size.toDouble

        return if (un == 0.0) (0.0) else (in / un)
    } 

//------------------------------------------------------------------------------
//  Auxiliary
//------------------------------------------------------------------------------
    def loadData(spark: SparkSession) = {
        import spark.implicits._
        val datafile = "/cs449/movielens/ml-latest/ratings.csv"
        spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
             .csv(datafile)
             .as[Rating].rdd
    }
//------------------------------------------------------------------------------
}
