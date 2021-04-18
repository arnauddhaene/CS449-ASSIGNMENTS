// Author: Arnaud Patrick Elias Dhaene

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._


object Assignment3 {
//------------------------------------------------------------------------------
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val spark = SparkSession.builder
                            .appName("SparkSQL").getOrCreate()

//------------------------------------------------------------------------------
    def main(args: Array[String]) = {

        val start = System.nanoTime

        val result = run(loadData()).show(50)

        val duration = (System.nanoTime - start) / 1e9d

        println(s"The 50 most frequent genome tags (+ conditions) [${duration} sec].")

        spark.stop()
    }

//------------------------------------------------------------------------------
    case class Tag(tagId: Int, tag: String)


    def run(collections: (DataFrame, DataFrame, DataFrame, DataFrame))
                                                              : Dataset[Tag] = {
        import spark.implicits._
        
        val (genomeScores, genomeTags, links, imdbRatings) = collections

        val filteredImdbRatings = imdbRatings
            // filter out movies with less than 150 votes
            .filter(col("numVotes") >= 150)

        val worstMovies = links
            // format imdbId adequately for joining
            .withColumn("imdbId", format_string("tt%07d", col("imdbId")))
            // join on imdbId
            .join(filteredImdbRatings, col("imdbId") === col("tconst"))
            // sort by imdbId first, in order for results to remain consistent
            .sort(asc("imdbId"))
            // sort by average IMDB rating, from worst to best
            .sort(asc("averageRating"))
            // select the 2000 worst movies
            .select("movieId")
            .limit(2000)

        
        val w = Window.partitionBy("movieId")
            // parition movies by their most relevant tags
            .orderBy(desc("relevance"))

        val twoHundredMostRelevant = worstMovies
            .join(genomeScores, "movieId")
            .withColumn("rn", row_number.over(w))
            // select only 200 most relevant tags
            .where(col("rn") <= 200)
            
        val frequentTags = twoHundredMostRelevant
            // group tags by their id
            .groupBy("tagId")
            // aggregate with a counting function
            .agg(count("tagId").as("count"))

        return frequentTags.join(genomeTags, "tagId")
            // sort from hightest to lowest count
            .sort(desc("count"))
            .select("tagId", "tag")
            .as[Tag]
    }

//------------------------------------------------------------------------------
//  Auxiliary
//------------------------------------------------------------------------------
    def loadData() = {
        import spark.implicits._
        val imdbDir = "/cs449/imdb/"
        val mlensDir = "/cs449/movielens/ml-latest/"
        
        // Files
        val genscoresFile = mlensDir + "genome-scores.csv"
        val gentagsFile = mlensDir + "genome-tags.csv"
        val linksFile = mlensDir + "links.csv"
        val imdbratingsFile = imdbDir + "title.ratings.tsv"

        val opts = Map("header" -> "true", "inferSchema" -> "true")
        val optsTab = opts + ("sep" -> "\t")

        (
            spark.read.options(opts).csv(genscoresFile).as("GenomeScores"),
            spark.read.options(opts).csv(gentagsFile).as("GenomeTags"),
            spark.read.options(opts).csv(linksFile).as("MLensLinks"),
            spark.read.options(optsTab).csv(imdbratingsFile).as("ImdbRatings")
        )
    }
//------------------------------------------------------------------------------
}