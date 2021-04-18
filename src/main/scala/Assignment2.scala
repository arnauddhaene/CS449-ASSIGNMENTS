// Author: Arnaud Patrick Elias Dhaene

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd._


object Assignment2 {
//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------
    def main(args: Array[String]) = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        val spark = SparkSession.builder
                                .appName("DBLP").getOrCreate()

        val start = System.nanoTime

        val result = run(loadData(spark)).take(20)

        val duration = (System.nanoTime - start) / 1e9d

        println(s"Top 20 pairs of co-authors and their number of 21st century publications [${duration} sec]:")
        result.foreach(println)

        spark.stop()
    }

//------------------------------------------------------------------------------
    case class MapInput(year: Long, authors: List[String])
    
    type AuthorPairCount = ((String, String), Int)

    def run(publications: RDD[MapInput]): RDD[AuthorPairCount] = {
        return mapReduce(publications, mapper, reducer)
    } 

//------------------------------------------------------------------------------
//  MapReduce
//------------------------------------------------------------------------------
    def mapReduce(publications: RDD[MapInput],
                  map: MapInput => List[AuthorPairCount],
                  reduce: (Int, Int) => Int): RDD[AuthorPairCount] = {

        return publications
            // filter out last century's publications
            .filter(_.year > 2000)
            // flatMap to RDD[AuthorPairCount]
            .flatMap(publication => map(publication))
            // aggregate all publications for pairs of co-authors
            .reduceByKey(reduce)
            // sort by co-authors with most publications
            .sortBy(_._2, ascending = false)
    } 

//------------------------------------------------------------------------------
    def mapper(pub: MapInput): List[AuthorPairCount] = {

        // edge case - `pub.authors` is empty
        // this should not be the case as already filtered in `loadData`
        // return an empty list as `sorted` would throw an error
        
        if (pub.authors.isEmpty) return List()

        // edge case - `pub.authors` contains only one author
        // the resulting author pairs will be an empty List

        // edge case - `pub.authors` contains a duplicate author
        // the author list is reduced to a unique list with `distinct`
        
        return pub.authors
            // remove any potential duplicate authors
            .distinct
            // automatically generates list of (u, v) with u smaller than v
            .sorted
            // generate all pairwise combinations
            .combinations(2)
            // luv (which is my nomenclature for List[(u, v)]) has length 2
            .map { case (luv) => ((luv(0), luv(1)), 1) }
            // transform scala iterator to List
            .toList
    }

//------------------------------------------------------------------------------
    def reducer(a: Int, b: Int) : Int = a + b

//------------------------------------------------------------------------------
//  Auxiliary
//------------------------------------------------------------------------------
    def loadData(spark: SparkSession) = {
        import spark.implicits._
        val datafile = "/cs449/dblp/publications.json"
        spark.read
             .json(datafile)
             .filter(col("year").isNotNull && col("authors").isNotNull)
             .select("year", "authors")
             .as[MapInput].rdd
    }
//------------------------------------------------------------------------------
}
