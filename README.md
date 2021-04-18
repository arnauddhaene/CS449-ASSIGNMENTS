# EPFL CS-449 Systems for Datascience, Programming assignments spring 2021

Programming assignments in Scala with Apache Spark RDDs and DataFrames for CS-449 (EPFL), spring semester of 2021.

## Assigment 1

You want to create a new social network for movie lovers. Apart from using the movie recommendation system that you are building in your project, you want to allow users with similar tastes to interact with each other via a chat interface within your platform.

We are using the MovieLens ml-latest dataset, which is available in HDFS at `/cs449/movielens/ml-latest`.

* Write a program that returns the identifiers and similarity score of the 10 nearest neighbours with whom a user can chat, based on the similarity of their movie rating profiles.
* The similarity metric will be the Jaccard coefficient similarity, where A and B represent the sets of items (i.e., movies) that two different users liked. We will
consider that users liked a movie when they rated it with a score greater than 3.
* Ignore users who have never given a score greater than.

The Jaccard coefficient is defined as follows

> J = |A ∩ B| / |A ∪ B| = |A ∩ B| / |A| + |B| − |A ∩ B|

## Assignment 2


Your movie social network was a huge success, but you refused to introduce ads in your platform and still cannot pay your next holidays in Hawaii with user voluntary donations. Attracted by the visibility you got, a major TV channel succeeded to hire you as data scientist. They are currently interested in producing a documentary about the importance of collaboration in science and ask you to find the people that they should invite for interviews.

A friend tells you about the [DBLP](https://dblp.org/) database, an open collection with more than 5.5M computer science publications as of 01.04.2021. You resolve to use it in order to find people who were co-authors in a large number of works. You decide to use the MapReduce programming model to count how many times each pair of co-authors have worked together in distinct publications that were published in this century (i.e., from 2001).

* Write a MapReduce program that uses the DBLP dataset for this purpose. It is available in HDFS at `/cs449/dblp/publications.json`.
* The map function receives as input a pair containing the year of each publication and its list of authors. As a result, we will have a collection of pairs of authors and the number of publications on which they worked together.
* Finally, sort the obtained results in descending order of number of collaborations (no need to use MapReduce in this part).

## Assignment 3

The documentary was praised by the critics and you got a raise. While enjoying a beach in Honolulu, you mistakenly check your emails: now your TV channel employer decided to produce a movie. They have some ideas about the plot, but they are really concerned that it could be a disaster (i.e., that it would have a bad score in IMDb). They come to you and ask what they should avoid doing to prevent that.

You remember that back in the days at EPFL, you worked with a dataset (MovieLens) that provided features of movies in the form of genome tag scores. You then decide to find out which are the most relevant tags of poorly rated movies in IMDb, so that you could advise them to avoid that.

Based on MovieLens (HDFS at `/cs449/movielens/ml-latest`) and IMDb (HDFS at `/cs449/imdb/`) datasets, use Spark SQL to compute the following:

* The 50 most frequent genome tags out of the 200 most relevant tags of each of the 2000 movies with the worst scores at IMDb that had at least 150 ratings.
* In order to do that, take the 2000 movies after merging the two datasets (MovieLens and IMDb), so that you discard the movies which are present in one of them and not in the other.
* You might notice that only a fraction of those movies will have genome scores. This is expected.

## Instructions


* For all assignments, you have a code template [here](https://gitlab.epfl.ch/sacs/cs-449-sds-public/exercises/practical-assignment).
* On top of each source file, replace `YOUR_FULL_NAME_HERE` by your full name.
* The source files include the expression `???` (it throws a NotImplementedError). You should replace them with your code.
* You are free to add as many functions as you wish, but you cannot change class names or signature of the functions that already are in the template (their names, parameters and return types). Moreover, do not edit the file build.sbt. We will use automated tests to grade the assignments.
* Remember to treat degenerate cases (empty sets, division by zero, identifiers not found). Do not throw exceptions. The three exercises combined (one running after the other) should complete execution in less than 12 minutes when running with 5 executors in the cluster.
* Do not hesitate to comment your code. In case your results diverge from what is expected, we may consider your comments in the evaluation.
* Even if you write your code elsewhere (e.g., your laptop), you must be sure that it runs with spark-submit on our cluster gateway (iccluster041.iccluster.epfl.ch). Your work will be evaluated over there.
* You are free to discuss with your classmates about how to solve the assignments, but each student will deliver their own solution: beware of plagiarism. We are going to compare your source files to detect similarities among different solutions. In case of reasonable doubt, all people involved will be invited to make oral presentations of their solutions and answer to the examiner’s questions.
