package search.sol

import java.io._

import search.src.{FileIO, PorterStemmer, StopWords}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}

/**
  * Represents a query REPL built off of a specified index
  *
  * @param titleIndex    - the filename of the title index
  * @param documentIndex - the filename of the document index
  * @param wordIndex     - the filename of the word index
  * @param usePageRank   - true if page rank is to be incorporated into scoring
  */
class Query(titleIndex: String, documentIndex: String, wordIndex: String,
            usePageRank: Boolean) {

  // Maps the document ids to the title for each document
  private val idsToTitle = new HashMap[Int, String]

  // Maps the document ids to the euclidean normalization for each document
  private val idsToMaxFreqs = new HashMap[Int, Double]

  // Maps the document ids to the page rank for each document
  private val idsToPageRank = new HashMap[Int, Double]

  // Maps each word to its inverse document frequency
  private val wordToInvFreq = new HashMap[String, Double]

  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word
  private val wordsToDocumentFrequencies = new HashMap[String, HashMap[Int, Double]]

  /**
   * An array of the id's of the top ten ids
   */
  val topTenID = new ListBuffer[Int]

  /**
   * Finds the max value in a hashmap
   * @param map - A hashmap of ints to doubles
   * @return - the max value in a hashmap
   */
  private def findMax(map: HashMap[Int, Double]): Double = {
    var max: Double = 0
    for ((_,v) <- map){
      if (v > max){
        max = v
      }
    }
    max
  }

  /**
   * Method to instantiate wordstToInvFreq
   * @return - A unit
   */
  private def fillwordToInvFreq()= {
    for ((k,v) <- wordsToDocumentFrequencies){
        wordToInvFreq(k) = v.size
    }
  }

  /**
   * Method to return the maxID of a hashmap
   * @param idToTS - A hashmap of ints to doubles
   * @return - the maxID of a hashmap
   */
  private def findMaxValue(idToTS: HashMap[Int, Double]): Int= {
    var max: Double = 0
    var maxID: Int = 0
    for ((k,v) <- idToTS){
      if (v > max){
        max = v
        maxID = k
      }
    }
    maxID
  }

  /**
    * Handles a single query and prints out results
    *
    * @param userQuery - the query text
    */
  private def query(userQuery: String) {
    val idToTotalScore = new HashMap[Int, Double]
    val totalWordFrequency =  new HashMap[String, HashMap[Int, Double]]
    var queryList: List[String] = Nil
    val linkslist: List[String] = userQuery.split(" ").toList
    for (l <- linkslist){
      if (!StopWords.isStopWord(l)){
        queryList = queryList ::: List(PorterStemmer.stem(l.toLowerCase))
      }
    }
    fillwordToInvFreq()
    for (q  <- queryList) {
        if (wordsToDocumentFrequencies.contains(q) && !q.equals("")) {
          val newHash = new mutable.HashMap[Int, Double]()
          for ((a, b) <- wordsToDocumentFrequencies(q)) {
            newHash(a) = (b / findMax(wordsToDocumentFrequencies(q))) * Math.log(idsToTitle.size / wordToInvFreq(q))
          }
          totalWordFrequency.put(q, newHash)
        }
    }
    for((_,v) <- totalWordFrequency){
      for ((a,b) <- v){
        if (idToTotalScore.contains(a)){
          idToTotalScore(a) = idToTotalScore(a) + b
        }
        else {
          idToTotalScore(a) = b
        }
      }
    }
    if (usePageRank) {
      for ((k,v) <- idToTotalScore){
        idToTotalScore(k) = v * (idsToPageRank(k) + 0.001)
      }
    }
    val tempIdToTotalScore: HashMap[Int, Double] = idToTotalScore
    for (a <- topTenID){
      topTenID -= a
    }
    for (_ <- 0 to 9){
      if (!(findMaxValue(tempIdToTotalScore) == 0)){
        val temp: Int = findMaxValue(tempIdToTotalScore)
        topTenID += temp
        tempIdToTotalScore.remove(temp)
      }
    }
    println("Search Results:")
    if (topTenID.isEmpty){
      println("There were no search results.")
    }
  }

  /**
    * Format and print up to 10 results from the results list
    *
    * @param results - an array of all results
    */
  private def printResults(results: ListBuffer[Int]) {
    for (i <- 0 until Math.min(10, results.size)) {
      println("\t" + (i + 1) + " " + idsToTitle(results(i)))
    }
  }

  /**
   * This reads the files the the processor creates
   */
  def readFiles(): Unit = {
    FileIO.readTitles(titleIndex, idsToTitle)
    FileIO.readDocuments(documentIndex, idsToMaxFreqs, idsToPageRank)
    FileIO.readWords(wordIndex, wordsToDocumentFrequencies)
  }

  /**
    * Starts the read and print loop for queries
    */
  def run() {
    val inputReader = new BufferedReader(new InputStreamReader(System.in))

    // Print the first query prompt and read the first line of input
    print("search> ")
    var userQuery = inputReader.readLine()

    // Loop until there are no more input lines (EOF is reached)
    while (userQuery != null) {
      // If ":quit" is reached, exit the loop
      if (userQuery == ":quit") {
        inputReader.close()
        return
      }

      // Handle the query for the single line of input
      query(userQuery)
      printResults(topTenID)
      // Print next query prompt and read next line of input
      print("search> ")
      userQuery = inputReader.readLine()
    }

    inputReader.close()
  }
}

/**
 * This calls our query method
 */
object Query {
  def main(args: Array[String]) {
    try {
      // Run queries with page rank
      var pageRank = false
      var titleIndex = 0
      var docIndex = 1
      var wordIndex = 2
      if (args.size == 4 && args(0) == "--pagerank") {
        pageRank = true;
        titleIndex = 1
        docIndex = 2
        wordIndex = 3
      } else if (args.size != 3) {
        println("Incorrect arguments. Please use [--pagerank] <titleIndex> "
          + "<documentIndex> <wordIndex>")
        System.exit(1)
      }
      val query: Query = new Query(args(titleIndex), args(docIndex), args(wordIndex), pageRank)
      query.readFiles()
      query.run()
    } catch {
      case _: FileNotFoundException =>
        println("One (or more) of the files were not found")
      case _: IOException => println("Error: IO Exception")
    }
  }
}
