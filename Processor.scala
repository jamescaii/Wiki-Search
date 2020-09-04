package search.sol
import java.io.FileNotFoundException

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.xml.{Node, NodeSeq}
import scala.util.matching.Regex
import search.src.{FileIO, PorterStemmer, StopWords}

import scala.math.sqrt
import scala.collection.mutable

/**
 * Class for our indexer
 */
class Processor {
  // Maps the document ids to the title for each document
  private val idsToTitles = new HashMap[Int, String]
  // Maps the document title to the id for each document
  private val titlesToId = new HashMap[String, Int]
  // Maps the document ids to the euclidean normalization for each document
  private val idsToMaxCounts = new HashMap[Int, Double]
  // Maps the document ids to the euclidean normalization for each document
  private val idsToMaxFreqs = new HashMap[Int, Double]
  // Maps each word to a map of document IDs and frequencies of documents that
  // contain that word
  private val wordsToDocumentFrequencies =
    new HashMap[String, HashMap[Int, Double]]
  // Maps the id to the pagerank value
  private val idsToPageRank = new HashMap[Int, Double]
  // Maps the id to the pagerank value
  private val idsToPageRankPrime = new HashMap[Int, Double]
  // Maps the id to a list of links that the id links to
  private val idsToLinks= new HashMap[Int, ListBuffer[Int]]
  // total number of documents
  private var totalDoc: Double = 0

  /**
   * Generates our idsToTiles, titlesToId, idsToMaxFreqs,
   * wordsToDocumentFrequencies, idsToLinks hashmaps
   * @param fileName - A string file name
   * @return - A Unit
   */
  def buildNode(fileName: String) = {
    val mainNode: Node = xml.XML.loadFile(fileName)
    var pageSeq: NodeSeq = mainNode \ "page"
    for (n <- pageSeq) {
      val id: Int = (n \ "id").text.trim.toInt
      val title: String = (n \ "title").text.trim
      idsToTitles(id) = title
      titlesToId(title) = id
    }
    for (n <- pageSeq) {
      val id: Int = (n \ "id").text.trim.toInt
      val title: String = (n \ "title").text.trim
      val text: String = (n \ "text").text.trim
      val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")
      val matchesIterator = regex.findAllMatchIn(title + " " + text)
      val matchesList = matchesIterator.toList.map { aMatch => aMatch.matched }
      var tokenizedList: List[String] = Nil
      val lst = new mutable.ListBuffer[Int]
      idsToLinks(id) = lst
      for (m <- matchesList){
        if(isLink(m)) {
          tokenizedList = tokenizedList ::: getRidOfLinks(m.toLowerCase)
          if (!getLink(m).equals(idsToTitles(id))) {
            if (titlesToId.contains(getLink(m))) {
              if (idsToLinks.contains(id)) {
                if (!idsToLinks(id).contains(titlesToId(getLink(m)))) {
                  idsToLinks(id) += titlesToId(getLink(m))
                }
              }
              else {
                idsToLinks += (id -> ListBuffer(titlesToId(getLink(m))))
              }
            }
          }
        }
        else {
          if (!StopWords.isStopWord(m)) {
            tokenizedList = tokenizedList :::
              List(PorterStemmer.stem(m.toLowerCase))
          }
        }
      }
      totalDoc = totalDoc + 1
      var max: Double = 0.0
      for (m <- tokenizedList) {
        if (wordsToDocumentFrequencies.contains(m)) {
          if (wordsToDocumentFrequencies(m).contains(id)){
            wordsToDocumentFrequencies(m)(id) =
              wordsToDocumentFrequencies(m)(id) + 1
          } else {
            wordsToDocumentFrequencies(m) += (id -> 1)
          }
        } else {
          val newHash = new mutable.HashMap[Int, Double]()
          newHash(id) = 1
          wordsToDocumentFrequencies += (m -> newHash)
        }
        if(max < wordsToDocumentFrequencies(m)(id)){
          max = wordsToDocumentFrequencies(m)(id)
        }
      }
      idsToMaxCounts(id) = max
      idsToMaxFreqs(id) = max
    }
    pageSeq = null
  }

  /**
   * Returns the link title of a link
   * @param word - A string link
   * @return the link title of a link
   */
  def getLink(word: String): String = {
    if (word.contains("|")){
      val textList: List[String] = word.split("""\[\[|\||\]\]""").toList
      textList.tail.head
    }
    else if (word.contains(":")){
      val textList: List[String] = word.split("""\[\[|\]\]""").toList
      textList.tail.head
    }
    else {
      val textList: List[String] = word.split("""\[\[|\]\]""").toList
      textList.tail.head
    }
  }

  /**
   * Returns the text we want to include from a link
   * @param word - A string link
   * @return - Returns the text we want to include from a link
   */
  def getRidOfLinks(word: String): List[String] = {
    if (word.contains("|")){
      var lst: List[String] = Nil
      val linksList: List[String] = word.split("""\[\[|\||\]\]""").toList
      for(l <- linksList) {
        if (!StopWords.isStopWord(l) && !l.equals("")){
          lst = lst ::: List(PorterStemmer.stem(l))
        }
      }
      if (lst.isEmpty) {
        Nil
      }
      else if (lst.tail.isEmpty){
        Nil
      }
      else {
        val text: String = lst.tail.head
        text.split(" ").toList
      }
    }
    else if (word.contains(":")){
      var lst: List[String] = Nil
      val linksList: List[String] = word.split("""\[\[|\]\]""").toList
      for(l <- linksList) {
        if (!StopWords.isStopWord(l) && !l.equals("")){
          lst = lst ::: List(PorterStemmer.stem(l))
        }
      }
      if (lst.isEmpty){
        Nil
      }
      else if (lst.tail.isEmpty){
        Nil
      }
      else{
        lst.head.split("""\:|\s""").toList
      }
    }
    else {
      var lst: List[String] = Nil
      val linksList: List[String] = word.split("""\[\[|\]\]""").toList
      for(l <- linksList) {
        if (!StopWords.isStopWord(l) && !l.equals("")){
          lst =  lst ::: List(PorterStemmer.stem(l))
        }
      }
      if (lst.isEmpty){
        Nil
      }
      else if (lst.tail.isEmpty){
        Nil
      }
      else {
        lst.head.split(" ").toList
      }
    }
  }

  /**
   * Determines if a string is a link
   * @param link
   * @return - A boolean if a string is a link
   */
  def isLink(link: String): Boolean = {
    link.startsWith("[[")
  }

  /**
   * Calculates the rank of a page
   * @return - A unit
   */
  def calculateRank(): HashMap[Int, Double] = {
    for((k,_) <- idsToTitles){
      idsToPageRank(k) = 0
      idsToPageRankPrime(k) = 1/totalDoc
    }
    while(euclidDistance(idsToPageRank, idsToPageRankPrime) > 0.001){
      for ((k, v) <- idsToPageRankPrime) {
        idsToPageRank(k) = v
      }
      for ((k1,_) <- idsToPageRankPrime){
        idsToPageRankPrime(k1) = 0
        for ((k2, _) <- idsToPageRank){
          val i = calculateWeight(k1, k2)
          idsToPageRankPrime(k1) = idsToPageRankPrime(k1) +
            idsToPageRank(k2) * i
        }
      }
    }
    idsToPageRank
  }

  /**
   * Calulates the distance between two rankings
   * @param r1 - A hashmap of ints to doubles
   * @param r2 - A hashmap of ints to doubls
   * @return - the distance between two pages
   */
  def euclidDistance(r1: HashMap[Int, Double], r2: HashMap[Int, Double]):
  Double = {
    var totalSum: Double = 0;
    for ((k,v) <- r1) {
      totalSum = totalSum + (r2(k) - v)*(r2(k) - v)
    }
    sqrt(totalSum)
  }

  /**
   * The weight between two pages
   * @param k1Key - The ID of K1
   * @param k2Key - The ID of K2
   * @return - weight between two pages
   */
  def calculateWeight(k1Key: Int, k2Key: Int): Double = {
    val eps = 0.15
    var toReturn = eps/totalDoc
    if (k1Key == k2Key){
      toReturn = eps/totalDoc
    }
    else if (idsToLinks(k2Key).isEmpty){
      toReturn = (eps/totalDoc) + (1-eps)*(1/(totalDoc-1))
    }
    else {
      if (idsToLinks(k2Key).contains(k1Key)){
        val n = idsToLinks(k2Key).size.toDouble
        toReturn = (eps/totalDoc) + (1-eps)*(1/(n))
      }
      else {
        toReturn = eps/totalDoc
      }
    }
    toReturn
  }
}

/**
 * Main method
 */
object Processor{
  def main(args: Array[String]): Unit = {
    if (args.length != 4){
      println("Incorrect number of arguments")
    }
    else {
      val processor: Processor = new Processor()
      processor.buildNode(args(0))
      processor.calculateRank()
      FileIO.printTitleFile(args(1), processor.idsToTitles)
      FileIO.printDocumentFile(args(2),
        processor.idsToMaxCounts, processor.idsToPageRank)
      FileIO.printWordsFile(args(3), processor.wordsToDocumentFrequencies)
    }
  }
}
