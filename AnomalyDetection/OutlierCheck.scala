package net.sansa_stack.template.spark.AnomalyDetection

import _root_.net.sansa_stack.rdf.spark.io.NTripleReader
import java.net.{ URI => JavaURI }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import scopt.OptionParser
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.log4j.Logger
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.io.IOException
import java.util.concurrent.TimeUnit
import org.apache.jena.riot.Lang
import org.apache.spark.storage.StorageLevel

object OutlierCheck {
  def main(args: Array[String]): Unit = {

    println("==================================================")
    println("|        Distributed Anomaly Detection           |")
    println("==================================================")

    val input = "src/main/resources/final.txt"
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "400")
      .config("spark.sql.autoBroadcastJoinThreshold", "304857600")
      .config("spark.executor.overhead.memory", "2048")
      .config("spark.driver.overhead.memory", "2048")
      .appName("Anomaly Detection")
      .getOrCreate()
      
       val objList = List("http://www.w3.org/2001/XMLSchema#double",
      "http://www.w3.org/2001/XMLSchema#integer",
      "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
      "http://dbpedia.org/datatype/squareKilometre")

    val textFile = sparkSession.sparkContext.textFile(input)
    val splitRdd = textFile.filter(s =>(!s.contains("wikiPageOutDegree"))).distinct()
       splitRdd.take(100).foreach(println) 
       splitRdd.coalesce(1).saveAsTextFile("src/main/resources/finalResulrDe")
//      val a = s.split(",")
//      val k=a.map(f=>f.replace("(", "").replace(")", ""))
//    
//      val date = (k(0),k(1),k(2))
//      date
//    }
//    splitRdd.take(100).foreach(println)
//    println("bbbbbbb")
//    val k = splitRdd.filter(f => (f._2.equals("wikiPageOutDegree")))
//    k.take(20).foreach(println)
//    println("abc")
//    val d=splitRdd.subtract(k)
//    d.take(200).foreach(println)
//    val kBroad = sparkSession.sparkContext.broadcast(k.collect())
//    val triplesRDD = NTripleReader.load(sparkSession,
//      JavaURI.create("/home/rajjat/Desktop/recent_dataset/out4.nt"))
//
//    //          /home/rajjat/Desktop/recent_dataset/medium.nt 
//    //         /home/rajjat/Desktop/recent_dataset/dbpedia_de/Dbpedia_de.nt
//    val getObjectLiteral = getObjectList(triplesRDD)
//   
//   
//    
//       val removedLangString = getObjectLiteral.filter(f => searchedge(f.getObject.toString(), objList))
//          val removewiki = removedLangString.filter(f => (!f.getPredicate.toString().contains("wikiPageID")) &&
//      (!f.getPredicate.toString().contains("wikiPageRevisionID"))
//      &&
//      (!f.getPredicate.toString().contains("wikiPageOutDegree")))
//
//
//     val triplesWithNumericLiteral = triplesWithNumericLit(removewiki)
//        println("triplesWithNumericLiteral")
// kBroad.value.map(f => (f._1)).take(10).foreach(println)
//    println("fffffffffffff")
//    val yse = triplesWithNumericLiteral.map(f => (getLocalName1(f.getSubject.toString()), getLocalName1(f.getPredicate.toString()), getNumber(f.getObject.toString())))
//yse.filter(f=>f._1.equals("Balearic_Islands")).foreach(println)
//
//println("in dinal")
//kBroad.value.map(f=>f._1).filter(p=>p.equals("Balearic_Islands")).foreach(println)
//kBroad.value.map(f=>f._2).filter(p=>p.equals("areaTotalKm")).foreach(println)
//kBroad.value.map(f=>f._3).filter(p=>p.equals("4992")).foreach(println)
//
//yse.map(f=>f._1).filter(p=>p.equals("Balearic_Islands")).foreach(println)
//yse.map(f=>f._2).filter(p=>p.equals("areaTotalKm")).foreach(println)
//yse.map(f=>f._3).filter(p=>p.equals("4992")).foreach(println)


//    val finValue = yse.map(f => (f._1.equals(kBroad.value.map(g => g._1))))
////        &&
////     (f._2.equals(kBroad.value.map(g => g._2))) )
////     ||
////   ( !f._3.equals(kBroad.value.map(g => g._3))))
////       val clusterOfProp = yse.map({
////      case (a, b,c) => (kBroad.value.filter(p=>p._1.equals(a)))
////    })
//    println("result")
//      finValue.take(10).foreach(println)

 // finValue.saveAsTextFile("src/main/resources/finalResulrDe")

  }
  def getLocalName1(x: String): String = {
    var a = x.lastIndexOf("/")
    val b = x.substring(a + 1)
    b
  }
   def getNumber(a: String): Object = {
    val c = a.indexOf('^')
    val subject = a.substring(1, c - 1)

    subject

  }
   
   def searchedge(x: String, y: List[String]): Boolean = {
    if (x.contains("^")) {
      val c = x.indexOf('^')
      val subject = x.substring(c + 2)
      y.contains(subject)
    } else
      false
  }
  
   def getObjectList(triplesRDD:RDD[Triple]): RDD[Triple] = triplesRDD.filter(f => f.getObject.isLiteral())

  def triplesWithNumericLit(objLit: RDD[Triple]): RDD[Triple] = objLit.filter(f => isNumeric(f.getObject.toString()))
    def isNumeric(x: String): Boolean =
    {
      if (x.contains("^")) {
        val c = x.indexOf('^')
        val subject = x.substring(1, c - 1)

        if (isAllDigits(subject))
          true
        else
          false
      } else
        false
    }

  def isAllDigits(x: String): Boolean = {
    var found = false
    for (ch <- x) {
      if (ch.isDigit || ch == '.')
        found = true
      else if (ch.isLetter) {

        found = false
      }
    }

    found
  }

}