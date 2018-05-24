package net.sansa_stack.template.spark.AnomalyDetection

import org.apache.jena.graph.Node
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import scala.collection.mutable
import scala.collection.mutable.HashSet
import org.apache.jena.graph.NodeFactory
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd._
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.functions.col
import org.apache.commons.math3.stat.descriptive._
import org.apache.spark.storage.StorageLevel
/*
 *
 * AnomalyDetection - Anomaly detection of numerical data
 * @objList - list of numerical type.
 * @triplesType - List of rdf type's objects.
 * @return - cluster of similar subjects.
 */

class AnomalydetectWithCollect(nTriplesRDD: RDD[Triple], objList: List[String],
                                   triplesType: List[String], JSimThreshold: Double,
                                   listSuperType: List[String], sparkSession: SparkSession, hypernym: String, numPartition: Int, anomalyListLimit: Int) extends Serializable {
  def run(): RDD[Set[(String, String, Object)]] = {

    // get all the triples whose objects are literal 
    //these literals also contains xsd:date as well as xsd:langstring 
    val getObjectLiteral = getObjectList()

    //remove the literal which has ^^xsd:date or xsd:langstring(only considering numerical)
    val removedLangString = getObjectLiteral.filter(f => searchedge(f.getObject.toString(), objList))

    val removewiki = removedLangString.filter(f => (!f.getPredicate.toString().contains("wikiPageID")) &&
      (!f.getPredicate.toString().contains("wikiPageRevisionID")))

    //checking still object has only numerical data only
    val triplesWithNumericLiteral = triplesWithNumericLit(removewiki)

    val mapSubWithTriples = propClustering(triplesWithNumericLiteral)
    
    val mappropwithtriples = propClustering1(triplesWithNumericLiteral)
    //get triples of hypernym
    val getHypernymTriples = getHyp()
    //filter rdf type having object value dbpedia and join with hyernym
    val rdfTypeDBwiki = rdfType(getHypernymTriples)
    //joining those subjects only who has rdf:ytpe and numerical literal 
    val rdfTypeWithSubject = mapSubWithTriples.join(rdfTypeDBwiki)
    val mapSubjectwithType = rdfTypeWithSubject.map(f => (f._1, f._2._2))
    val propwithSub = propwithsubject(triplesWithNumericLiteral)
    //cluster subjects on the basis of rdf type
    val jacardSimilarity = jSimilarity(triplesWithNumericLiteral, mappropwithtriples, propwithSub, mapSubjectwithType, mapSubWithTriples)

    jacardSimilarity

  }

  def getHyp(): RDD[Triple] = nTriplesRDD.filter(f => f.getPredicate.toString().equals(hypernym))

  def getObjectList(): RDD[Triple] = nTriplesRDD.filter(f => f.getObject.isLiteral())

  def triplesWithNumericLit(objLit: RDD[Triple]): RDD[Triple] = objLit.filter(f => isNumeric(f.getObject.toString()))

  def propwithsubject(a: RDD[Triple]): RDD[(String, String)] = a.map(f => (getLocalName1(f.getSubject), getLocalName1(f.getPredicate)))
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

  def searchedge(x: String, y: List[String]): Boolean = {
    if (x.contains("^")) {
      val c = x.indexOf('^')
      val subject = x.substring(c + 2)
      y.contains(subject)
    } else
      false
  }

  def rdfType(getHypernym: RDD[Triple]): RDD[(String, HashSet[String])] = {

    //filter triples with predicate as rdf:type
    val triplesWithRDFType = nTriplesRDD.filter(_.getPredicate.toString() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

    val triplesWithDBpedia = triplesWithRDFType.filter(f => searchType(f.getObject.toString(), triplesType))

    val subWithType1 = triplesWithDBpedia.map(f =>
      // ...
      (getLocalName1(f.getSubject), (getLocalName1(f.getObject)))) //.reduceByKey(_ ++ _) //.partitionBy(new HashPartitioner(8)).persist()

    val initialSet1 = mutable.HashSet.empty[String]
    val addToSet1 = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets1 = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
    val uniqueByKey1 = subWithType1.aggregateByKey(initialSet1)(addToSet1, mergePartitionSets1)

    val hyper1 = getHypernym.map(f =>
      (getLocalName1(f.getSubject), (getLocalName1(f.getObject) + ("hypernym")))) //.partitionBy(new HashPartitioner(8)).persist

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
    val uniqueByKey = hyper1.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    val joinOp = uniqueByKey union uniqueByKey1
    val initialSet2 = mutable.HashSet.empty[HashSet[String]]
    val addToSet2 = (s: mutable.HashSet[HashSet[String]], v: HashSet[String]) => s += v
    val mergePartitionSets2 = (p1: mutable.HashSet[HashSet[String]], p2: mutable.HashSet[HashSet[String]]) => p1 ++= p2
    val uniqueByKey2 = joinOp.aggregateByKey(initialSet2)(addToSet2, mergePartitionSets2)
    val Joinopgroup2 = uniqueByKey2.map(f => (f._1, f._2.flatMap(f => f)))

    Joinopgroup2

  }

  def getLocalName1(x: Node): String = {
    var a = x.toString().lastIndexOf("/")
    val b = x.toString().substring(a + 1)
    b
  }
  def searchType(x: String, y: List[String]): Boolean = {
    if (y.exists(x.contains)) {
      true
    } else
      false
  }
  def jSimilarity(TriplesWithNumericLiteral: RDD[Triple], mappropwithtriples: RDD[(String, mutable.Set[(String, String, Object)])], xse: RDD[(String, String)],
                  rdfTypeDBwiki: RDD[(String, HashSet[String])], mapSubWithTriples: RDD[(String, mutable.Set[(String, String, Object)])]): RDD[Set[(String, String, Object)]] = {

    nTriplesRDD.unpersist()
    import sparkSession.implicits._
     val hashtoseq = rdfTypeDBwiki.map(f => (f._1, f._2.toSeq))
    // val hashtoseq = o.map(f => ( f._2._1,f._1,f._2._2.toSeq))
    // val dfA = hashtoseq.toDF("id", "sub","values")
    val part = new RangePartitioner(30, hashtoseq)
    val partitioned = hashtoseq.partitionBy(part).persist(StorageLevel.MEMORY_AND_DISK)
    val dfA = partitioned.toDF("id", "values")
    val dropDup = dfA.dropDuplicates()

    val hashingTF = new HashingTF()
      .setInputCol("values").setOutputCol("features").setNumFeatures(1048576)

    val featurizedData = hashingTF.transform(dropDup) //.repartition(numPartition).persist()
    val mh = new MinHashLSH()
      .setNumHashTables(3) //tested with 100 on out4.nt file ..result in /home/rajjat/Desktop/recent_dataset/output_removed_boolean_udf.txt
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = mh.fit(featurizedData)

    val dffilter = model.approxSimilarityJoin(featurizedData, featurizedData, 0.45)
    val opiu = dffilter.filter($"datasetA.id".isNotNull).filter($"datasetB.id".isNotNull)
      .filter(($"datasetA.id" =!= $"datasetB.id"))
      // .filter(($"distCol" <= 0.45)).
      .select(col("datasetA.id").alias("id1"),
        col("datasetB.id").alias("id2")) //heap space error due to persist
   val x1 = opiu.repartition(400).persist(StorageLevel.MEMORY_AND_DISK) 
    val x1Map = x1.rdd.map(row => {
       val id = row.getString(0)
        val value = row.getString(1)
        (id, value)
      })

    val initialSet3 = mutable.Set.empty[String]
    val addToSet3 = (s: mutable.Set[String], v: String) => s += v
    val mergePartitionSets3 = (p1: mutable.Set[String], p2: mutable.Set[String]) => p1 ++= p2
    val uniqueByKey3 = x1Map.aggregateByKey(initialSet3)(addToSet3, mergePartitionSets3)

    val k = uniqueByKey3.map(f => ((f._2 += (f._1)).toSet)).map(a => (a, a))
      .aggregateByKey(Set[String]())((x, y) => y, (x, y) => x)
      .keys.distinct()

    val superset1 = k.filter(f => f.size > 200)

    val xs1 = xse.map(f => (f._2, Set(f._1))).reduceByKey(_ ++ _)

    val collectbroad = sparkSession.sparkContext.broadcast(superset1.collect()) //never use collect
    val intersectrdd = xs1.flatMap {
      case (key, value) =>
        collectbroad.value.map { otherValue =>
          val a = otherValue.intersect(value)

          (key, a)

        }

    }

    val partitioner = new HashPartitioner(500)

    val hu = intersectrdd.filter(f => f._2.size > 1).partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK) //for removing empty result
    val mappropwithtriples1 = mappropwithtriples.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK)
    val joinSimSubTriples2 = hu.join(mappropwithtriples1)
    val joinMaptoSet = joinSimSubTriples2.map(f => (f._2._1, f._2._2.toSet))

    val hjops = joinMaptoSet.map {
      case (iter, iter1) => iter1.filter(f => searchType(f._1, iter.toList))
    }

    hjops

  }
  def isContains(a: List[Node], b: List[Node]): Boolean = {
    if (a.forall(b.contains) || b.forall(a.contains)) {
      true
    } else
      false
  }

  def propClustering(triplesWithNumericLiteral: RDD[Triple]): RDD[(String, mutable.Set[(String, String, Object)])] = {

    val subMap = triplesWithNumericLiteral.map(f => (getLocalName1(f.getSubject),
      (getLocalName1(f.getSubject), getLocalName1(f.getPredicate), getNumber(f.getObject.toString())))) //.partitionBy(new HashPartitioner(8)) //make a function instead of using

    val initialSet = mutable.Set.empty[(String, String, Object)]
    val addToSet = (s: mutable.Set[(String, String, Object)], v: (String, String, Object)) => s += v
    val mergePartitionSets = (p1: mutable.Set[(String, String, Object)], p2: mutable.Set[(String, String, Object)]) => p1 ++= p2
    val uniqueByKey = subMap.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    uniqueByKey
  }
  def propClustering1(triplesWithNumericLiteral: RDD[Triple]): RDD[(String, mutable.Set[(String, String, Object)])] = {

    val subMap = triplesWithNumericLiteral.map(f => (getLocalName1(f.getPredicate),
      (getLocalName1(f.getSubject), getLocalName1(f.getPredicate), getNumber(f.getObject.toString())))) //.partitionBy(new HashPartitioner(8)) //make a function instead of using

    val initialSet = mutable.Set.empty[(String, String, Object)]
    val addToSet = (s: mutable.Set[(String, String, Object)], v: (String, String, Object)) => s += v
    val mergePartitionSets = (p1: mutable.Set[(String, String, Object)], p2: mutable.Set[(String, String, Object)]) => p1 ++= p2
    val uniqueByKey = subMap.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    uniqueByKey
  }

  def getNumber(a: String): Object = {
    val c = a.indexOf('^')
    val subject = a.substring(1, c - 1)

    subject

  }


  def iqr2(cluster: Seq[(String, String, Object)], anomalyListLimit: Int): Seq[(String, String, Object)] = {

    //create sample data 

    val listofData = cluster.map(b => (b._3.toString()).toDouble).toArray

    val c = listofData.sorted

    val arrMean = new DescriptiveStatistics()
    genericArrayOps(c).foreach(v => arrMean.addValue(v))
    // Get first and third quartiles and then calc IQR
    val Q1 = arrMean.getPercentile(25)
    //println("Q1="+Q1)
    val Q3 = arrMean.getPercentile(75)
    //println("Q3="+Q3)
    val IQR = Q3 - Q1
    //println("IQR="+IQR)
    val lowerRange = Q1 - 1.5 * IQR
    //println("lowerRange="+lowerRange)
    val upperRange = Q3 + 1.5 * IQR
    //    println("upperRange="+upperRange)
    val yse = c.filter(p => (p < lowerRange || p > upperRange))

    val xde = cluster.filter(f => search(f._3.toString().toDouble, yse))

    xde
    //    // result.show()
    //    result.where(result.col("outliers").isNotNull)
  }
  def search(a: Double, b: Array[Double]): Boolean = {
    if (b.contains(a))
      true
    else
      false

  }

}
object AnomalydetectWithCollect {
  def apply(nTriplesRDD: RDD[Triple], objList: List[String], triplesType: List[String],
            JSimThreshold: Double, listSuperType: List[String], sparkSession: SparkSession, hypernym: String, numPartition: Int, anomalyListLimit: Int) = new AnomalydetectWithCollect(nTriplesRDD, objList, triplesType,
    JSimThreshold, listSuperType, sparkSession, hypernym, numPartition, anomalyListLimit)
}
