import java.util.concurrent.TimeUnit

import dto.{Rule, RuleDefinition}
import info.debatty.java.stringsimilarity.JaroWinkler
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.{immutable, mutable}

object EntityResolutionObjects {

  val jw = new JaroWinkler
  val jwDistanceUdf: UserDefinedFunction = udf((x: String, y: String) => jw.similarity(x, y))


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("entity-resolution")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val idCol: String = "identity_id"

    // apply the ruleset for identity resolution
    // parse the rule set and extract datasets and dataset ids
    val rule_set: List[Rule] = ObjectGeneration.getRuleSet(1)
    val dsMap: mutable.Map[VertexId, (DataFrame, String)] = collection.mutable.Map[Long, (DataFrame, String)]()
    for (elem <- rule_set) {
      if (!dsMap.keySet.contains(elem.datasetId1)) {
        dsMap.put(elem.datasetId1, (LocalMetaStoreManager.getDataset(elem.datasetId1, elem.pk1, idCol), elem.pk1))
      }
    }

    identity_resolution(rule_set, spark, idCol, dsMap, dsMap.values.map(x => x._1).to[collection.immutable.Seq],
      dsMap.values.map(x => x._2).to[collection.immutable.Seq])

    // TODO: Add MsSql support
    // TODO: Support for algorithms
    // TODO: Support for the mandatory columns
    // TODO: match error tolerance user friendly names change
  }


  def identity_resolution(rule_set: List[Rule], spark: SparkSession, idCol: String, dsMap: mutable.Map[VertexId,
    (DataFrame, String)], datasets: immutable.Seq[DataFrame], pks: immutable.Seq[String]): DataFrame = {

    import spark.implicits._
    val start = System.currentTimeMillis()
    println("Records before identity resolution: " + datasets.map(x => x.count()).sum)

    // union output of all the maps
    val map_all = rule_set.map(rule => resolve(rule, idCol, dsMap, spark)).reduce(_ union _)
    //      .repartition(col("_2"))

    // create a graph for the mapping tables and run union-find on it
    val ed = map_all.map(r => Edge(r.getAs[VertexId](0), r.getAs[VertexId](1), null)).rdd
    val nd = map_all.map(r => (r.getAs[VertexId]("_2"), 1)).rdd
    val graph = Graph(nd, ed)
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    val cc = graph.connectedComponents()
    cc.vertices.toDF().show()


    // Get back the original ids from the dataset
    def joinWithRowNums(ds1: DataFrame, ds2: DataFrame): DataFrame = {
      val key: String = "" + ds2.hashCode()
      ds1.join(ds2.as(key)
        , (col("_1") === col(key + "." + idCol)) || (col("_2") === col(key + "." + idCol))
        , "fullOuter")
    }

    val tempPks: Seq[String] = Seq("_2").union((pks zip datasets).map(x => x._2.hashCode() + "." + x._1))
    val tempDatasets: Seq[DataFrame] = Seq(cc.vertices.toDF()).union(datasets)

    val out = tempDatasets.reduce((x, y) => joinWithRowNums(x, y)).select("_1", tempPks: _*).distinct()
    out.show()


    // union the results of union find on the mappers
    def pkToUUID(pk: String): DataFrame = {
      out.withColumn("uuid", col(pk))
        .withColumn("dataset", lit(pk))
        .select("_2", "uuid", "dataset")
    }


    val singleMappingTable = (pks zip datasets).map(x => x._2.hashCode() + "." + x._1)
      .map(pk => pkToUUID(pk))
      .reduce(_ union _)
      .filter(col("uuid").isNotNull)
      .distinct()
      .withColumn("temp_id", col("_2"))
      .select("temp_id", "uuid", "dataset")


    // assign a new uuid in a mdpid column whenever the temp_id is null
    // and assign the same uuid for the same temp_id

    val singleMappingTableWithMDPIDs = singleMappingTable
      .filter(col("temp_id").isNull).withColumn("mdpid", expr("uuid()"))
      .union(
        singleMappingTable.as("ds1").join(
          singleMappingTable.as("ds2")
            .select("temp_id").distinct()
            .withColumn("mdpid", expr("uuid()")),
          col(d1c("temp_id")) === col(d2c("temp_id"))
        )
          .select(d1c("temp_id"), "uuid", "dataset", "mdpid")
      )

    println("Records after identity resolution: " +
      singleMappingTableWithMDPIDs.select("mdpid").distinct().count())

    singleMappingTableWithMDPIDs.show()

    val stop = System.currentTimeMillis()
    println("Time taken: " + TimeUnit.MILLISECONDS.toSeconds(stop - start) + " seconds")

    singleMappingTableWithMDPIDs
  }


  def d1c(columnName: String): String = "ds1." + columnName


  def d2c(columnName: String): String = "ds2." + columnName


  def resolve(rule: Rule, idCol: String, dsMap: mutable.Map[VertexId, (DataFrame, String)],
              spark: SparkSession): Dataset[Row] = {

    import spark.implicits._

    val ds1: DataFrame = dsMap.get(rule.datasetId1).map(x => x._1).orNull
    val ds2: DataFrame = dsMap.get(rule.datasetId2).map(x => x._1).orNull
    val pk1: String = dsMap.get(rule.datasetId1).map(x => x._2).orNull
    val pk2: String = dsMap.get(rule.datasetId2).map(x => x._2).orNull

    if (ds1 == null || ds2 == null) throw new IllegalArgumentException()

    println(s"Datasets")
    ds1.show()
    ds2.show()

    val joinResults = ds1.as("ds1").join(ds2.as("ds2"), conditions(rule.ruleDefinitions, rule.threshold, pk1, pk2),
      "left")
    joinResults.show()

    val edges: Dataset[Row] = joinResults.filter(col(d2c(idCol)).isNotNull).withColumn("edgeCondition",
      lit(rule.toString))

    println("Match rule analysis")
    edges.select("edgeCondition")
      .groupBy("edgeCondition")
      .count().as("count")
      .withColumn("match ratio", col("count") / ds1.count())
      .show()


    val edgesRDD = edges.select(d1c(idCol), d2c(idCol))
      .map(r => Edge(r.getAs[VertexId](0), r.getAs[VertexId](1), null))
      .rdd

    println("Graph connections formed")
    edges.show()

    // generate nodes
    val nodesRDD = ds1.as("ds1")
      .map(r => (r.getAs[VertexId](idCol), 1))
      .rdd

    // create graph
    val graph = Graph(nodesRDD, edgesRDD)
    // TODO: handle dense connections here -=- using
    // run connected components
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    val cc = graph.connectedComponents(3)
    cc.vertices.toDF()
  }


  def conditions(ruleDefinitions: List[RuleDefinition], threshold: Double, pk1: String, pk2: String): Column = {
    ruleDefinitions
      .map(c => matchCriteria(c.columnids1, c.columnids2, c.tolerance, pk1, pk2) * c.weight)
      .reduce(_ + _)
      .geq(threshold)
  }


  def matchCriteria(ds1Col: String, ds2Col: String, maxDiff: Long, pk1: String, pk2: String): Column = {
    if (maxDiff == 0) {
      (concat_ws(" ", ds1Col.split(",").map(c => col(d1c(c))): _*)
        === concat_ws(" ", ds2Col.split(",").map(c => col(d2c(c))): _*)).cast(IntegerType)
    } else {
      jwDistanceUdf(concat_ws(" ", ds1Col.split(",").map(c => col(d1c(c))): _*),
        concat_ws(" ", ds2Col.split(",").map(c => col(d2c(c))): _*))
    }
  }

}
