import info.debatty.java.stringsimilarity.JaroWinkler
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.immutable

object EntityResolution {

  val rootLogger: Logger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  val jw = new JaroWinkler

  val jwDistanceUdf: UserDefinedFunction = udf((x: String, y: String) => jw.similarity(x, y))

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("entity-resolution")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // key -> (ds1, ds2, ds1Col, ds2Col, error, weight)

    val idCol: String = "identity_id"
    val path1 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/adobe_analytics.csv"
    val path2 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/hubspot.csv"
    val path3 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/apache-commons.csv"
    val ds1 = getCSVData(path1, idCol, spark)
    var ds2 = getCSVData(path2, idCol, spark)
    ds2 = combineColumns(ds2, "first_name,last_name")
    var ds3 = getCSVData(path3, idCol, spark)
    ds3 = combineColumns(ds3, "first_name,last_name")

    val pk1 = "ecid"
    val pk2 = "hubspotutk"
    val pk3 = "apid"

    // ruleset for adobe-adobe
    val rule1 = List[(String, String, Int, Double)](("ecid", "ecid", 0, 1.0))
    val threshold1 = 1
    val rule2 = List[(String, String, Int, Double)](("zip", "zip", 0, 0.6), ("city", "city", 1, 0.2), ("ip", "ip", 0, 0.2))
    val threshold2 = 0.8
    val rules1 = Tuple5[DataFrame, DataFrame, String, String, List[(List[(String, String, Int, Double)], Double)]](
      ds1, ds1, pk1, pk1, List((rule1, threshold1), (rule2, threshold2)))

    // rule for hubspot-hubspot
    val rule3 = List[(String, String, Int, Double)](("vid", "vid", 0, 1.0))
    val threshold3 = 1
    val rules2 = Tuple5[DataFrame, DataFrame, String, String, List[(List[(String, String, Int, Double)], Double)]](
      ds2, ds2, pk2, pk2, List((rule3, threshold3)))

    // ruleset for hubspot-adobe
    val rule4 = List[(String, String, Int, Double)](("Name", "first_name,last_name", 1, 0.5), ("emailhash_from_url", "emailhash", 0, 0.5))
    val threshold4 = 0.9
    val rules3 = Tuple5[DataFrame, DataFrame, String, String, List[(List[(String, String, Int, Double)], Double)]](
      ds1, ds2, pk1, pk2, List((rule4, threshold4)))

    // ruleset for adobe-apache
    val rule5 = List[(String, String, Int, Double)](("Name", "first_name,last_name", 1, 0.5), ("zip", "zip_code", 0, 0.5))
    val threshold5 = 0.9
    val rules4 = Tuple5[DataFrame, DataFrame, String, String, List[(List[(String, String, Int, Double)], Double)]](
      ds1, ds3, pk1, pk3, List((rule5, threshold5)))

    // union output of all the maps
    val rule_set: Seq[(DataFrame, DataFrame, String, String, List[(List[(String, String, Int, Double)], Double)])] = List(rules1, rules2, rules3, rules4)

    identity_resolution(rule_set, spark, idCol, List[DataFrame](ds1, ds2, ds3), List[String](pk1, pk2, pk3))

    // TODO: Test with new datasets
    // TODO: Add MySql support
    // TODO: Support for algorithms
    // TODO: Support for the mandatory columns
    // TODO: match error tolerance user friendly names change

    /**
     * So, I have finished the generalization of identity framework
     * I can start with testing new datasets
     * I also spent sometime optimizing the code, as a milestone, there are now no for loops in the entire code.
     **/
  }


  def identity_resolution(rule_set: Seq[(DataFrame, DataFrame, String, String, List[(List[(String, String, Int, Double)], Double)])],
                          spark: SparkSession, idCol: String, datasets: immutable.Seq[DataFrame], pks: immutable.Seq[String]): DataFrame = {
    import spark.implicits._
    val map_all = rule_set.map(rule => resolve(rule, idCol, spark)).reduce(_ union _)

    // create graph for the mapping tables and run union find on it
    val ed = map_all.map(r => Edge(r.getAs[VertexId](0), r.getAs[VertexId](1), null)).rdd
    val nd = map_all.map(r => (r.getAs[VertexId]("_2"), 1)).rdd
    val graph = Graph(nd, ed)
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    val cc = graph.connectedComponents()
    cc.vertices.toDF().show()


    // Get back the original ids from the dataset
    def joinWithRownums(ds1: DataFrame, ds2: DataFrame): DataFrame = {
      val key: String = "" + ds2.hashCode()
      ds1.join(ds2.as(key), (col("_1") === col(key + "." + idCol)) || (col("_2") === col(key + "." + idCol)), "fullOuter")
    }

    val temppks: Seq[String] = Seq("_2").union(pks)
    val tempDatasets: Seq[DataFrame] = Seq(cc.vertices.toDF()).union(datasets)

    val out = tempDatasets.reduce((x, y) => joinWithRownums(x, y)).select("_1", temppks: _*).distinct()
    out.show()


    // union the results of union find on the mappers
    def pkToUUID(pk: String): DataFrame = {
      out.withColumn("uuid", col(pk))
        .withColumn("dataset", lit(pk))
        .select("_2", "uuid", "dataset")
    }


    val singleMappingTable = pks.map(pk => pkToUUID(pk)).reduce(_ union _)
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
            .withColumn("mdpid", expr("uuid()")), col(d1c("temp_id")) === col(d2c("temp_id")))
          .select(d1c("temp_id"), "uuid", "dataset", "mdpid"
          )
      )

    singleMappingTableWithMDPIDs.show()
    singleMappingTableWithMDPIDs
  }

  def d2c(columnName: String): String = "ds2." + columnName

  def resolve(rules_set: Tuple5[DataFrame, DataFrame, String, String, List[(List[(String, String, Int, Double)], Double)]], idCol: String, spark: SparkSession): Dataset[Row] = {
    // generate edges
    import spark.implicits._

    val ds1: DataFrame = rules_set._1
    val ds2: DataFrame = rules_set._2
    val rules = rules_set._5

    println(s"Number of records before Entity Resolution ${ds1.count() + ds2.count()}")
    ds1.show()
    ds2.show()

    val edges: Dataset[Row] = ds1.as("ds1")
      .join(ds2.as("ds2"), rules.map(x => conditions(x._1, x._2)).reduce(_ || _))

    val edgesRDD = edges.select(d1c(idCol), d2c(idCol)).map(r => Edge(r.getAs[VertexId](0), r.getAs[VertexId](1), null)).rdd

    println("Graph connections formed")
    edges.show()

    // generate nodes
    val nodesRDD = ds1.as("ds1")
      .map(r => (r.getAs[VertexId](idCol), 1))
      .rdd

    // create graph
    val graph = Graph(nodesRDD, edgesRDD)

    // run connected components
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    val cc = graph.connectedComponents()
    cc.vertices.toDF()
  }

  def conditions(matchCols: List[(String, String, Int, Double)], threshold: Double): Column = {
    matchCols
      .map(c => matchCriteria(c._1, c._2, c._3) * c._4)
      .reduce(_ + _)
      .geq(threshold)
  }

  def matchCriteria(ds1Col: String, ds2Col: String, maxDiff: Int): Column = {
    if (maxDiff == 0) {
      (col(d1c(ds1Col)) === col(d2c(ds2Col))).cast(IntegerType)
    } else {
      jwDistanceUdf(col(d1c(ds1Col)), col(d2c(ds2Col)))
    }
  }

  def d1c(columnName: String): String = "ds1." + columnName

  def getCSVData(path: String, idCol: String, spark: SparkSession): DataFrame = {
    var records = spark.read.format("csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(path).cache()

    val r = new scala.util.Random()
    val id_gen: UserDefinedFunction = udf(() => ((System.currentTimeMillis() >> 32) + "" + r.nextInt(math.pow(2, 31).toInt)).toLong)

    records = records.withColumn(idCol, id_gen()).persist()
    records
  }

  def combineColumns(dataset: DataFrame, columnListString: String): DataFrame = {
    dataset
      .withColumn(columnListString, concat_ws(" ", columnListString.split(",")
        .map(c => col(c)): _*))
  }

}
