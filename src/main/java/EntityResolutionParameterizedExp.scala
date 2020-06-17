import info.debatty.java.stringsimilarity.JaroWinkler
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object EntityResolutionParameterizedExp {

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
    val idCol: String = "identity_id"
    // key -> (ds1, ds2, ds1Col, ds2Col, error, weight)


    // ruleset for adobe-adobe
    //    val rule1 = List[Tuple4[String,String,Int,Double]](("ecid", "ecid", 0, 1.0))
    //    val threshold1 = 1
    //    val rule2 = List[Tuple4[String,String,Int,Double]](("zip", "zip", 0, 0.6), ("city", "city", 1, 0.2), ("ip", "ip", 0, 0.2))
    //    val threshold2 = 0.8
    //    val rules = List[(List[Tuple4[String,String,Int,Double]], Double)]((rule1, threshold1), (rule2, threshold2))
    //    val path1 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/adobe_analytics.csv"
    //    val path2 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/adobe_analytics.csv"
    //    var ds1 = getCSVData(path1, idCol, spark)
    //    val ds2 = getCSVData(path2, idCol, spark)
    //    val pk1 = "ecid" //very important
    //    val pk2 = "ecid"


    // ruleset for hubspot-hubspot
    val path1 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/hubspot.csv"
    val path2 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/hubspot.csv"
    var ds1 = getCSVData(path1, idCol, spark)
    val ds2 = getCSVData(path2, idCol, spark)
    val pk1 = "hubspotutk"
    val pk2 = "hubspotutk"

    val rule1 = List[(String, String, Int, Double)](("vid", "vid", 0, 1.0))
    val threshold1 = 1
    val rules = List[(List[(String, String, Int, Double)], Double)]((rule1, threshold1))

    // ruleset for hubspot-adobe
    //    val rule1 = List[(String, String, Int, Double)](("first_name,last_name", "Name", 1, 1.0))
    //    val threshold1 = 0.9
    //    val rule2 = List[(String, String, Int, Double)](("emailhash", "emailhash_from_url", 0, 1.0))
    //    val threshold2 = 1
    //    val rules = List[(List[(String, String, Int, Double)], Double)]( (rule1, threshold1), (rule2, threshold2))
    //    val path1 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/hubspot.csv"
    //    val path2 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/adobe_analytics.csv"
    //    var ds1 = getCSVData(path1, idCol, spark)
    //    ds1 = combineColumns(ds1, "first_name,last_name")
    //    val ds2 = getCSVData(path2, idCol, spark)
    //    val pk1 = "hubspotutk"
    //    val pk2 = "ecid"

    resolve(ds1, ds2, pk1, pk2, rules, idCol, spark)

    // TODO: Support for multiple datasets, more than 2 basically
    /**
     * Process to be followed:
     * For each rule create a separate mapping table
     * Combine the mapping tables at the end into one single large table, basically join everything
     */
    // TODO: Figure out, what if a value matches only a few columns instead of all the columns
    // TODO: Support for algorithms
    // TODO: Support for the mandatory columns
    // TODO: match error tolerance user friendly names change
  }

  def getCSVData(path: String, idCol: String, spark: SparkSession): DataFrame = {
    var records = spark.read.format("csv").option("header", value = true).option("delimiter", ",").option("mode", "DROPMALFORMED").option("inferSchema", "true").load(path).cache()
    records = records.withColumn(idCol, monotonically_increasing_id())
    records
  }

  def resolve(ds1: DataFrame, ds2: DataFrame, pk1: String, pk2: String, rules: List[(List[(String, String, Int, Double)], Double)], idCol: String, spark: SparkSession): Unit = {
    // generate edges
    import spark.implicits._

    println(s"Number of records before Entity Resolution ${ds1.count() + ds2.count()}")
    ds1.show()
    ds2.show()

    val edges: Dataset[Row] = ds1.as("ds1").join(ds2.as("ds2"), rules.map(x => conditions(x._1, x._2)).reduce(_ || _))

    val edgesRDD = edges.select(d1c(idCol), d2c(idCol)).map(r => Edge(r.getAs[VertexId](0), r.getAs[VertexId](1), null)).rdd

    println("Graph connections formed")
    edges.show()

    // generate nodes
    val nodesRDD = ds1.as("ds1").map(r => (r.getAs[VertexId](idCol), 1)).rdd

    // create graph
    val graph = Graph(nodesRDD, edgesRDD)

    // run connected components
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    val cc = graph.connectedComponents()

    // making sure that vertices with inDegree less than 1 do not show up in the results
    val cleanedCc = cc.inDegrees.toDF().as("inDegrees").join(cc.vertices.toDF().as("vertices"), col("inDegrees._1") === col("vertices._1")).select("vertices._1", "vertices._2")

    val out = cleanedCc.join(ds1.as("ds1"), col("_2") === col(idCol)).select(pk1, "_1").join(ds2.as("ds2"), col("_1") === col(idCol)).select(d1c(pk1), d2c(pk2)).distinct()

    out.show()
  }

  def d1c(columnName: String): String = {
    "ds1." + columnName
  }

  def conditions(matchCols: List[(String, String, Int, Double)], threshold: Double): Column = {
    matchCols.map(c => matchCriteria(c._1, c._2, c._3) * c._4).reduce(_ + _).geq(threshold)
  }

  def matchCriteria(ds1Col: String, ds2Col: String, maxDiff: Int): Column = {
    if (maxDiff == 0) {
      (col(d1c(ds1Col)) === col(d2c(ds2Col))).cast(IntegerType)
    } else {
      jwDistanceUdf(col(ds1Col), col(ds2Col))
      //      (length(col(d1c(ds1Col))) - levenshtein(col(d1c(ds1Col)), col(d1c(ds2Col)))) / length(col(d1c(ds1Col)))
    }
  }

  def d2c(columnName: String): String = {
    "ds2." + columnName
  }

  def combineColumns(dataset: DataFrame, columnListString: String): DataFrame = {
    dataset.withColumn(columnListString, concat_ws(" ", columnListString.split(",").map(c => col(c)): _*))
  }

}
