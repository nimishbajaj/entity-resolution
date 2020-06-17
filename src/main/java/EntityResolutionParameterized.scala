import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}

object EntityResolutionParameterized {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  //  def getDataAndMirror(path: String, idCol: String, spark: SparkSession): (DataFrame, DataFrame) = {
  //    var records = spark.read.format("csv")
  //      .option("header", value = true)
  //      .option("delimiter", ",")
  //      .option("mode", "DROPMALFORMED")
  //      .option("inferSchema", "true")
  //      .load(path)
  //      .cache()
  //
  //    records = records.withColumn(idCol, col(idCol).cast(LongType))
  //
  //    val mirrorColNames = for (col <- records.columns) yield gm(col)
  //    val mirror = records.toDF(mirrorColNames: _*)
  //
  //    (records, mirror)
  //  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .master("local")
      .appName("entity-resolution")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")



    //    val map = Map("Email Domain" -> 1)
    //    val path = "data/Hubspot-contactsv3.csv"
    //    val sep = "\t"
    //    val idCol = "VID"

    // 1. take a RuleSet
    // 2. Start picking rules by priority
    // 3. Convert the rule into the map r seepresentation
    // 4. model rest of the code to run all the rules as per the priority


    // key -> (ds1Col, ds2Col, error, weight)
    val rule1 = List[Tuple4[String, String, Int, Double]](("ecid", "ecid", 0, 1.0))
    val threshold1 = 1

    val rule2 = List[Tuple4[String, String, Int, Double]](("zip", "zip", 0, 0.6), ("city", "city", 0, 0.2), ("ip", "ip", 0, 0.2))
    val threshold2 = 0.9

    val rules = List[(List[Tuple4[String, String, Int, Double]], Double)]((rule1, threshold1), (rule2, threshold2))

    val path = "data/adobe_analytics.csv"
    val sep = ","
    val idCol: String = "ecid"

    resolve(path, path, rules, 0.8, idCol, spark)

    // next stpe

    // yesterday I finished handling priority in the code
    // today after our discussion on the mapping table of user ids and customer 360 table. I will start with that.
    //
  }

  //  def gm(columnName: String): String = {
  //    "_" + columnName
  //  }

  def resolve(path1: String, path2: String, rules: List[(List[Tuple4[String, String, Int, Double]], Double)], threshold: Double, idCol: String, spark: SparkSession): Unit = {
    // generate edges
    import spark.implicits._

    val ds1 = getCSVData(path1, idCol, spark)
    val ds2 = getCSVData(path2, idCol, spark)

    println(s"Number of records before Entity Resolution ${ds1.count() + ds2.count()}")
    ds1.show()
    ds2.show()

    //    val ruleset = matchCols.map(c => s"Column: ${c._1} \n Error Tolerance: ${c._2._1} \n Weight: ${c._2._2} \n\n").reduce(_ + _)

    //    println(s"Rule Set: \n $ruleset")
    //    println(s"Threshold: $threshold")

    var edges: Dataset[Row] = ds1.as("ds1")
      .join(ds2.as("ds2"), rules.map(x => conditions(x._1, x._2)).reduce(_ || _))

    //    if (matchCols.map(c => c._2._3).sum > 0) {
    //      edges = edges.filter(matchCols
    //        .map(c => (c._1, c._2._3))
    //        .filter(c => c._2 == 1)
    //        .map(c => col(c._1) === col(gm(c._1)))
    //        .reduce(_ && _)
    //      )
    //    }

    val edgesRDD = edges.select(d1c(idCol), d2c(idCol))
      .map(r => Edge(r.getAs[VertexId](0), r.getAs[VertexId](1), null))
      .rdd

    println("Graph connections formed")
    edges.show()

    // generate nodes
    val nodesRDD = ds1.map(r => (r.getAs[VertexId](idCol), 1)).rdd

    // create graph
    val graph = Graph(nodesRDD, edgesRDD)

    // run connected components
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    val cc = graph.connectedComponents()

    val out = cc.vertices.toDF()
    val temp = ds1
      .join(out, col(idCol) === $"_1")
      .drop(d1c(idCol))
      .withColumnRenamed("_2", d2c(idCol))

    val exprs = ds1.columns.map(collect_set)
    val result = temp
      .groupBy(idCol)
      .agg(exprs.head, exprs.tail: _*)
      .drop(s"collect_set($$d1c(idCol))")
      .toDF(ds1.columns: _*)

    println(s"Number of records after Entity Resolution ${result.count()}")

    //    result.write.csv("newly-formed.csv")
    result.show()
  }

  def getCSVData(path: String, idCol: String, spark: SparkSession): DataFrame = {
    var records = spark.read.format("csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(path)
      .cache()

    records = records.withColumn(idCol, col(idCol).cast(LongType))
    records
  }

  def d2c(columnName: String): String = {
    "ds2." + columnName
  }

  def conditions(matchCols: List[Tuple4[String, String, Int, Double]], threshold: Double): Column = {
    //    val sumWeights = matchCols.map(c => c._2._2).sum
    matchCols.map(c => matchCriteria(c._1, c._2, c._3) * c._4).reduce(_ + _).gt(threshold)
  }

  def matchCriteria(ds1Col: String, ds2Col: String, maxDiff: Int): Column = {
    if (maxDiff == 0) {
      (col(d1c(ds1Col)) === col(d2c(ds2Col))).cast(IntegerType)
    } else {
      //      (soundex(col(c)) === soundex(col(gm(c)))).cast(IntegerType)
      (length(col(d1c(ds1Col))) - levenshtein(col(d1c(ds1Col)), col(d1c(ds2Col)))) / length(col(d1c(ds1Col)))
    }
  }

  def d1c(columnName: String): String = {
    "ds1." + columnName
  }

}