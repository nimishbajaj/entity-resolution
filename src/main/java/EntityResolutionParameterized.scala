import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.graphx._
import org.apache.spark.graphx.{Edge, Graph, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.commons.text.similarity
import org.apache.commons.text.similarity.JaroWinklerSimilarity

object EntityResolutionParameterized {

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  def getDataAndMirror(path: String, idCol: String, spark: SparkSession): (DataFrame, DataFrame) =  {
    var records = spark.read.format("csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(path)
      .cache()

    records = records.withColumn(idCol, col(idCol).cast(LongType))

    val mirrorColNames = for (col <- records.columns) yield gm(col)
    val mirror = records.toDF(mirrorColNames: _*)

    (records, mirror)
  }

  def gm(columnName: String): String = {
    "_" + columnName
  }

  def resolve(path: String, matchCols: Map[String, Int], idCol: String, spark: SparkSession): Unit = {
    // generate edges
    import spark.implicits._

    val (records, mirror) = getDataAndMirror(path, idCol, spark)

    println(s"Number of records before Entity Resolution ${mirror.count()}")
    mirror.show()

    val edges = records.join(mirror, conditions(matchCols))
    val edgesRDD = edges.select(idCol,gm(idCol))
      .map(r => Edge(r.getAs[VertexId](0), r.getAs[VertexId](1), null))
      .rdd

    // generate nodes
    val nodesRDD = records.map(r => (r.getAs[VertexId](idCol), 1)).rdd

    // create graph
    val graph = Graph(nodesRDD, edgesRDD)

    // run connected components
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    val cc = graph.connectedComponents()

    val out = cc.vertices.toDF()
    val temp = records
        .join(out, col(idCol) === $"_1")
        .drop(idCol)
        .withColumnRenamed("_2", idCol)

    val exprs = records.columns.map(collect_set)
    val result = temp
      .groupBy(idCol)
      .agg(exprs.head, exprs.tail: _*)
      .drop(s"collect_set($idCol)")
      .toDF(records.columns: _*)

    println(s"Number of records after Entity Resolution ${result.count()}")

//    result.write.csv("newly-formed.csv")
    result.show()
  }


  def matchCriteria(c: String, maxDiff: Int): Column = {
    if(maxDiff==0){
      col(c)===col(gm(c))
    } else {
      soundex(col(c)) === soundex(col(gm(c))) ||
      levenshtein(col(c), col(gm(c))) < maxDiff
    }
  }


  def conditions(matchCols: Map[String, Int]): Column = {
    matchCols.map(c => matchCriteria(c._1, c._2)).reduce(_ && _)
  }


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .master("local")
      .appName("entity-resolution")
      .getOrCreate()


//    val map = Map("Email Domain" -> 1)
//    val path = "data/Hubspot-contactsv3.csv"
//    val sep = "\t"
//    val idCol = "VID"

    val map = Map("zip" -> 0, "city" -> 1, "ip" -> 0)
    val path = "data/adobe_analytics.csv"
    val sep = ","
    val idCol:String  = "ecid"

    resolve(path, map, idCol, spark)
  }



}
