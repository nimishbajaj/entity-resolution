import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
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

  def matchCriteria(c: String, maxDiff: Int): Column = {
    if(maxDiff==0){
      (col(c) === col(gm(c))).cast(IntegerType)
    } else {
//      (soundex(col(c)) === soundex(col(gm(c)))).cast(IntegerType)
      (length(col(c)) - levenshtein(col(c), col(gm(c))))/length(col(c))
    }
  }


  def conditions(matchCols: Map[String, (Int, Int, Int)]): Column = {
    matchCols.map(c => matchCriteria(c._1, c._2._1)*c._2._2).reduce(_ + _)
  }


  def resolve(path: String, matchCols: Map[String, (Int, Int, Int)], threshold: Double, idCol: String, spark: SparkSession): Unit = {
    // generate edges
    import spark.implicits._

    val (records, mirror) = getDataAndMirror(path, idCol, spark)

    println(s"Number of records before Entity Resolution ${mirror.count()}")
    mirror.show()

    val ruleset = matchCols.map(c => s"Column: ${c._1} \n Error Tolerance: ${c._2._1} \n Weight: ${c._2._2} \n\n").reduce(_ + _)

    println(s"Rule Set: \n $ruleset")
    println(s"Threshold: $threshold")

    val sumWeights = matchCols.map(c => c._2._2).sum

    var edges: Dataset[Row] = records
      .join(mirror, (conditions(matchCols) / sumWeights) >= threshold)

    if(matchCols.map(c => c._2._3).sum > 0 ) {
      edges = edges.filter(matchCols
        .map(c => (c._1, c._2._3))
        .filter(c => c._2 == 1)
        .map(c => col(c._1) === col(gm(c._1)))
        .reduce(_ && _)
      )
    }

    val edgesRDD = edges.select(idCol,gm(idCol))
      .map(r => Edge(r.getAs[VertexId](0), r.getAs[VertexId](1), null))
      .rdd

    println("Graph connections formed")
    edges.show()

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


    // key -> (error, weight, mandatory, algorithm)
    val map = Map("zip" -> (0,4,0), "city" -> (0,1,0), "ip" -> (0,1,0))
    val path = "data/adobe_analytics.csv"
    val sep = ","
    val idCol:String  = "ecid"

    resolve(path, map, 0.8, idCol, spark)
  }

}
