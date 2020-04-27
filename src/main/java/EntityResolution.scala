import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.graphx._
import org.apache.spark.graphx.{Edge, Graph, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object EntityResolution {

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .master("local")
      .appName("entity-resolution")
      .getOrCreate()

    val records = spark.read.format("csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load("data/adobe_analytics.csv")
      .cache()

    val mirrorColNames = for (col <- records.columns) yield "_"+col.toString
    val mirror = records.toDF(mirrorColNames: _*)

    println(s"Number of records before Entity Resolution ${mirror.count()}")
    mirror.collect.foreach(println)
//    mirror.printSchema()

    def conditions(matchCols: Seq[String]): Column = {
      col("ecid")===col("_ecid") ||
        (col("visid_high")===col("_visid_high") &&
          col("visid_low")===col("_visid_low") ) ||
        matchCols.map(c => col(c)===col("_"+c)).reduce(_ && _)
    }

  // survivorship rules
    // primary attributes - name, dob
    // secondary attributes - address, phone number etc
    // Record confidence

    // 

    val edges = records.join(mirror, conditions(Seq("zip", "city", "ip", "device")))

    import spark.implicits._
    val edgesRDD = edges.select("ecid","_ecid")
      .map(r => Edge(r.getAs[VertexId](0), r.getAs[VertexId](1), null))
      .rdd


    val nodesRDD = records.map(r => (r.getAs[VertexId]("ecid"), 1)).rdd

    val graph = Graph(nodesRDD, edgesRDD)

    val cc = graph.connectedComponents()

//    println(cc.edges.collect().mkString("\n"))
//    println(cc.vertices.collect().mkString("\n"))

    val out = cc.vertices.toDF()
    val temp = records.join(out, $"ecid"===$"_1")

//    println("Data After resolving the entities")
//    println(temp.collect().mkString("\n"))


//    println(temp.schema)

    val gg=temp.withColumnRenamed("_2", "mdpid")
      .groupBy("mdpid")
      .agg(
        concat_ws(" | ", collect_set($"emailhash_from_url") as "emailhash"),
        concat_ws(" | ", collect_set($"zip") as "zip"),
        concat_ws(" | ", collect_set($"city") as "city"),
        concat_ws(" | ", collect_set($"ip") as "ip"),
        concat_ws(" | ", collect_set($"device") as "device"))


    println(s"\n\nNumber of records after Entity Resolution ${gg.count()}")
    println(gg.collect().mkString("\n"))
  }

}
