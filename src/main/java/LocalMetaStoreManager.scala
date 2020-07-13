import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, concat_ws, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

/* Created by Nimish.Bajaj on 06/07/20 */

object LocalMetaStoreManager {
  val path1 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/adobe_analytics.csv"
  val path2 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/hubspot.csv"
  val path3 = "/Users/Nimish.Bajaj/Documents/Courses/entity-resolution/data/apache-commons.csv"
  val pk1 = "ecid"
  val pk2 = "hubspotutk"
  val pk3 = "apid"

  def getDataset(datasetId: Long, idCol: String, operatorId: Long, columnIds: String): DataFrame = {
    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("fetch-datasets")
      .getOrCreate()

    val dataset = datasetId match {
      case 1 => getCSVData(path1, pk1, idCol, spark)
      case 2 => getCSVData(path2, pk2, idCol, spark)
      case 3 => getCSVData(path3, pk3, idCol, spark)
    }

    operatorId match {
      case 0 => dataset
      case 1 => combineColumns(dataset, columnIds)
    }
  }

  def getDataset(datasetId: Long, pk: String, idCol: String): DataFrame = {
    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("fetch-datasets")
      .getOrCreate()

    datasetId match {
      case 1 => getCSVData(path1, pk, idCol, spark)
      case 2 => getCSVData(path2, pk, idCol, spark)
      case 3 => getCSVData(path3, pk, idCol, spark)
    }
  }


  def getCSVData(path: String, pk: String, idCol: String, spark: SparkSession): DataFrame = {
    var records = spark.read.format("csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(path).cache()

    val r = new scala.util.Random()
    val id_gen: UserDefinedFunction = udf(() =>
      ((System.currentTimeMillis() >> 32) + "" + r.nextInt(math.pow(2, 31).toInt)).toLong)

    records = records.schema(pk).dataType match {
      case StringType => records.withColumn(idCol, id_gen())
      case IntegerType => records.withColumn(idCol, col(pk).cast(LongType))
      case LongType => records.withColumn(idCol, col(pk))
      case _ => records.withColumn(idCol, id_gen())
    }

    //    println(records.printSchema())
    records = records.persist()
    records
  }

  def combineColumns(dataset: DataFrame, columnListString: String): DataFrame = {
    dataset
      .withColumn(columnListString, concat_ws(" ", columnListString.split(",")
        .map(c => col(c)): _*))
  }
}