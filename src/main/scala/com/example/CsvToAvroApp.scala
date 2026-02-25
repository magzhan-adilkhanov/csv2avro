package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.expr



import scala.jdk.CollectionConverters._

object CsvToAvroApp {
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load().getConfig("app")
    val spark = SparkSession.builder()
      .appName("CSV to Avro Converter")
      .master("local[*]")
      .getOrCreate()

    try {
      val inputPath = config.getString("sourceDir")
      val outputPath = config.getString("destDir")
      val delimiter = config.getString("delimiter")
      val dedupKey = config.getString("dedupKey")
      val partitionCol = if (config.hasPath("partitionCol")) Some(config.getString("partitionCol")) else None

      // Read CSV
      val rawDF = spark.read
        .option("header", "true")
        .option("delimiter", delimiter)
        .option("mode", "PERMISSIVE")
        .option("inferSchema", "true")
        .csv(inputPath)

      logger.info(s"Read ${rawDF.count()} records from $inputPath")

      // Schema mapping
      val schemaMapping = config.getConfig("schemaMapping").entrySet().asScala.map { entry =>
        entry.getKey -> entry.getValue.unwrapped().toString
      }.toMap

      val dateFormats = if (config.hasPath("dateFormats")) {
        config.getConfig("dateFormats").entrySet().asScala.map { entry =>
        entry.getKey -> config.getString(s"dateFormats.${entry.getKey}")
        }.toMap
      } else Map.empty[String, String]

      val timestampFormats = if (config.hasPath("timestampFormats")) {
        config.getConfig("timestampFormats").entrySet().asScala.map { entry =>
        entry.getKey -> config.getString(s"timestampFormats.${entry.getKey}")
        }.toMap
      } else Map.empty[String, String]

      // Type casting
      val castedDF = schemaMapping.foldLeft(rawDF) { case (df, (colName, targetType)) =>
        val format = dateFormats.get(colName).orElse(timestampFormats.get(colName))
        castColumn(df, colName, targetType, format)
      }

      // Add processing timestamp
      val withTimestampDF = castedDF.withColumn("processing_timestamp", current_timestamp())

      // Remove duplicates
      val dedupedDF = withTimestampDF.dropDuplicates(dedupKey :: Nil)

      // Write to Avro
      val writer = partitionCol match {
        case Some(col) => dedupedDF.write.partitionBy(col)
        case None      => dedupedDF.write
      }
      writer.format("avro").mode("overwrite").save(outputPath)

      logger.info(s"Wrote ${dedupedDF.count()} records to $outputPath")
    } catch {
      case e: Exception =>
        logger.error("Error during processing", e)
    } finally {
      spark.stop()
    }
  }

  def castColumn(df: DataFrame, colName: String, targetType: String, format: Option[String]): DataFrame = {
    targetType match {
      case "StringType"    => df.withColumn(colName, col(colName).cast(StringType))
      case "IntegerType"   => df.withColumn(colName, expr(s"try_cast($colName as int)"))
      case "LongType"      => df.withColumn(colName, expr(s"try_cast($colName as bigint)"))
      case "DoubleType"    => df.withColumn(colName, expr(s"try_cast($colName as double)"))
      case "FloatType"     => df.withColumn(colName, expr(s"try_cast($colName as float)"))
      case "BooleanType"   => df.withColumn(colName, expr(s"try_cast($colName as boolean)"))
      case "DateType"      => df.withColumn(colName, to_date(col(colName), format.getOrElse("yyyy-MM-dd")))
      case "TimestampType" => df.withColumn(colName, to_timestamp(col(colName), format.getOrElse("yyyy-MM-dd HH:mm:ss")))
      case dt if dt.startsWith("DecimalType") =>
        val pattern = "DecimalType\\((\\d+),(\\d+)\\)".r
        val pattern(precision, scale) = dt
        df.withColumn(colName, expr(s"try_cast($colName as decimal($precision,$scale))"))
      case _ => df
    }
  }
}