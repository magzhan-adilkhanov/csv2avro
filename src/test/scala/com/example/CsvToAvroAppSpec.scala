package com.example

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class CsvToAvroAppSpec extends AnyFunSuite {
  val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()
  import spark.implicits._

  test("castColumn should cast to IntegerType") {
    val df = Seq(("1"), ("2"), ("abc")).toDF("id")
    val casted = CsvToAvroApp.castColumn(df, "id", "IntegerType", None)
    val result = casted.collect().map(_.get(0))
    assert(result.contains(1) && result.contains(2) && result.contains(null))
  }

  test("castColumn should cast to DateType with format") {
    val df = Seq(("2023-12-31"), ("2024-01-01"), ("bad")).toDF("createdDate")
    val casted = CsvToAvroApp.castColumn(df, "createdDate", "DateType", Some("yyyy-MM-dd"))
    val result = casted.collect().map(_.get(0))
    assert(result(0) != null && result(1) != null && result(2) == null)
  }
}