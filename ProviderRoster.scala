package com.availity.spark.provider

import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types.{StructType, DateType, StringType,IntegerType}

import org.apache.spark.sql.functions.{concat_ws,count, lit, array, collect_list, col, month, avg,date_format}
//import org.apache.spark.sql.functions.concat_ws

object ProviderRoster  {

  def main(args: Array[String]): Unit = {
    println("Hello")
    val spark = SparkSession.builder()
      .appName("Provider Roster")
      .master("local[*]")
      .getOrCreate()
    val providersPath = "data/providers.csv"
    val visitsPath = "data/visits.csv"
    val outputTotalVisitsPath = "data/output/total_visits_per_provider"
    val outputVisitsPerMonthPath = "data/output/total_visits_per_provider_per_month"
//    val schema = new StructType()
//      .add(StructField("visits_id", IntegerType, nullable = true))
//      .add(StructField("provider_id", IntegerType, nullable = true))
//      .add(StructField("date_of_service", DateType, nullable = true))
    val providersDf = read_csv(spark,providersPath,"|","true")
    val visitsDf = read_csv(spark,visitsPath,",","false").toDF("visit_id","provider_id","date_of_service")
//    visitsDf.show()
    val joinedDF = visitsDf.join(providersDf,Seq("provider_id"))
    val visitsByProviderDf = joinedDF.withColumn("name", concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")))
      .groupBy("provider_id", "name", "provider_specialty")
      .agg(count("visit_id").as("total_visits"))

    val visitsByMonthDf = visitsDf.groupBy(col("provider_id"), date_format(col("date_of_service"), "yyyy-MM").as("month"))
      .agg(count("visit_id").as("total_visits"))
    visitsByProviderDf.coalesce(1).write.mode("overwrite").json(outputTotalVisitsPath)
    visitsByMonthDf.coalesce(1).write.mode("overwrite").json(outputVisitsPerMonthPath)

  }

  private def read_csv(spark: SparkSession, path: String, delimiter: String,header: String): DataFrame = {
    spark.read
      .option("header", header)
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(path)
  }

}
