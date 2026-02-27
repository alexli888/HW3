import org.apache.spark.sql.SparkSession

object P2Query1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("P2Query1")
      .master("local")
      .getOrCreate()

    // Read purchases file P with header and inferSchema
    val purchases = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/Purchases.csv")

    // Register as temp view and run Spark SQL to filter TransTotal <= 100
    purchases.createOrReplaceTempView("P")
    val t1 = spark.sql(
      """
        |SELECT *
        |FROM P
        |WHERE CAST(TransTotal AS DOUBLE) <= 100
      """.stripMargin)

    // Write result
    t1.write
      .option("header", "true")
      .mode("overwrite")
      .csv("target/generated-sources/T1")

    // show count
    println(s"Records written to target/generated-sources/T1: ${t1.count()}")

    spark.stop()
  }
}
