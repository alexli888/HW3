import org.apache.spark.sql.SparkSession

object P2Query2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("P2Query2")
      .master("local")
      .getOrCreate()

    // Read T1 from data/T1.csv
    val t1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/T1.csv")

    t1.createOrReplaceTempView("T1")
    println(s"Records read from data/T1.csv: ${t1.count()}")

    // agg T1 by TransNumItems and compute median, min, max
    val agg = spark.sql(
      """
        |SELECT CAST(TransNumItems AS INT) AS TransNumItems,
        |       percentile_approx(CAST(TransTotal AS DOUBLE), 0.5) AS medianTransTotal,
        |       MIN(CAST(TransTotal AS DOUBLE)) AS minTransTotal,
        |       MAX(CAST(TransTotal AS DOUBLE)) AS maxTransTotal
        |FROM T1
        |GROUP BY CAST(TransNumItems AS INT)
        |ORDER BY TransNumItems
      """.stripMargin)

    // Collect & print
    val results = agg.collect()
    println("Results (TransNumItems, medianTransTotal, minTransTotal, maxTransTotal):")
    results.foreach(row => println(row.mkString(", ")))

    spark.stop()
  }
}
