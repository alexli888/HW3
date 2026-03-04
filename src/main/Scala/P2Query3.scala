import org.apache.spark.sql.SparkSession

object P2Query3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("P2Query3")
      .master("local")
      .getOrCreate()

    // Read T1 from data/T1.csv
    val t1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/T1.csv")

    t1.createOrReplaceTempView("T1")
    println(s"Records read from data/T1.csv: ${t1.count()}")

    // Read Customers and register view
    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/Customers.csv")

    customers.createOrReplaceTempView("Customers")

    // Agg for GenZ customers, group purchases by customer
    val agg = spark.sql(
      """
        |SELECT
        |  c.CustID AS CustID,
        |  c.Age AS Age,
        |  SUM(CAST(t.TransNumItems AS INT)) AS totalItems,
        |  SUM(CAST(t.TransTotal AS DOUBLE)) AS totalAmount
        |FROM T1 t
        |JOIN Customers c
        |  ON t.CustID = c.CustID
        |WHERE c.Age BETWEEN 18 AND 21
        |GROUP BY c.CustID, c.Age
        |ORDER BY c.CustID
      """.stripMargin)

    // register and write T3 to data/T3.csv
    agg.createOrReplaceTempView("T3")
    agg.write
      .option("header", "true")
      .mode("overwrite")
      .csv("data/T3.csv")

    println(s"Records written to data/T3.csv: ${agg.count()}")
    println("Sample results (CustomerID, Age, totalItems, totalAmount):")
    agg.show(20, truncate = false)

    spark.stop()
  }
}
