import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object P2Query4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("P2Query4")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Read T1 and Customers
    val t1Path = "data/T1.csv"
    val customersPath = "data/Customers.csv"

    val t1 = spark.read.option("header", "true").option("inferSchema", "true").csv(t1Path)
    val customers = spark.read.option("header", "true").option("inferSchema", "true").csv(customersPath)

    val t1Norm = t1.select($"CustID", $"TransTotal")

    val customersNorm = customers.select($"CustID", $"Salary", $"Address")

    // Aggregate total expenses per customer
    val expenses = t1Norm
      .groupBy("CustID")
      .agg(round(sum($"TransTotal"), 2).alias("TotalExpenses"))

    // Join w/ customers and find those who cannot cover expenses (Salary <= TotalExpenses)
    val joined = expenses.join(customersNorm, Seq("CustID"), "left")

    val cannotCover = joined
      .filter($"Salary".isNotNull && $"Salary" <= $"TotalExpenses")
      .select($"CustID", $"TotalExpenses", $"Salary", $"Address")
      .orderBy($"CustID")

    // Return results to client
    println(s"Customers who cannot cover their total expenses (Salary <= TotalExpenses):")
    cannotCover.show(false)

    // Print some sample tuples
    val rows = cannotCover.collect()
    rows.foreach { r =>
      val cid = if (r.isNullAt(0)) "NULL" else r.getAs[Any]("CustID").toString
      val total = if (r.isNullAt(1)) "NULL" else r.getAs[Any]("TotalExpenses").toString
      val sal = if (r.isNullAt(2)) "NULL" else r.getAs[Any]("Salary").toString
      val addr = if (r.isNullAt(3)) "NULL" else r.getAs[Any]("Address").toString
      println(s"CustID=$cid, TotalExpenses=$total, Salary=$sal, Address=$addr")
    }

    spark.stop()
  }
}
