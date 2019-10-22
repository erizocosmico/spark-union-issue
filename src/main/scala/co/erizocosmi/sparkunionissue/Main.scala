package co.erizocosmi.sparkunionissue

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("test issue app")
      .getOrCreate()

    spark.read.format("co.erizocosmi.sparkunionissue")
      .option("table", "products")
      .load()
      .createOrReplaceTempView("products")

    spark.read.format("co.erizocosmi.sparkunionissue")
      .option("table", "users")
      .load()
      .createOrReplaceTempView("users")

    val products = spark.sql("SELECT name, COUNT(*) as count FROM products GROUP BY name")
    val users = spark.sql("SELECT name, COUNT(*) as count FROM users GROUP BY name")

    products.union(users)
      .select("name")
      .explain(true)

    products.union(users)
      .select("name")
      .show(truncate = false, numRows = 50)
  }
}