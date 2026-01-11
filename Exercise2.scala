import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Exercise2 {
  def main(args: Array[String]): Unit = {

    val inputCsv = "src/inputs/tweets.csv"
    val out1 = "outputs/topwords"
    val out2 = "outputs/topreason"

    val spark = SparkSession.builder()
      .appName("Exercise2")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputCsv)

    // Top 5 words per airline_sentiment
    // Clean text: lowercase and remove punctuation
    val cleaned = df.withColumn("clean_text", lower(regexp_replace(col("text"), "[^a-zA-Z0-9\\s]", " ")))

    // Split to words and explode (one word per row)
    val words = cleaned
      .withColumn("word", explode(split(col("clean_text"), "\\s+")))
      .filter(length(col("word")) > 0) // remove empty
      .filter(col("airline_sentiment").isin("positive", "negative", "neutral"))

    // Count words per sentiment
    val wordCounts = words
      .groupBy(col("airline_sentiment"), col("word"))
      .count()

    // Keep top 5 per sentiment using window
    val wSent = Window.partitionBy("airline_sentiment").orderBy(col("count").desc)

    val top5PerSentiment = wordCounts
      .withColumn("rn", row_number().over(wSent))
      .filter(col("rn") <= 5)
      .drop("rn")
      .orderBy(col("airline_sentiment"), col("count").desc)

    println("\n=== Top 5 words per sentiment ===")
    top5PerSentiment.show(200, truncate = false)

    // Save Q1 output
    top5PerSentiment
      .select(col("airline_sentiment"), col("word"), col("count"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(out1)

    //  Main complaint reason per airline (negativereason_confidence > 0.5)

    val complaints = df
      .filter(col("negativereason").isNotNull)
      .filter(col("airline").isNotNull)
      .filter(col("negativereason_confidence") > 0.5)

    val reasonCounts = complaints
      .groupBy(col("airline"), col("negativereason"))
      .count()

    val wAirline = Window.partitionBy("airline").orderBy(col("count").desc)

    val topReasonPerAirline = reasonCounts
      .withColumn("rn", row_number().over(wAirline))
      .filter(col("rn") === 1)
      .drop("rn")
      .orderBy(col("airline"))

    println("\n=== Top complaint reason per airline (negativereason_confidence > 0.5) ===")
    topReasonPerAirline.show(200, truncate = false)

    topReasonPerAirline
      .select(col("airline"), col("negativereason"), col("count"))
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(out2)

    spark.stop()
  }
}
