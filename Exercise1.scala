import org.apache.spark.{SparkConf, SparkContext}

object Exercise1 {
  def main(args: Array[String]): Unit = {
    //create Spark context
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Exercise1")
    val sc = new SparkContext(sparkConf)

    val inputFile = "src/inputs/SherlockHolmes.txt"
    val outputDir = "outputs/output1"
    val txtFile = sc.textFile(inputFile)

    val results = txtFile.flatMap(line => line.split(" ")) // Split text to lines
      .map(word => word.replaceAll("[\\p{Punct}]", ""))    // remove punctuation
      .map(word => word.toLowerCase)  // lowercase
      .filter(word => word.matches("[a-z].*"))  // keep words starting with a-z (ignores digits)
      .map(word => (word.charAt(0), word.length)) // Create a tuple for each word: (word's first letter, word's length)
      .groupByKey() // Group the tuples based on the key
      .mapValues(lengths => lengths.sum.toDouble / lengths.size) // For each group calculate the length average
      .sortBy(-_._2) // Sort the tuples by the length average in descending order

    // Print all results
    results.collect().foreach(println)

    // Save the output as text
    results.map(_.toString().replace("(","").replace(")", "")).saveAsTextFile(outputDir)

    sc.stop()

  }
}
