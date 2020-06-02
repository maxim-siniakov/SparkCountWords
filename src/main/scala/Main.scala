import org.apache.spark.ml.feature.{ RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    val sqlContext = spark.sqlContext

    val df = sqlContext.read.text("1984.txt")

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("value")
      .setOutputCol("words")
      .setPattern("\\W")

    val regexTokenized = regexTokenizer.transform(df)

    StopWordsRemover.loadDefaultStopWords("english")
    val stopWordRemover = new StopWordsRemover().setInputCol("words").setOutputCol("filteredWords")
    val newDF = stopWordRemover.transform(regexTokenized)

    val wordsDF = newDF.select(explode(newDF.col("filteredWords").alias("word")))

    wordsDF.show(20)

    val result= wordsDF.groupBy("col").count().orderBy(desc("count"))
    result.coalesce(1).write.mode("overwrite").option("header","true").csv("result.csv")

  }
}