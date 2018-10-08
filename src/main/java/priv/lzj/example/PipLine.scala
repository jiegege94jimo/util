package priv.lzj.example

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object PipLine{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("pipTest").getOrCreate()
    val testData = spark.createDataFrame(
      Seq(("id1", 100,Array("我","去你","大二")), ("id2", 300,Array("大爷","来呢了")), ("id3", 800,Array("加上","对话","爱上你地方")), ("id4", 800,Array("加上对话","爱上你地方"))))
      .toDF("id", "year","text")
    testData.show()

    val word2Vec = new Word2Vec()
    word2Vec.setInputCol("text").setOutputCol("vectors").setMinCount(1)
    val kmeans = new KMeans().setK(2).setSeed(1L).setFeaturesCol("vectors").setPredictionCol("juhedian")
    val pipeline = new Pipeline().setStages(Array(word2Vec,kmeans))
    val model = pipeline.fit(testData)
    model.transform(testData).show()

    spark.stop()
  }
}
