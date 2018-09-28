import java.util.Properties

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import priv.lzj.util.HanFenci

object test{
  def fenci(message:String):Array[String] = {
    val result = HanFenci.participleWord(message)
    return result.split(",")
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("udfTest").getOrCreate()
    import spark.implicits._
    //val df = Seq(("id1", 1, 100,"我去你大二"), ("id2", 4, 300,"大爷来呢了"), ("id3", 5, 800,"加上对话爱上你地方")).toDF("id", "value", "cnt","content")

    spark.udf.register("fenci",fenci _)
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val jdbc = spark.read.format("jdbc")
      .option("driver","oracle.jdbc.driver.OracleDriver")
      .option("url","jdbc:oracle:thin:@192.168.2.198:1521:orcl")
      .option("dbtable","TX_ASR_FLOW_RECORD_JH")
      .option("user","arask").option("password","arask1234").load()

    val needData = jdbc.select("ASR_CONTENT","ASR_ID","CONFIDENCE","ASR_DATE","ASR_BBH")
    val dt = needData.select($"ASR_CONTENT",$"ASR_ID",$"CONFIDENCE",$"ASR_DATE",$"ASR_BBH",callUDF("fenci",$"ASR_CONTENT"))
      .toDF("ASR_CONTENT","ASR_ID","CONFIDENCE","ASR_DATE","ASR_BBH","words")
    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(dt)

    val re = model.transform(dt)
    val kmeans = new KMeans().setK(100).setSeed(1L).setFeaturesCol("result").setPredictionCol("juhedian")
    val modell = kmeans.fit(re)
    val predictions = modell.transform(re)

    val connectionProperties = new Properties()
    predictions.show(10)
    predictions.printSchema()
    connectionProperties.put("user", "arask")
    connectionProperties.put("password", "arask1234")
    connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

    val out = predictions.select("ASR_CONTENT","ASR_ID","juhedian","CONFIDENCE","ASR_BBH")

    out.show(10)
    out.printSchema()
    out.write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@192.168.2.198:1521:orcl","TX_ASR_FLOW_RECORD_JH_LZJ",connectionProperties)




  }
}
