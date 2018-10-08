import java.util.Properties

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.callUDF
import priv.lzj.util.HanFenci

object MlTest{
  def fenci(message:String):Array[String] = {
    val result = HanFenci.participleWord(message)
    return result.split(",")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("udfTest").getOrCreate()
    import spark.implicits._

    spark.udf.register("fenci",fenci _)

    val mysqlDf = spark.read.format("jdbc")
      .option("url","jdbc:mysql://diaoye.com:3306/lzj")
      .option("dbtable","sparkTest")
      .option("user","root")
      .option("password","1234")
      .load()

    val needData = mysqlDf.select("ASR_CONTENT","ASR_ID","CONFIDENCE","ASR_DATE","ASR_BBH")
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
