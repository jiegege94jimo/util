import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Word2Vec
import priv.lzj.util.HanFenci
import org.apache.spark.sql.functions._
object SparkJdbc{
  def fenci(message:String):Array[String] = {
    val result = HanFenci.participleWord(message)
    return result.split(",")
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").getOrCreate()

    Class.forName("oracle.jdbc.driver.OracleDriver")
    spark.udf.register("fenci",fenci _)
    val jdbc = spark.read.format("jdbc")
        .option("driver","oracle.jdbc.driver.OracleDriver")
      .option("url","jdbc:oracle:thin:@192.168.2.198:1521:orcl")
      .option("dbtable","TX_ASR_FLOW_RECORD_JH")
      .option("user","arask").option("password","arask1234").load()
    val needData = jdbc.select("ASR_CONTENT","ASR_ID","CONFIDENCE","ASR_DATE","ASR_BBH")





    val word2Vec = new Word2Vec()
      .setInputCol("ASR_CONTENT")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(needData)

    val result = model.transform(needData)



    print("111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
    print(result.first().toString())
//    val line = needData.first()
    print("2222222222222222222222222222222222222222222222222222222222222222")
//    print(line.toString())
  }
}