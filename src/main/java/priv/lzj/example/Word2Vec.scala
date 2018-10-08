package priv.lzj.example

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.callUDF
import priv.lzj.util.HanFenci

/**
  * 1.创建dataframe
  * 2.提取字段做切词
  * 3.调用word2vec模型完成词转向量
  */
object Word2Vec{
  def fenci(message:String):Array[String] = {
    val result = HanFenci.participleWord(message)
    return result.split(",")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("word2VecExample").getOrCreate()
    import spark.implicits._

    //1
    val df = Seq(("id1", 1, 100,"我去你大二"), ("id2", 4, 300,"大爷来呢了"), ("id3", 5, 800,"加上对话爱上你地方")).toDF("id", "value", "cnt","content")
    //2
    spark.udf.register("fenci",fenci _)
    val dt = df.select($"content",callUDF("fenci",$"content")).toDF("content","words")
    //3
    val word2Vec = new Word2Vec().setInputCol("words").setOutputCol("result").setVectorSize(3).setMinCount(0)
    val model = word2Vec.fit(dt)
    val re = model.transform(dt)
    df.show()
    dt.show()
    re.show()
  }
}