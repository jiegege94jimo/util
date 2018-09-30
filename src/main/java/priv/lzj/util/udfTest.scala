package priv.lzj.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object udfTest{
  def fenci(message:String):Array[String] = {
    val result = HanFenci.participleWord(message)
    return result.split(",")
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("udfTest").getOrCreate()
    import spark.implicits._
    val df = Seq(("id1", 1, 100,"我去你大二"), ("id2", 4, 300,"大爷来呢了"), ("id3", 5, 800,"加上对话爱上你地方")).toDF("id", "value", "cnt","content")

    print("注册udf函数-------------------------------------------------------------------------------")
    spark.udf.register("fenci",fenci _)

    val b = df.select($"id",$"content",callUDF("fenci",$"content"))
    print("显示df-----------------------------------------------------------------------------------")
    b.show()
    print(b.first().toString())

  }
}