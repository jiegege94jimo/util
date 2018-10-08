package priv.lzj.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.callUDF
import priv.lzj.util.HanFenci

/**
  * 用于dataFrame的udf函数例子
  */
object SparkUdf{

  /**
    * 定义一个测试函数，将输入数字乘二返回
    * @param num
    * @return
    */
  def numDouble(num:Int):Int = {
    val result = num +num
    return result
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("udfTest").master("local").getOrCreate()

    /**
      * 导入隐式转换，手动建立dataframe
      */
    import spark.implicits._
    val df = Seq(("id1", 1, 100,"我去你大二"), ("id2", 4, 300,"大爷来呢了"), ("id3", 5, 800,"加上对话爱上你地方")).toDF("id", "value", "cnt","content")
    df.show()

    /**
      * 注册udf函数后使用
      */
    spark.udf.register("numDouble",numDouble _)
    spark.udf.register("fenci",HanFenci.participleWord _)
    val b = df.select($"id",$"content",callUDF("numDouble",$"cnt"),callUDF("fenci",$"content"))
    b.show()
  }
}