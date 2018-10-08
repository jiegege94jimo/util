package priv.lzj.example

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 从mysql中创建dataFrame
  * 并重新导回至mysql数据库
  */
object DFFromMysql{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DFFromMysql").master("local").getOrCreate()

    //从mysql加载数据
    val mysqlDf = spark.read.format("jdbc")
      .option("url","jdbc:mysql://diaoye.com:3306/lzj")
      .option("dbtable","sparkTest")
      .option("user","root")
      .option("password","1234")
      .load()
    mysqlDf.show()

    //df保存至mysql
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "1234")
    //SaveMode有Append，Overwrite，ErrorIfExists，Ignore模式，默认为:存在则报错
    mysqlDf.write.mode(SaveMode.Append).jdbc("jdbc:mysql://diaoye.com:3306/lzj","sparkdf",connectionProperties)
  }
}