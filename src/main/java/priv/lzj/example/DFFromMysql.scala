package priv.lzj.example

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 从mysql中创建dataframe
  * 并重新导回至mysql数据库
  */
object DFFromMysql{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DFFromMysql").master("local").getOrCreate()

    val mysqlDf = spark.read.format("jdbc")
      .option("url","jdbc:mysql://diaoye.com:3306/lzj")
      .option("dbtable","sparkTest")
      .option("user","root")
      .option("password","1234")
      .load()
    mysqlDf.show()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "1234")
    mysqlDf.write.mode(SaveMode.Append).jdbc("jdbc:mysql://diaoye.com:3306/lzj","sparkdf",connectionProperties)
  }
}