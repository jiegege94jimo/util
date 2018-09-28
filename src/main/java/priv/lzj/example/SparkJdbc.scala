import org.apache.spark.sql.SparkSession
object SparkJdbc{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").getOrCreate()
    print("start load driver")
    Class.forName("oracle.jdbc.driver.OracleDriver")
    print("start load data")
    val jdbc = spark.read.format("jdbc")
        .option("driver","oracle.jdbc.driver.OracleDriver")
      .option("url","jdbc:oracle:thin:@192.168.2.198:1521:orcl")
      .option("dbtable","TX_ASR_FLOW_RECORD_JH")
      .option("user","arask").option("password","arask1234").load()
    val needData = jdbc.select()
    print("111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
    val line = jdbc.first()
    print("2222222222222222222222222222222222222222222222222222222222222222")
    print(line.toString())

  }
}