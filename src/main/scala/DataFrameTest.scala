import BroadbandCarrierSelection.parquet_file_location

object DataFrameTest extends SparkSessionProvider {


  def main(args: Array[String]): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.parquet(parquet_file_location)

    df.printSchema

    // after registering as a table you will be able to run sql queries
    df.registerTempTable("userdata")

    sqlContext.sql("select * from userdata").collect.foreach(println)

    df.select("first_name").show()

    df.filter(df("id") < 100).show()

    df.sort("first_name").show()


  }

}
