

object BroadbandCarrierSelection extends SparkSessionProvider {

  override protected def applicationName: String = "BroadbandCarrierSelection"

  val parquet_file_location: String = sys.env.getOrElse("TEST_DATA_LOCATION", "D:\\testdata\\userdata1.parquet")


  def main(args: Array[String]): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.parquet(parquet_file_location)

    df.printSchema

    // after registering as a table you will be able to run sql queries
    df.registerTempTable("userdata")

    sqlContext.sql("select * from userdata").collect.foreach(println)

  }
}


//    val current_broadband_consolidate_carriers =
//      Try(
//        spark.
//          read.
//          parquet(parquet_file_location).
//          select("first_name", "last_name", "email").as("userdata")
//          distinct).
//        getOrElse(
//          spark.
//            sparkContext.
//            parallelize(Array("Male")))
//
//    println(current_broadband_consolidate_carriers.getClass)
//    val dff = current_broadband_consolidate_carriers
//    print(dff.getClass)