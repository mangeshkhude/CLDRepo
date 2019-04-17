
import BroadbandCarrierSelection.parquet_file_location
import org.apache.spark.SparkContext
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}


object OperationsOnData extends SparkSessionProvider {

  case class Countries(Countries: String)

  def main(args: Array[String]): Unit = {
    val sqlContext: SQLContext = new SQLContext(sc)

    //writeParquet(sc, sqlContext)
    //readParquet(sqlContext)
    convertColumnAsRow(sqlContext)
    seprateNullValues(sqlContext)
    mergingSchemaDemo(sqlContext)
  }

  def writeParquet(sc: SparkContext, sqlContext: SQLContext) = {
    // Read file as RDD
    val rdd = sqlContext.read.format("csv").option("header", "true").load(".\\testdata\\countries.csv")
    // Convert rdd to data frame using toDF; the following import is required to use toDF function.
    val df: DataFrame = rdd.toDF()
    // Write file to parquet
    df.write.parquet(".\\testdata\\countries.parquet")
  }

  def readParquet(sqlContext: SQLContext) = {
    // read back parquet to DF
    val newDataDF = sqlContext.read.parquet(".\\testdata\\countries.parquet")
    // show contents
    newDataDF.show(40)
  }

  def convertColumnAsRow(sqlContext: SQLContext): Unit = {

    //take all columns in Array
    val newDataDF = sqlContext.read.parquet(".\\testdata\\countries.parquet")
    //Add Array to DataFrame
    val allColumns = newDataDF.columns

    // Encoders are created for case classes
    val caseClassDS = Seq(Countries(allColumns(0)))
    val caseClassDS1 = caseClassDS :+ Countries(allColumns(1))
    val caseClassDS2 = caseClassDS1 :+ Countries(allColumns(2))
    val caseClassDS3 = caseClassDS2 :+ Countries(allColumns(3))
    val caseClassDS4 = caseClassDS3 :+ Countries(allColumns(4))
    val caseClassDS5 = caseClassDS4 :+ Countries(allColumns(5))
    val caseClassDS6 = caseClassDS5 :+ Countries(allColumns(6))
    val caseClassDS7 = caseClassDS6 :+ Countries(allColumns(7))
    val caseClassDS8 = caseClassDS7 :+ Countries(allColumns(8))
    val caseClassDS9 = caseClassDS8 :+ Countries(allColumns(9))
    val caseClassDS10 = caseClassDS9 :+ Countries(allColumns(10))
    val caseClassDS11 = caseClassDS10 :+ Countries(allColumns(11))
    val caseClassDS12 = caseClassDS11 :+ Countries(allColumns(12))
    val caseClassDS13 = caseClassDS12 :+ Countries(allColumns(13))
    val caseClassDS14 = caseClassDS13 :+ Countries(allColumns(14))
    val caseClassDS15 = caseClassDS14 :+ Countries(allColumns(15))
    val caseClassDS16 = caseClassDS15 :+ Countries(allColumns(16))
    val caseClassDS17 = caseClassDS16 :+ Countries(allColumns(17))
    val caseClassDS18 = caseClassDS17 :+ Countries(allColumns(18))
    val caseClassDS19 = caseClassDS18 :+ Countries(allColumns(19))
    val caseClassDS20 = caseClassDS19 :+ Countries(allColumns(20))
    val caseClassDS21 = caseClassDS20 :+ Countries(allColumns(21))
    val caseClassDS22 = caseClassDS21 :+ Countries(allColumns(22))
    val caseClassDS23 = caseClassDS22 :+ Countries(allColumns(23))
    val caseClassDS24 = caseClassDS23 :+ Countries(allColumns(24))
    val caseClassDS25 = caseClassDS24 :+ Countries(allColumns(25))
    val caseClassDS26 = caseClassDS25 :+ Countries(allColumns(26))

    import spark.implicits._
    val newDF = caseClassDS25.toDF()
    try{
      newDF.write.format("parquet").save("countries.parquet/key=1")
    } catch {
      case ex: AnalysisException => {
        println("File Already Exists")
      }
    }
    val newDataDF1 = sqlContext.read.parquet("countries.parquet/key=1")
    println("From Parquet file")
    newDataDF1.show(100)
  }


  def seprateNullValues(sqlContext: SQLContext): Unit ={
    val newDataDF = sqlContext.read.parquet(".\\testdata\\countries.parquet")
    newDataDF.createOrReplaceTempView("countries")

    val sqlNullDF = spark.sql("SELECT * FROM countries where Afghanistan IS NULL")
    sqlNullDF.show()

    val sqlDataDF = spark.sql("SELECT * FROM countries where Afghanistan = 'Afghanistan'")
    sqlDataDF.show()
  }

  def mergingSchemaDemo(sqlContext: SQLContext): Unit ={
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.parquet(parquet_file_location)
    df.createOrReplaceTempView("alldata")
    val sqldf1 = spark.sql("SELECT * FROM alldata")
    sqldf1.show(1000, false)

    val df2 = sqlContext.read.parquet("countries.parquet/key=1")
    df2.createOrReplaceTempView("CountryNames")
    val sqldf2 = spark.sql("SELECT * FROM CountryNames")
    sqldf2.show()

    val rightjoindf = spark.sql("SELECT alldata.id, alldata.first_name, alldata.last_name, alldata.email, alldata.gender, alldata.title, alldata.country FROM alldata JOIN CountryNames ON alldata.country = CountryNames.Countries AND CountryNames.Countries = 'Argentina'")
    rightjoindf.show(1000, false)
  }

}

