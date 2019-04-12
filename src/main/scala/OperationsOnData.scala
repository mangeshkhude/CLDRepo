
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}


object OperationsOnData extends SparkSessionProvider {

  def main(args: Array[String]): Unit = {
    val sqlContext: SQLContext = new SQLContext(sc)
    // writeParquet(sc, sqlContext)
    readParquet(sqlContext)
    convertColumnAsRow(sqlContext)
  }

  def writeParquet(sc: SparkContext, sqlContext: SQLContext) = {
    // Read file as RDD
    val rdd = sqlContext.read.format("csv").option("header", "true").load("D:\\testdata\\countries.csv")
    // Convert rdd to data frame using toDF; the following import is required to use toDF function.
    val df: DataFrame = rdd.toDF()
    // Write file to parquet
    df.write.parquet("D:\\testdata\\countries.parquet")
  }

  def readParquet(sqlContext: SQLContext) = {
    // read back parquet to DF
    val newDataDF = sqlContext.read.parquet("D:\\testdata\\countries.parquet")
    // show contents
    newDataDF.show()
  }

  def convertColumnAsRow(sqlContext: SQLContext): Unit = {
    //take all columns in Array
    val newDataDF = sqlContext.read.parquet("D:\\testdata\\countries.parquet")
    //Add Array to DataFrame
    val allColumns = newDataDF.columns
    val allcolList = allColumns.toList
    for (column <- allColumns) {
      println(column)
    }
    val struct =
      StructType(
        StructField("Countries", StringType, true):: Nil)




  }
}
