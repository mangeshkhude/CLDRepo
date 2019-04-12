import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.util.Try

trait SparkSessionProvider extends Serializable {

  private def localMode: String = "Local"

  private def clusterMode: String = "Master"

  protected def applicationName: String = "SparkSession"

  @transient lazy val spark: SparkSession = {
    Try(
      SparkSession.
        builder.
        appName(applicationName.concat(clusterMode)).
        getOrCreate
    ).getOrElse(
      SparkSession.
        builder.
        master("local[*]").
        appName(applicationName.concat(localMode)).
        config("spark.driver.host", "localhost").
        config("spark.ui.showConsoleProgress", "true").
        config("spark.ui.enabled", "false").
        getOrCreate
    )
  }

  @transient lazy val sc: SparkContext = spark.sparkContext
}
