package by.zhuk.experemental.experement

import org.apache.spark.sql.functions.{col, input_file_name, lit, substring, substring_index}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

object ExperimentFinal_3 {


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("ExperimentFinal")
      .config("spark.eventLog.enabled", "true")
      .config("spark.sql.shuffle.partitions", "32")
      .master("local[*]")
      .config("spark.history.fs.logDirectory", "file:/D:/tmp/")
      .getOrCreate()

    val df = session
      .read
      .format("parquet")
      .option("header", "true")
      .load("hotel_parquet_partition_by_date/srch_ci=2014-01-*")
      .withColumn("srch_ci",
        substring_index(substring_index(substring_index(input_file_name(), "/", -2), "/", 1), "=", -1)
      )

    val df2 = session
      .read
      .format("parquet")
      .load("coefficient_ci_parquet")
      .filter(
        substring(col("date"), 0, 7) === "2014-01"
      )

    val df3 = session
      .read
      .format("parquet")
      .load("coefficient_co_parquet")

    val dfFinal = df
      .join(df2, df("srch_ci") === df2("date"), "left")
      .drop(df2("date"))
      .join(df3, df("srch_co") === df3("date"), "left")
      .drop(df3("date"))
      .withColumn("coefficient", col("coefficient_co") + col("coefficient_ci"))
      .drop("coefficient_co", "coefficient_ci")

    dfFinal.cache()
    dfFinal
      .select("date_time", "coefficient")
      .distinct()
      .write.mode(SaveMode.Overwrite)
      .format("parquet")
      .save("res_1_final_parquet")

    dfFinal
      .write.mode(SaveMode.Overwrite)
      .format("parquet")
      .save("res_2_final_parquet")

    session.close()

  }

}
