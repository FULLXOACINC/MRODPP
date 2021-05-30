package by.zhuk.experemental.experement

import org.apache.spark.sql.functions.{col, substring, substring_index}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

object ExperimentRaw_1 {


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("ExperimentRaw")
      .config("spark.eventLog.enabled", "true")
      .config("spark.sql.shuffle.partitions", "32")
      .master("local[4]")
      .config("spark.history.fs.logDirectory", "file:/D:/tmp/")
      .getOrCreate()

    val df = session
      .read
      .format("csv")
      .option("header", "true")
      .load("train.csv")

    val df2 = session
      .read
      .format("csv")
      .option("header", "true")
      .load("coefficient_ci_csv")

    val df3 = session
      .read
      .format("csv")
      .option("header", "true")
      .load("coefficient_co_csv")

    val dfFirstCoefficient = df
      .join(df2, df("srch_ci") === df2("date"), "left")
      .drop(df2("date"))
      .withColumnRenamed("coefficient_ci", "coefficient")


    val dfSecondCoefficient = df
      .join(df3, df("srch_co") === df3("date"), "left")
      .drop(df3("date"))
      .withColumnRenamed("coefficient_co", "coefficient")

    val dfFinal = dfFirstCoefficient
      .union(dfSecondCoefficient)
      .groupBy("date_time", "site_name", "posa_continent", "user_location_country", "user_location_region", "user_location_city", "orig_destination_distance", "user_id", "is_mobile", "is_package", "channel", "srch_ci", "srch_co", "srch_adults_cnt", "srch_children_cnt", "srch_rm_cnt", "srch_destination_id", "srch_destination_type_id", "is_booking", "cnt", "hotel_continent", "hotel_country", "hotel_market", "hotel_cluster")
      .agg(functions.sum("coefficient").as("coefficient"))
      .filter(
        substring(col("srch_ci"), 0, 7) === "2014-01"
      )


    dfFinal
      .select("date_time", "coefficient")
      .distinct()
      .write.mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", value = true)
      .save("res_1_raw_csv")

    dfFinal
      .write.mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", value = true)
      .save("res_2_raw_csv")

    session.close()

  }

}
