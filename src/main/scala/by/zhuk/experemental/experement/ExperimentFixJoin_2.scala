package by.zhuk.experemental.experement

import org.apache.spark.sql.functions.{col, substring, substring_index}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ExperimentFixJoin_2 {


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("ExperimentFixJoin")
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
      .filter(
        substring(col("srch_ci"), 0, 7) === "2014-01"
      )

    val df2 = session
      .read
      .format("csv")
      .option("header", "true")
      .load("coefficient_ci_csv")
      .filter(
        substring(col("date"), 0, 7) === "2014-01"
      )

    val df3 = session
      .read
      .format("csv")
      .option("header", "true")
      .load("coefficient_co_csv")

    val dfFinal = df
      .join(df2, df("srch_ci") === df2("date"), "left")
      .drop(df2("date"))
      .join(df3, df("srch_co") === df3("date"), "left")
      .drop(df3("date"))
      .withColumn("coefficient", col("coefficient_co") + col("coefficient_ci"))
      .drop("coefficient_co", "coefficient_ci")

    dfFinal
      .select("date_time", "coefficient")
      .distinct()
      .write.mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", value = true)
      .save("res_1_fix_join_csv")

    dfFinal
      .write.mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", value = true)
      .save("res_2_fix_join_csv")

    session.close()

  }

}
