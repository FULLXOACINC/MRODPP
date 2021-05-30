package by.zhuk.experemental.datasetcreation

import org.apache.spark.sql.functions.{col, rand, substring_index}
import org.apache.spark.sql.{SaveMode, SparkSession}

object CoefficientCreation {


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("SparkBasic")
      .config("spark.eventLog.enabled", "true")
      .config("spark.sql.shuffle.partitions", "16")
      .master("local[*]")
      .config("spark.history.fs.logDirectory", "file:/D:/tmp/")
      .getOrCreate()

    val df1 = session
      .read
      .format("csv")
      .option("header", "true")
      .load("train.csv")
      .select(
        substring_index(col("srch_co"), " ", 1).as("date")
      )
      .distinct()
      .withColumn("coefficient_co", rand())

    df1.write.mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", value = true)
      .save("coefficient_co_csv")
    df1.write.mode(SaveMode.Overwrite)
      .format("parquet")
      .save("coefficient_co_parquet")


    val df3 = session
      .read
      .format("csv")
      .option("header", "true")
      .load("train.csv")
      .select(
        substring_index(col("srch_ci"), " ", 1).as("date")
      )
      .distinct()
      .withColumn("coefficient_ci", rand())


    df3.write.mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", value = true)
      .save("coefficient_ci_csv")
    df3.write.mode(SaveMode.Overwrite)
      .format("parquet")
      .save("coefficient_ci_parquet")


    session.close()

  }

}
