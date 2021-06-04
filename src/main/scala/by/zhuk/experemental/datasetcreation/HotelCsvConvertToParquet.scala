package by.zhuk.experemental.datasetcreation

import org.apache.spark.sql.functions.{col, substring_index}
import org.apache.spark.sql.{SaveMode, SparkSession}

object HotelCsvConvertToParquet {


  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("SparkBasic")
      .config("spark.eventLog.enabled", "true")
      .config("spark.sql.shuffle.partitions", "16")
      .master("local[*]")
      .config("spark.history.fs.logDirectory", "file:/D:/tmp/")
      .getOrCreate()


    session
      .read
      .format("csv")
      .option("header", "true")
      .load("train.csv")
      .write.mode(SaveMode.Overwrite)
      .format("parquet")
      .partitionBy("srch_ci")
      .save("hotel_parquet_partition_by_date")



    session.close()

  }

}
