# MRAOADPP(method of recursive analysis and optimization of the abstract data processing pipeline)
This project is an experimental comparison of the method of recursive analysis and optimization of the abstract data processing pipeline (MRAOADPP) developed as part of master degree dissertation

**Environment setup for Window 10**

1. Install and setup java 8 - https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html
2. Install and setup scala 2.13 - https://www.scala-lang.org/download/
3. Install and setup spark 3 - https://spark.apache.org/downloads.html
4. Install and setup Maven - https://maven.apache.org/install.html
5. Setup Spark History server - https://medium.com/@eyaldahari/how-to-run-spark-history-server-on-windows-52cde350de07

**Data source**

https://www.kaggle.com/c/expedia-hotel-recommendations/

Put train.csv to project root

**Parquet source preparation**

1. Class for preparation coefficients creation - by.zhuk.experemental.datasetcreation.CoefficientCreation
2. Hotel recommendations parquet conversion - by.zhuk.experemental.datasetcreation.HotelCsvConvertToParquet

Results

Experiment №0 by.zhuk.experemental.experement.ExperimentDisabledCatalystOptimizer_0.scala
![](img/ExperimentDisabledCatalystOptimizer_0_result.jpg?raw=true)

Experiment №1 by.zhuk.experemental.experement.ExperimentRaw_1.scala
![](img/ExperimentRaw_1_result.jpg?raw=true)

Experiment №2 by.zhuk.experemental.experement.ExperimentFixJoin_2.scala
![](img/ExperimentFixJoin_2_result.jpg?raw=true)

Experiment №3 by.zhuk.experemental.experement.ExperimentFinal_3.scala
![](img/ExperimentFinal_3_result.jpg?raw=true)