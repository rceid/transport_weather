//Code guidance from the following three links:
//LR tutorial:
//https://medium.com/@awabmohammedomer/how-to-fit-a-linear-regression-model-in-apache-spark-using-scala-f246dd06a3b1
//model metrics:
//https://www.programcreek.com/scala/org.apache.spark.ml.evaluation.RegressionEvaluator
//making df's in scala:
//https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393

import org.apache.spark.sql.SaveMode
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import spark.implicits._
import org.apache.spark.mllib.evaluation.RegressionMetrics

//monthly and daily views for weather
val weather = spark.sql(
  """select SUBSTR(weather_date, 1, 4) as year, SUBSTR(weather_date, 6, 2) as month,
    SUBSTR(weather_date, 9, 2) as day, round(avg(precipitation), 2) as avg_precipitation,
    round(avg(snow_amount), 2) as avg_snow, cast(avg(avg_temp) as int) as avg_temp from reid7_weather
    group by day, month, year;""")

weather.createOrReplaceTempView("weather")

val divvy = spark.sql(
  """select tripid, SUBSTR(starttime, 1, 2) as month, SUBSTR(starttime, 4, 2) as day,
    SUBSTR(starttime, 7, 4) as year, tripduration, usertype, gender, birthyear 
    from reid7_divvy;""")

divvy.createOrReplaceTempView("divvy")

val divvy_weather  = spark.sql("""select tripid, d.day, d.month, d.year, 
  d.month || "-" || d.year as mo_yr, tripduration, usertype, gender, 
  (d.year - birthyear) as age, avg_precipitation, avg_snow, avg_temp from divvy d 
  join weather w on d.day = w.day and d.month = w.month and d.year= w.year""")

val df = divvy_weather.toDF()

//first regression

//designated outcome column, trip duration
var df1 = df.withColumnRenamed("tripduration", "label")

//preparing categorical variables for analysis
var monthIndexer = new StringIndexer()
  .setInputCol("month")
  .setOutputCol("monthIndexed")
df1 = monthIndexer.fit(df1).transform(df1)

var genderIndexer = new StringIndexer()
  .setInputCol("gender")
  .setOutputCol("genderIndexed")
df1 = genderIndexer.fit(df1).transform(df1)

var yearIndexer = new StringIndexer()
  .setInputCol("year")
  .setOutputCol("yearIndexed")
df1 = yearIndexer.fit(df1).transform(df1)

var subIndexer = new StringIndexer()
  .setInputCol("usertype")
  .setOutputCol("subscriberIndexed")
df1 = subIndexer.fit(df1).transform(df1)

var moYrIndexer = new StringIndexer()
  .setInputCol("mo_yr")
  .setOutputCol("mo_yrIndexed")
df1 = moYrIndexer.fit(df1).transform(df1)

//one-hot encoding non-binary categories
var monthencoder = new OneHotEncoder()
   .setInputCols(Array(monthIndexer.getOutputCol))
   .setOutputCols(Array("MonthEncoded"))
df1 = monthencoder.fit(df1).transform(df1)

//rows containing null in their columns must be dropped, otherwise LR won't run.
df1 = df1.na.drop()

//bringing together all features into one column. LR1
var assembler = new VectorAssembler()
  .setInputCols(Array("monthIndexed","genderIndexed","yearIndexed",
                      "subscriberIndexed","mo_yrIndexed","age",
                      "avg_precipitation","avg_snow","avg_temp"))
  .setOutputCol("features")
  .setHandleInvalid("skip")
var df2 = assembler.transform(df1)

//train test split
var Array(train1, test1) = df2.randomSplit(Array(.8, .2), seed=42)
var lr = new LinearRegression()
var lrModel = lr.fit(train1)
var lrPredictions = lrModel.transform(test1)

//getting RMSE and R2 metrics:
val rmseEvaluator = new RegressionEvaluator()
  .setMetricName("rmse")
  .setLabelCol("label")
  .setPredictionCol("prediction")
var rmse = rmseEvaluator.evaluate(lrPredictions)
rmse = rmse - (rmse % 0.01)

val r2Evaluator = new RegressionEvaluator()
  .setMetricName("r2")
  .setLabelCol("label")
  .setPredictionCol("prediction")
var r2 = r2Evaluator.evaluate(lrPredictions)
r2 = r2 - (r2 % 0.0001)

//rmse = 750.01
//r2 = 0.0976


//LR2
var assembler = new VectorAssembler()
  .setInputCols(Array("genderIndexed", "subscriberIndexed","age",
                      "avg_precipitation","avg_snow","avg_temp"))
  .setOutputCol("features")
  .setHandleInvalid("skip")
var df3 = assembler.transform(df1)
//train test split
var Array(train2, test2) = df3.randomSplit(Array(.8, .2), seed=42)
var lr = new LinearRegression()
var lrModel = lr.fit(train2)
var lrPredictions = lrModel.transform(test2)
//getting RMSE and R2 metrics:
var rmse_2 = rmseEvaluator.evaluate(lrPredictions)
var r2_2 = r2Evaluator.evaluate(lrPredictions)
rmse_2 = rmse_2 - (rmse_2 % 0.01)
r2_2 = r2_2 - (r2_2 % 0.00001)
//rmse = 750.01
//r2 = 0.0973


// LR3
var assembler = new VectorAssembler()
  .setInputCols(Array("avg_precipitation","avg_snow","avg_temp"))
  .setOutputCol("features")
  .setHandleInvalid("skip")
var df4 = assembler.transform(df1)
//train test split
var Array(train3, test3) = df4.randomSplit(Array(.8, .2), seed=42)
var lr = new LinearRegression()
var lrModel = lr.fit(train3)
var lrPredictions = lrModel.transform(test3)
//getting RMSE and R2 metrics:
var rmse_3 = rmseEvaluator.evaluate(lrPredictions)
var r2_3 = r2Evaluator.evaluate(lrPredictions)
rmse_3 = rmse_3 - (rmse_3 % 0.01)
r2_3 = r2_3 - (r2_3 % 0.0001)
//rmse = 785.54
//r2 = 0.01

//Random forests with same data subsets
//rf1 cannot year month_year col because it has too many bins, removing it.
var assembler = new VectorAssembler()
  .setInputCols(Array("monthIndexed","genderIndexed","yearIndexed",
                      "subscriberIndexed","age",
                      "avg_precipitation","avg_snow","avg_temp"))
  .setOutputCol("features")
  .setHandleInvalid("skip")
var df2 = assembler.transform(df1)

//train test split
var Array(train1, test1) = df2.randomSplit(Array(.8, .2), seed=42)

var rf = new RandomForestRegressor()
var rfModel = rf.fit(train1)
var rfPredictions = rfModel.transform(test1)
var rf_rmse_1 = rmseEvaluator.evaluate(rfPredictions)
var rf_r2_1 = r2Evaluator.evaluate(rfPredictions)
//rounding metrics for cleaner rending in html later
rf_rmse_1 = rf_rmse_1 - (rf_rmse_1 % .01)  
rf_r2_1 = rf_r2_1 - (rf_r2_1 % .001)

//rf2
var rf = new RandomForestRegressor()
var rfModel = rf.fit(train2)
var rfPredictions = rfModel.transform(test2)
var rf_rmse_2 = rmseEvaluator.evaluate(rfPredictions)
var rf_r2_2 = r2Evaluator.evaluate(rfPredictions)
rf_rmse_2 = rf_rmse_2 - (rf_rmse_2 % .1)  
rf_r2_2 = rf_r2_2 - (rf_r2_2 % .001)
//rmse = 748.5
//r2 = 0.101

//rf3
var rf = new RandomForestRegressor()
var rfModel = rf.fit(train3)
var rfPredictions = rfModel.transform(test3)
var rf_rmse_3 = rmseEvaluator.evaluate(rfPredictions)
var rf_r2_3 = r2Evaluator.evaluate(rfPredictions)
rf_rmse_3 = rf_rmse_3 - (rf_rmse_3 % 1)  
rf_r2_3 = rf_r2_3 - (rf_r2_3 % .0001)
//rmse = 789
//r2 =  0.0099

val mlDF = Seq(
  ("Linear Regression", "X", "X", "X", rmse, r2 ),
  ("Linear Regression", "-", "X", "X", rmse_2, r2_2),
  ("Linear Regression", "-", "-", "X", rmse_3, r2_3),
  ("Random Forest Regression", "X", "X", "X", rf_rmse_1, rf_r2_1),
  ("Random Forest Regression", "-", "X", "X", rf_rmse_2, rf_r2_2),
  ("Random Forest Regression", "-", "-", "X", rf_rmse_3, rf_r2_3)
).toDF("model", "date_features", "rider_features", "weather_features", "rmse", "r2")

mlDF.write.mode(SaveMode.Overwrite).saveAsTable("reid_ML_results")

