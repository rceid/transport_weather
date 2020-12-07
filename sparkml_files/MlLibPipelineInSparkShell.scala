import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

case class Document(id: Long, text: String)
case class LabeledDocument(id: Long, text: String, label: Double)

// Load 2 types of emails from text files: spam and ham (non-spam).
// Each line has text from one email.
val spam = sc.textFile("/LearningSpark/spam.txt")
val ham = sc.textFile("/LearningSpark/ham.txt")

// Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
val positiveExamples = spam.zipWithIndex().map { case (email, index) =>
  LabeledDocument(index, email, 1.0)
}
val negativeExamples = ham.zipWithIndex().map { case (email, index) =>
  LabeledDocument(index, email, 0.0)
}
val trainingData = positiveExamples ++ negativeExamples

// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
// Each stage outputs a column in a SchemaRDD and feeds it to the next stage's input column.
val tokenizer = new Tokenizer(). // Splits each email into words
  setInputCol("text").
  setOutputCol("words")
val hashingTF = new HashingTF(). // Maps email words to vectors of 100 features.
  setNumFeatures(100).
  setInputCol(tokenizer.getOutputCol).
  setOutputCol("features")
val lr = new LogisticRegression() // LogisticRegression uses inputCol "features" by default.
val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

// Fit the pipeline to training documents.
// RDDs of case classes work well with Pipelines since Spark SQL can infer a schema from
// case classes and convert the data into a SchemaRDD.
val model = pipeline.fit(spark.createDataFrame(trainingData))

// Make predictions on test documents.
// The fitted model automatically transforms features using Tokenizer and HashingTF.
val testSpam = "O M G GET cheap stuff by sending money to ..."
val testHam = "Hi Dad, I started studying Spark the other ..."
val testData = sc.parallelize(Seq(
  Document(0, testSpam), // positive example (spam)
  Document(1, testHam)   // negative example (ham)
))
val predictions = model.transform(spark.createDataFrame(testData)).
  select("id", "prediction").collect().
  map { case Row(id, prediction) => (id, prediction) }.toMap
println(s"Prediction for testSpam: ${predictions(0)}")
println(s"Prediction for testHam: ${predictions(1)}")
