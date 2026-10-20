// ==============================================================================
// MediSpark — Stage 4: Model Training
// RandomForest classifiers for all 4 diseases, 80/20 split, save to HDFS
// ==============================================================================
package medispark.training

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}

object ModelTraining {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MediSpark-Stage4-Training")
      .getOrCreate()

    println("=" * 70)
    println("  MEDISPARK — STAGE 4: MODEL TRAINING")
    println("=" * 70)

    val inPath    = "hdfs://namenode:9000/medispark/features"
    val modelPath = "hdfs://namenode:9000/medispark/models"
    val splitPath = "hdfs://namenode:9000/medispark/splits"

    val diseases = Seq("heart", "diabetes", "stroke", "lung_cancer")

    diseases.foreach { disease =>
      println(s"\n── Training: $disease ──")

      val data = spark.read.parquet(s"$inPath/$disease")
      val totalRows = data.count()
      println(s"  Total samples: $totalRows")

      // Class distribution
      data.groupBy("label").count().orderBy("label").show()

      // Leakage-safe split: honor the train/test tag assigned during
      // augmentation (synthetic neighbors were generated separately per
      // split), rather than randomly re-splitting the already-augmented
      // data — a random split here would scatter near-duplicate synthetic
      // siblings across train/test and inflate test accuracy.
      val train = data.filter(data("_split") === "train")
      val test = data.filter(data("_split") === "test")
      println(s"  Train: ${train.count()}, Test: ${test.count()}")

      // Configure RandomForest
      val numTrees = disease match {
        case "lung_cancer" => 150   // multi-class needs more trees
        case _             => 100
      }

      val rf = new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setNumTrees(numTrees)
        .setMaxDepth(10)
        .setMinInstancesPerNode(5)
        .setSeed(42L)
        .setFeatureSubsetStrategy("sqrt")

      // Train
      val startTime = System.currentTimeMillis()
      val model = rf.fit(train)
      val trainTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(f"  Training time: $trainTime%.1f seconds")

      // Quick evaluation on test set
      val predictions = model.transform(test)

      val mcEval = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")

      val accuracy = mcEval.setMetricName("accuracy").evaluate(predictions)
      val f1 = mcEval.setMetricName("f1").evaluate(predictions)

      println(f"  Test Accuracy: ${accuracy * 100}%.2f%%")
      println(f"  Test F1 Score: ${f1 * 100}%.2f%%")

      // Feature importances
      println("  Top 5 Feature Importances:")
      val importances = model.featureImportances.toArray.zipWithIndex
        .sortBy(-_._1).take(5)
      importances.foreach { case (imp, idx) =>
        println(f"    Feature[$idx]: $imp%.4f")
      }

      // Save model
      model.write.overwrite().save(s"$modelPath/$disease")
      println(s"  Model saved: $modelPath/$disease")

      // Save test data for evaluation stage
      test.write.mode("overwrite").parquet(s"$splitPath/${disease}_test")
      // Save predictions for evaluation
      predictions.write.mode("overwrite").parquet(s"$splitPath/${disease}_predictions")
      println(s"  Test data saved: $splitPath/${disease}_test")
    }

    println("\n" + "=" * 70)
    println("  STAGE 4 COMPLETE — All models trained and saved")
    println("=" * 70)

    spark.stop()
  }
}
