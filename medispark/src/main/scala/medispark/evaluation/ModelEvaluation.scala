// ==============================================================================
// MediSpark — Stage 6: Model Evaluation
// Computes accuracy, precision, recall, F1, AUC-ROC, confusion matrix
// Saves results as JSON for the webapp
// ==============================================================================
package medispark.evaluation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}

object ModelEvaluation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MediSpark-Stage6-Evaluation")
      .getOrCreate()

    import spark.implicits._

    println("=" * 70)
    println("  MEDISPARK — STAGE 6: MODEL EVALUATION")
    println("=" * 70)

    val modelPath  = "hdfs://namenode:9000/medispark/models"
    val splitPath  = "hdfs://namenode:9000/medispark/splits"
    val resultPath = "hdfs://namenode:9000/medispark/evaluation"

    val diseases = Seq("heart", "diabetes", "stroke", "lung_cancer")

    diseases.foreach { disease =>
      println(s"\n${"=" * 60}")
      println(s"  EVALUATING: ${disease.toUpperCase.replace("_", " ")}")
      println(s"${"=" * 60}")

      // Load model and predictions
      val model = RandomForestClassificationModel.load(s"$modelPath/$disease")
      val predictions = spark.read.parquet(s"$splitPath/${disease}_predictions")
      val testCount = predictions.count()

      println(s"  Test samples: $testCount")

      // ── Multiclass metrics ────────────────────────────────────
      val mcEval = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")

      val accuracy  = mcEval.setMetricName("accuracy").evaluate(predictions)
      val f1        = mcEval.setMetricName("f1").evaluate(predictions)
      val precision = mcEval.setMetricName("weightedPrecision").evaluate(predictions)
      val recall    = mcEval.setMetricName("weightedRecall").evaluate(predictions)

      println(f"  Accuracy:  ${accuracy * 100}%.2f%%")
      println(f"  F1 Score:  ${f1 * 100}%.2f%%")
      println(f"  Precision: ${precision * 100}%.2f%%")
      println(f"  Recall:    ${recall * 100}%.2f%%")

      // ── AUC-ROC (binary only) ────────────────────────────────
      val numClasses = predictions.select("label").distinct().count().toInt
      var aucRoc = 0.0
      if (numClasses == 2) {
        try {
          val binEval = new BinaryClassificationEvaluator()
            .setLabelCol("label")
            .setRawPredictionCol("rawPrediction")
            .setMetricName("areaUnderROC")
          aucRoc = binEval.evaluate(predictions)
          println(f"  AUC-ROC:   ${aucRoc * 100}%.2f%%")
        } catch {
          case _: Exception =>
            println("  AUC-ROC:   N/A (computation error)")
        }
      } else {
        println(s"  AUC-ROC:   N/A (multi-class with $numClasses classes)")
      }

      // ── Confusion Matrix ──────────────────────────────────────
      println("\n  Confusion Matrix:")
      val cm = predictions.groupBy("label", "prediction")
        .count()
        .orderBy("label", "prediction")
      cm.show()

      // ── Feature Importances ───────────────────────────────────
      println("  Feature Importances (top 10):")
      val importances = model.featureImportances.toArray.zipWithIndex
        .sortBy(-_._1).take(10)
      importances.foreach { case (imp, idx) =>
        println(f"    Feature[$idx]: $imp%.4f")
      }

      // ── Per-class metrics ─────────────────────────────────────
      println("\n  Per-class Precision/Recall:")
      val classes = predictions.select("label").distinct()
        .collect().map(_.getDouble(0)).sorted

      classes.foreach { cls =>
        val tp = predictions.filter($"label" === cls && $"prediction" === cls).count()
        val fp = predictions.filter($"label" =!= cls && $"prediction" === cls).count()
        val fn = predictions.filter($"label" === cls && $"prediction" =!= cls).count()
        val prec = if (tp + fp > 0) tp.toDouble / (tp + fp) else 0.0
        val rec  = if (tp + fn > 0) tp.toDouble / (tp + fn) else 0.0
        val f1c  = if (prec + rec > 0) 2 * prec * rec / (prec + rec) else 0.0
        println(f"    Class $cls%.0f: Precision=$prec%.4f  Recall=$rec%.4f  F1=$f1c%.4f  Support=${tp+fn}")
      }

      // ── Save results as JSON ──────────────────────────────────
      val resultDf = Seq((
        disease,
        accuracy * 100, f1 * 100, precision * 100, recall * 100,
        aucRoc * 100, testCount, numClasses
      )).toDF("disease", "accuracy", "f1", "precision", "recall",
              "auc_roc", "test_samples", "num_classes")

      resultDf.write.mode("overwrite").json(s"$resultPath/$disease")
      println(s"\n  Results saved: $resultPath/$disease")
    }

    println("\n" + "=" * 70)
    println("  STAGE 6 COMPLETE — All models evaluated")
    println("=" * 70)

    spark.stop()
  }
}
