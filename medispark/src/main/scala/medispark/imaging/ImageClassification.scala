// =============================================================================
// MediSpark — Stage 7: Chest X-ray Image Classification (V2 — Handcrafted + GBT)
// Reads pre-extracted handcrafted pixel features + demographics from Parquet,
// then trains a GBT classifier for Normal vs Abnormal chest X-ray classification.
//
// Features: 16 total (14 handcrafted pixel features + 2 demographics)
//   - Pixel statistics: mean, std, skew, kurtosis, IQR, CV
//   - Spatial: center_mean, periphery_ratio
//   - Edge/texture: edge_density, gradient_mag_mean, entropy
//   - Symmetry: symmetry_lr_mean
//   - Profile: row_profile_std, col_profile_std
//   - Demographics: patient_age, patient_gender
//
// Input:  /medispark/xray_features/ (Parquet with pixel features)
// Output: /medispark/models/xray_model/ (saved model)
//         /medispark/evaluation/xray/ (evaluation metrics)
// =============================================================================

package medispark.imaging

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object ImageClassification {

  def main(args: Array[String]): Unit = {

    println("=" * 70)
    println("STAGE 7: CHEST X-RAY IMAGE CLASSIFICATION (V2 — Handcrafted + GBT)")
    println("=" * 70)

    val spark = SparkSession.builder()
      .appName("MediSpark-Stage7-ImageClassification")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
      .getOrCreate()

    import spark.implicits._

    try {
      // ── 1. Load pre-extracted features from HDFS ──────────────────────
      println("\n[1/7] Loading X-ray feature data from HDFS...")
      val rawDF = spark.read.parquet("hdfs://namenode:9000/medispark/xray_features/")
      val totalRecords = rawDF.count()
      println(s"      Loaded $totalRecords records")

      // Show class distribution
      val labelCounts = rawDF.groupBy("label").count().collect()
      labelCounts.foreach { row =>
        val lbl = if (row.getAs[Long]("label") == 0L) "Normal" else "Abnormal"
        println(s"      $lbl: ${row.getAs[Long]("count")}")
      }

      // ── 2. Define feature columns ─────────────────────────────────────
      println("\n[2/7] Defining feature columns...")

      val handcraftedCols = Array(
        "pixel_mean", "pixel_std", "pixel_skew", "pixel_kurtosis",
        "pixel_iqr", "pixel_cv",
        "center_mean", "periphery_ratio",
        "edge_density", "gradient_mag_mean",
        "entropy",
        "symmetry_lr_mean",
        "row_profile_std", "col_profile_std"
      )

      val demographicCols = Array("patient_age", "patient_gender")

      val featureCols = handcraftedCols ++ demographicCols
      val totalFeatures = featureCols.length

      println(s"      Handcrafted features: ${handcraftedCols.length}")
      println(s"      Demographic features: ${demographicCols.length}")
      println(s"      Total features: $totalFeatures")

      val castDF = rawDF.withColumn("label", rawDF("label").cast("double"))

      val cleanDF = castDF.na.drop(featureCols :+ "label")
      val cleanCount = cleanDF.count()
      println(s"      Clean records: $cleanCount")

      // ── 3. Build ML pipeline ──────────────────────────────────────────
      println("\n[3/7] Building GBT pipeline...")

      val assembler = new VectorAssembler()
        .setInputCols(featureCols)
        .setOutputCol("raw_features")
        .setHandleInvalid("skip")

      val scaler = new StandardScaler()
        .setInputCol("raw_features")
        .setOutputCol("features")
        .setWithStd(true)
        .setWithMean(true)

      val gbt = new GBTClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(200)
        .setMaxDepth(6)
        .setStepSize(0.1)
        .setSubsamplingRate(0.8)
        .setMinInstancesPerNode(5)
        .setSeed(42)

      val pipeline = new Pipeline()
        .setStages(Array(assembler, scaler, gbt))

      // ── 4. Train-test split with balanced training ────────────────────
      println("\n[4/7] Splitting data (80/20, balancing training set)...")

      val Array(origTrainDF, testDF) = cleanDF.randomSplit(Array(0.8, 0.2), seed = 42)
      println(s"      Test set (original distribution): ${testDF.count()} records")

      val trainNormal = origTrainDF.filter($"label" === 0.0)
      val trainAbnormal = origTrainDF.filter($"label" === 1.0)
      val trainNormalCount = trainNormal.count()
      val trainAbnormalCount = trainAbnormal.count()
      println(s"      Training before balance: Normal=$trainNormalCount, Abnormal=$trainAbnormalCount")

      val balancedTrainDF = if (trainNormalCount > trainAbnormalCount && trainAbnormalCount > 0) {
        val ratio = trainNormalCount.toDouble / trainAbnormalCount.toDouble
        val oversampledMinority = trainAbnormal.sample(withReplacement = true, fraction = ratio, seed = 42)
        trainNormal.unionAll(oversampledMinority)
      } else {
        origTrainDF
      }

      val cachedTrainDF = balancedTrainDF.cache()
      println(s"      Balanced training set: ${cachedTrainDF.count()} records")

      // ── 5. Train the model ────────────────────────────────────────────
      println(s"\n[5/7] Training GBT (200 iterations, maxDepth=6, $totalFeatures features)...")
      val startTime = System.currentTimeMillis()
      val model = pipeline.fit(cachedTrainDF)
      val trainTime = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"      Training complete in ${trainTime}s")

      // ── 6. Evaluate ───────────────────────────────────────────────────
      println("\n[6/7] Evaluating model...")
      val predictions = model.transform(testDF)

      val accuracyEval = new MulticlassClassificationEvaluator()
        .setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
      val accuracy = accuracyEval.evaluate(predictions)

      val f1Eval = new MulticlassClassificationEvaluator()
        .setLabelCol("label").setPredictionCol("prediction").setMetricName("f1")
      val f1 = f1Eval.evaluate(predictions)

      val precisionEval = new MulticlassClassificationEvaluator()
        .setLabelCol("label").setPredictionCol("prediction").setMetricName("weightedPrecision")
      val precision = precisionEval.evaluate(predictions)

      val recallEval = new MulticlassClassificationEvaluator()
        .setLabelCol("label").setPredictionCol("prediction").setMetricName("weightedRecall")
      val recall = recallEval.evaluate(predictions)

      val aucEval = new BinaryClassificationEvaluator()
        .setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
      val auc = aucEval.evaluate(predictions)

      println("\n" + "=" * 55)
      println("CHEST X-RAY CLASSIFICATION RESULTS (V2 — Handcrafted)")
      println("=" * 55)
      println(f"  Accuracy:  ${accuracy * 100}%.2f%%")
      println(f"  F1 Score:  ${f1 * 100}%.2f%%")
      println(f"  Precision: ${precision * 100}%.2f%%")
      println(f"  Recall:    ${recall * 100}%.2f%%")
      println(f"  AUC-ROC:   ${auc * 100}%.2f%%")

      println("\nConfusion Matrix:")
      println("                  Predicted Normal  Predicted Abnormal")
      val tp = predictions.filter($"label" === 1 && $"prediction" === 1).count()
      val tn = predictions.filter($"label" === 0 && $"prediction" === 0).count()
      val fp = predictions.filter($"label" === 0 && $"prediction" === 1).count()
      val fn = predictions.filter($"label" === 1 && $"prediction" === 0).count()
      println(f"  Actual Normal:     $tn%6d            $fp%6d")
      println(f"  Actual Abnormal:   $fn%6d            $tp%6d")

      val normalPrecision = if (tn + fn > 0) tn.toDouble / (tn + fn) else 0.0
      val normalRecall    = if (tn + fp > 0) tn.toDouble / (tn + fp) else 0.0
      val normalF1        = if (normalPrecision + normalRecall > 0)
        2 * normalPrecision * normalRecall / (normalPrecision + normalRecall) else 0.0

      val abnormalPrecision = if (tp + fp > 0) tp.toDouble / (tp + fp) else 0.0
      val abnormalRecall    = if (tp + fn > 0) tp.toDouble / (tp + fn) else 0.0
      val abnormalF1        = if (abnormalPrecision + abnormalRecall > 0)
        2 * abnormalPrecision * abnormalRecall / (abnormalPrecision + abnormalRecall) else 0.0

      println("\nPer-Class Metrics:")
      println(f"  Normal   — Precision: ${normalPrecision * 100}%.2f%%  Recall: ${normalRecall * 100}%.2f%%  F1: ${normalF1 * 100}%.2f%%")
      println(f"  Abnormal — Precision: ${abnormalPrecision * 100}%.2f%%  Recall: ${abnormalRecall * 100}%.2f%%  F1: ${abnormalF1 * 100}%.2f%%")

      val gbtModel = model.stages.last.asInstanceOf[org.apache.spark.ml.classification.GBTClassificationModel]
      val importances = gbtModel.featureImportances.toArray
      val featureImportanceMap = featureCols.zip(importances).sortBy(-_._2)

      println(s"\nFeature Importances (all $totalFeatures):")
      featureImportanceMap.foreach { case (name, imp) =>
        println(f"  $name%-25s $imp%.4f")
      }

      // ── 7. Save model and metrics ─────────────────────────────────────
      println("\n[7/7] Saving model and evaluation to HDFS...")
      model.write.overwrite().save("hdfs://namenode:9000/medispark/models/xray_model")
      println("  Model saved to /medispark/models/xray_model")

      val metricsJson = s"""{
        |  "accuracy": ${accuracy * 100},
        |  "f1": ${f1 * 100},
        |  "precision": ${precision * 100},
        |  "recall": ${recall * 100},
        |  "auc_roc": ${auc * 100},
        |  "confusion_matrix": [[$tn, $fp], [$fn, $tp]],
        |  "normal_precision": ${normalPrecision * 100},
        |  "normal_recall": ${normalRecall * 100},
        |  "normal_f1": ${normalF1 * 100},
        |  "abnormal_precision": ${abnormalPrecision * 100},
        |  "abnormal_recall": ${abnormalRecall * 100},
        |  "abnormal_f1": ${abnormalF1 * 100},
        |  "total_records": $totalRecords,
        |  "training_time_seconds": $trainTime,
        |  "model_type": "GBT",
        |  "features_count": $totalFeatures,
        |  "handcrafted_features": ${handcraftedCols.length},
        |  "demographic_features": ${demographicCols.length},
        |  "image_resolution": "128x128 (handcrafted)",
        |  "version": "V2-Handcrafted"
        |}""".stripMargin

      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val evalPath = new org.apache.hadoop.fs.Path("hdfs://namenode:9000/medispark/evaluation/xray")
      if (hdfs.exists(evalPath)) {
        hdfs.delete(evalPath, true)
      }

      val metricsRDD = spark.sparkContext.parallelize(Seq(metricsJson))
      metricsRDD.coalesce(1).saveAsTextFile("hdfs://namenode:9000/medispark/evaluation/xray")
      println("  Evaluation saved to /medispark/evaluation/xray")

      println("\n" + "=" * 70)
      println("STAGE 7 COMPLETE — Chest X-ray Image Classification (V2 — Handcrafted)")
      println("=" * 70)

    } catch {
      case e: Exception =>
        println(s"\n[ERROR] Stage 7 failed: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}
