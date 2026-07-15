// =============================================================================
// ModelTrainer.scala
// Stage 4 of MediSpark — Model Training & Evaluation
//
// What this does:
//   For each of the 4 disease datasets:
//     1. Reads feature-engineered Parquet from /medispark/features/{disease}
//     2. Splits data into 80% training / 20% testing
//     3. Trains a RandomForestClassifier on the training data
//     4. Evaluates on the test data (accuracy, precision, recall, F1-score)
//     5. Saves the trained model to /medispark/models/{disease}
//     6. Writes an evaluation report to /medispark/output/evaluation_report/
//
// Why Random Forest?
//   - Works for both binary (heart/diabetes/stroke) and multi-class (lung cancer)
//   - Handles high-dimensional data well (many features)
//   - Resistant to overfitting because it averages many decision trees
//   - No need for extensive hyperparameter tuning to get good results
//   - Provides feature importance rankings
// =============================================================================

package medispark.training

// --- IMPORTS ---
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

// RandomForestClassifier: the ML algorithm — builds many decision trees and
// combines their votes for a final prediction
import org.apache.spark.ml.classification.RandomForestClassifier
// RandomForestClassificationModel: the trained model object we can save and reuse
import org.apache.spark.ml.classification.RandomForestClassificationModel
// MulticlassClassificationEvaluator: calculates metrics like accuracy, precision,
// recall, and F1-score — works for both binary and multi-class problems
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object ModelTrainer {

  // =========================================================================
  // MAIN — entry point when Spark runs this job
  // =========================================================================
  def main(args: Array[String]): Unit = {

    // --- Step 1: Create SparkSession ---
    val spark = SparkSession.builder()
      .appName("MediSpark-ModelTrainer")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
      .getOrCreate()

    import spark.implicits._

    // --- Step 2: Define the 4 diseases to train models for ---
    // Each entry: (disease name, number of classes)
    // numClasses tells RandomForest how many categories to predict:
    //   2 = binary (yes/no), 3 = three levels (Low/Medium/High)
    val diseases = Seq(
      ("heart", 2),        // HeartDiseaseorAttack: 0 or 1
      ("diabetes", 2),     // Diabetes_binary: 0 or 1
      ("stroke", 2),       // stroke: 0 or 1
      ("lungcancer", 3)    // Level: Low(2), Medium(1), High(0) — 3 classes
    )

    // We'll collect evaluation results for all diseases
    var evalRows = Seq.empty[(String, Double, Double, Double, Double, Long, Long)]

    // --- Step 3: Train a model for each disease ---
    for ((disease, numClasses) <- diseases) {
      println(s"\n${"=" * 60}")
      println(s"  TRAINING MODEL: $disease")
      println(s"${"=" * 60}")

      // ----- 3a: Read feature-engineered data -----
      // This Parquet has two columns: "label" and "features"
      // "label" = what we want to predict (0.0, 1.0, or 2.0)
      // "features" = the scaled feature vector from Stage 3
      val df = spark.read.parquet(s"/medispark/features/$disease")
      val totalRows = df.count()
      println(s"  Loaded $totalRows rows")

      // ----- 3b: Split into training and test sets -----
      // 80% of data for training (the model learns from this)
      // 20% of data for testing (we evaluate performance on this)
      //
      // Why split? If we test on the same data we trained on, the model
      // could just memorize the answers. Testing on unseen data tells us
      // how well it will work on new patients.
      //
      // The seed (42) makes the split reproducible — same split every time
      val Array(trainData, testData) = df.randomSplit(Array(0.8, 0.2), seed = 42L)

      val trainCount = trainData.count()
      val testCount = testData.count()
      println(s"  Training set: $trainCount rows (80%)")
      println(s"  Test set:     $testCount rows (20%)")

      // ----- 3c: Configure and train RandomForestClassifier -----
      // How Random Forest works:
      //   1. It creates many decision trees (we use 100)
      //   2. Each tree is trained on a random subset of the data
      //   3. Each tree looks at a random subset of features at each split
      //   4. For prediction, all trees "vote" and the majority wins
      //
      // Parameters:
      //   numTrees = 100: number of decision trees to build
      //     More trees = better accuracy but slower training
      //     100 is a good balance for our dataset sizes
      //   maxDepth = 10: how deep each tree can grow
      //     Deeper = more complex patterns but risk of overfitting
      //     10 is moderate — captures important patterns without memorizing noise
      //   maxBins = 32: how many "buckets" to split continuous features into
      //     Higher = more precise splits but slower
      //     32 is the default and works well for most cases
      //   seed = 42: makes results reproducible

      println(s"  Training RandomForest (100 trees, maxDepth=10)...")

      val rf = new RandomForestClassifier()
        .setLabelCol("label")         // column containing the true answers
        .setFeaturesCol("features")   // column containing the feature vectors
        .setNumTrees(100)             // build 100 decision trees
        .setMaxDepth(10)              // each tree can be up to 10 levels deep
        .setMaxBins(32)               // 32 bins for continuous feature splits
        .setSeed(42L)                 // reproducible results

      // .fit() is where the actual training happens — this is the slow part
      // Spark distributes the work across all executor cores
      val model = rf.fit(trainData)

      println(s"  Training complete!")

      // ----- 3d: Make predictions on the test set -----
      // .transform() runs each test row through all 100 trees and returns:
      //   - "prediction": the model's predicted label (0.0, 1.0, etc.)
      //   - "rawPrediction": raw vote counts from each tree
      //   - "probability": probability for each class
      val predictions = model.transform(testData)

      // ----- 3e: Evaluate the model -----
      // We calculate 4 metrics:
      //
      // ACCURACY: % of predictions that are correct
      //   accuracy = correct_predictions / total_predictions
      //   Example: 950 correct out of 1000 = 95% accuracy
      //
      // PRECISION (weighted): of all patients predicted as "sick",
      //   what % actually are sick?
      //   High precision = few false alarms
      //
      // RECALL (weighted): of all patients who ARE sick,
      //   what % did we correctly identify?
      //   High recall = we don't miss sick patients
      //
      // F1-SCORE (weighted): harmonic mean of precision and recall
      //   Balances both — useful when classes are imbalanced
      //   F1 = 2 * (precision * recall) / (precision + recall)

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")

      // Calculate each metric by changing the metricName
      val accuracy  = evaluator.setMetricName("accuracy").evaluate(predictions)
      val precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
      val recall    = evaluator.setMetricName("weightedRecall").evaluate(predictions)
      val f1        = evaluator.setMetricName("f1").evaluate(predictions)

      // Print results
      println(s"\n  === EVALUATION RESULTS ===")
      println(f"  Accuracy:  ${accuracy * 100}%.2f%%")
      println(f"  Precision: ${precision * 100}%.2f%%")
      println(f"  Recall:    ${recall * 100}%.2f%%")
      println(f"  F1-Score:  ${f1 * 100}%.2f%%")

      // ----- 3f: Print feature importance -----
      // Random Forest can rank which features matter most for predictions.
      // Higher importance = that feature has more influence on the outcome.
      // This is useful for medical interpretation — e.g., "BMI is the #1
      // predictor of diabetes in our model"
      val importances = model.featureImportances.toArray
      println(s"\n  Top 5 feature importances (by index):")
      importances.zipWithIndex
        .sortBy(-_._1)       // sort by importance descending
        .take(5)             // take top 5
        .foreach { case (importance, index) =>
          println(f"    Feature $index: $importance%.4f")
        }

      // Collect evaluation data for the report
      evalRows = evalRows :+ (disease, accuracy, precision, recall, f1, trainCount, testCount)

      // ----- 3g: Save the trained model to HDFS -----
      // The model is saved as a directory containing metadata and tree data.
      // It can be loaded later with RandomForestClassificationModel.load(path)
      // for making predictions on new patients (Stage 5 streaming).
      val modelPath = s"/medispark/models/$disease"
      model.write.overwrite().save(modelPath)
      println(s"\n  Model saved to: $modelPath")
    }

    // =========================================================================
    // Step 4: Write evaluation report
    // =========================================================================
    println(s"\n${"=" * 60}")
    println("  WRITING EVALUATION REPORT")
    println(s"${"=" * 60}")

    val reportDF = evalRows.toDF(
      "disease",
      "accuracy",
      "precision",
      "recall",
      "f1_score",
      "train_rows",
      "test_rows"
    )

    // Save as CSV
    reportDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/medispark/output/evaluation_report")

    println("  Evaluation report saved to: /medispark/output/evaluation_report/")

    // Print summary table
    println(s"\n${"=" * 60}")
    println("  MODEL EVALUATION SUMMARY")
    println(s"${"=" * 60}")
    reportDF.show(false)

    // Clean up
    spark.stop()
    println("\n  ModelTrainer COMPLETE!")
  }
}
