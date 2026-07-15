// =============================================================================
// FeatureEngineering.scala
// Stage 3 of MediSpark — Feature Engineering
//
// What this does:
//   For each of the 4 disease datasets (heart, diabetes, stroke, lung cancer):
//     1. Reads cleaned Parquet data from /medispark/clean/{disease}
//     2. Converts categorical string columns into numeric indices (StringIndexer)
//     3. Assembles ALL feature columns into a single "features" vector
//     4. Scales (normalizes) the features so all values are on a similar range
//     5. Saves the ML-ready data to /medispark/features/{disease}
//
// Why feature engineering?
//   ML algorithms need numbers, not strings. "Male"/"Female" must become 0.0/1.0.
//   Also, columns like "age" (0-100) and "bmi" (15-50) have different scales.
//   StandardScaler normalizes them so no single feature dominates the model.
// =============================================================================

package medispark.features

// --- IMPORTS ---
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

// Spark ML imports for the transformation pipeline
// StringIndexer: converts string columns ("Male", "Female") into numeric indices (0.0, 1.0)
import org.apache.spark.ml.feature.StringIndexer
// VectorAssembler: combines multiple columns into one "features" vector column
import org.apache.spark.ml.feature.VectorAssembler
// StandardScaler: normalizes feature values (mean=0, stddev=1)
import org.apache.spark.ml.feature.StandardScaler
// Pipeline: chains multiple transformations together so they run in sequence
import org.apache.spark.ml.Pipeline

object FeatureEngineering {

  // =========================================================================
  // MAIN — entry point when Spark runs this job
  // =========================================================================
  def main(args: Array[String]): Unit = {

    // --- Step 1: Create SparkSession ---
    val spark = SparkSession.builder()
      .appName("MediSpark-FeatureEngineering")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
      .getOrCreate()

    import spark.implicits._

    // --- Step 2: Define dataset configurations ---
    // Each entry: (disease name, target column, categorical columns, numeric columns)
    // These must match exactly what Stage 2 produced in the cleaned Parquet files.
    val datasets = Seq(
      // HEART DISEASE
      // All columns are already numeric (0/1 binary or integer-coded from BRFSS survey)
      // No categorical columns to index
      (
        "heart",
        "HeartDiseaseorAttack",           // target column
        Seq.empty[String],                // no categorical columns
        Seq("HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke",
            "Diabetes", "PhysActivity", "Fruits", "Veggies", "HvyAlcoholConsump",
            "AnyHealthcare", "NoDocbcCost", "GenHlth", "MentHlth", "PhysHlth",
            "DiffWalk", "Sex", "Age", "Education", "Income")
      ),

      // DIABETES
      // Same structure as heart — all numeric
      (
        "diabetes",
        "Diabetes_binary",
        Seq.empty[String],
        Seq("HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke",
            "HeartDiseaseorAttack", "PhysActivity", "Fruits", "Veggies",
            "HvyAlcoholConsump", "AnyHealthcare", "NoDocbcCost", "GenHlth",
            "MentHlth", "PhysHlth", "DiffWalk", "Sex", "Age", "Education", "Income")
      ),

      // STROKE
      // Has 5 categorical columns that need StringIndexer
      (
        "stroke",
        "stroke",
        Seq("gender", "ever_married", "work_type", "Residence_type", "smoking_status"),
        Seq("age", "hypertension", "heart_disease", "avg_glucose_level", "bmi")
      ),

      // LUNG CANCER
      // All numeric feature columns, but the TARGET "Level" is categorical
      // ("Low", "Medium", "High") — it needs StringIndexer too
      (
        "lungcancer",
        "Level",
        Seq.empty[String],       // no categorical feature columns
        Seq("Age", "Gender", "Air Pollution", "Alcohol use", "Dust Allergy",
            "OccuPational Hazards", "Genetic Risk", "chronic Lung Disease",
            "Balanced Diet", "Obesity", "Smoking", "Passive Smoker", "Chest Pain",
            "Coughing of Blood", "Fatigue", "Weight Loss", "Shortness of Breath",
            "Wheezing", "Swallowing Difficulty", "Clubbing of Finger Nails",
            "Frequent Cold", "Dry Cough", "Snoring")
      )
    )

    // --- Step 3: Process each dataset ---
    for ((disease, targetCol, catCols, numCols) <- datasets) {
      println(s"\n${"=" * 60}")
      println(s"  FEATURE ENGINEERING: $disease")
      println(s"${"=" * 60}")

      // ----- 3a: Read cleaned Parquet from Stage 2 -----
      var df = spark.read.parquet(s"/medispark/clean/$disease")
      println(s"  Loaded ${df.count()} rows, ${df.columns.length} columns")

      // ----- 3b: Cast numeric columns to DoubleType -----
      // Spark ML requires numeric features to be Double, not Int or String.
      // This ensures consistency even if Parquet stored them as integers.
      for (c <- numCols) {
        if (df.columns.contains(c)) {
          df = df.withColumn(c, col(c).cast(DoubleType))
        }
      }

      // ----- 3c: Handle the target column -----
      // For binary targets (heart, diabetes, stroke): cast to Double (0.0 or 1.0)
      // For multi-class target (lung cancer "Level"): use StringIndexer
      //
      // The final target column will always be called "label" — this is the
      // standard name Spark ML expects for the column being predicted.

      val targetIsString = df.schema(targetCol).dataType == StringType

      if (targetIsString) {
        // Lung cancer: "Level" is "Low"/"Medium"/"High" — needs indexing
        // StringIndexer converts: "High"→0.0, "Medium"→1.0, "Low"→2.0
        // (most frequent value gets index 0 by default)
        println(s"  Target '$targetCol' is categorical — indexing to 'label'")
        val targetIndexer = new StringIndexer()
          .setInputCol(targetCol)       // read from this column
          .setOutputCol("label")        // write the numeric index here
          .setHandleInvalid("keep")     // if unknown values appear, give them an index
        df = targetIndexer.fit(df).transform(df)
      } else {
        // Heart/diabetes/stroke: target is already 0/1 numeric
        // Just rename it to "label" for consistency
        println(s"  Target '$targetCol' is numeric — renaming to 'label'")
        df = df.withColumn("label", col(targetCol).cast(DoubleType))
      }

      // ----- 3d: Index categorical feature columns -----
      // StringIndexer creates a new column for each categorical column:
      //   "gender" → "gender_idx" (Male→0.0, Female→1.0, Other→2.0)
      //   "work_type" → "work_type_idx" (Private→0.0, Self-employed→1.0, ...)
      //
      // We keep the indexed columns and drop the original string columns later.

      val indexedCatCols = catCols.map(c => s"${c}_idx")  // new column names

      val indexers = catCols.map { c =>
        new StringIndexer()
          .setInputCol(c)                // original string column
          .setOutputCol(s"${c}_idx")     // new numeric column
          .setHandleInvalid("keep")      // handle any unexpected values
      }

      // Apply all indexers
      if (indexers.nonEmpty) {
        println(s"  Indexing ${catCols.length} categorical columns: ${catCols.mkString(", ")}")
        val indexerPipeline = new Pipeline().setStages(indexers.toArray)
        df = indexerPipeline.fit(df).transform(df)
      }

      // ----- 3e: Assemble all feature columns into one vector -----
      // VectorAssembler takes multiple columns and merges them into a single
      // "features" column. This is what Spark ML models expect as input.
      //
      // Example: if a row has age=45.0, bmi=28.5, gender_idx=0.0
      // The "features" column becomes: [45.0, 28.5, 0.0]
      //
      // We use: all numeric columns + all indexed categorical columns

      val featureCols = numCols ++ indexedCatCols
      println(s"  Assembling ${featureCols.length} features into vector")

      val assembler = new VectorAssembler()
        .setInputCols(featureCols.toArray)   // columns to combine
        .setOutputCol("features_raw")        // output vector column name
        .setHandleInvalid("skip")            // skip rows with null/NaN features

      df = assembler.transform(df)

      // ----- 3f: Scale features with StandardScaler -----
      // StandardScaler transforms each feature to have mean=0 and stddev=1.
      //
      // Why? Without scaling:
      //   - "age" ranges from 0 to 100
      //   - "bmi" ranges from 15 to 50
      //   - "gender_idx" is just 0 or 1
      // The model would think "age" is more important just because its numbers
      // are bigger. Scaling puts them all on the same footing.
      //
      // Formula: scaled_value = (value - mean) / standard_deviation

      val scaler = new StandardScaler()
        .setInputCol("features_raw")     // the unscaled vector
        .setOutputCol("features")        // the final scaled vector
        .setWithMean(true)               // subtract the mean (center at 0)
        .setWithStd(true)                // divide by standard deviation

      val scalerModel = scaler.fit(df)
      df = scalerModel.transform(df)

      // ----- 3g: Select only the columns we need for ML -----
      // We keep: "label" (what to predict) + "features" (scaled feature vector)
      // Everything else is dropped to save space and simplify the ML stage.

      val finalDF = df.select("label", "features")
      val finalCount = finalDF.count()
      println(s"  Final row count: $finalCount")

      // ----- 3h: Save as Parquet to /medispark/features/{disease} -----
      finalDF.write
        .mode(SaveMode.Overwrite)
        .parquet(s"/medispark/features/$disease")

      println(s"  Saved to: /medispark/features/$disease")

      // Print a sample to verify
      println(s"\n  Sample (first 3 rows):")
      finalDF.show(3, truncate = false)
    }

    // Clean up
    spark.stop()
    println("\n  FeatureEngineering COMPLETE!")
  }
}
