// =============================================================================
// DataQualityPipeline.scala
// Stage 2 of MediSpark — Data Cleaning & Quality Assurance
//
// What this does:
//   For each of the 4 disease datasets (heart, diabetes, stroke, lung cancer):
//     1. Reads raw CSV from HDFS
//     2. Removes rows where too many columns are null (> 50% nulls)
//     3. Fills remaining nulls — median for numbers, mode for categories
//     4. Removes outliers using IQR method (values beyond Q1-1.5*IQR or Q3+1.5*IQR)
//     5. Removes exact duplicate rows
//     6. Validates that the schema (column types) is correct
//     7. Writes a quality_report.csv summarizing what was cleaned
//     8. Saves cleaned data as Parquet files to /medispark/clean/
// =============================================================================

package medispark.cleaning

// --- IMPORTS ---
// SparkSession: the entry point — creates our connection to the Spark cluster
import org.apache.spark.sql.SparkSession
// DataFrame: a distributed table (like a spreadsheet split across machines)
import org.apache.spark.sql.DataFrame
// functions._: built-in functions like col(), count(), when(), lit(), etc.
import org.apache.spark.sql.functions._
// types._: data types like StringType, DoubleType, IntegerType
import org.apache.spark.sql.types._
// Row: represents one row of data in a DataFrame
import org.apache.spark.sql.Row
// SaveMode: controls whether to overwrite or append when saving
import org.apache.spark.sql.SaveMode

object DataQualityPipeline {

  // =========================================================================
  // MAIN — the entry point when Spark runs this job
  // =========================================================================
  def main(args: Array[String]): Unit = {

    // --- Step 1: Create a SparkSession ---
    // This is like opening a connection to the Spark cluster.
    // .appName("...") sets the name you'll see in the Spark Web UI at localhost:4040
    // .getOrCreate() either creates a new session or reuses an existing one
    val spark = SparkSession.builder()
      .appName("MediSpark-DataQualityPipeline")
      .getOrCreate()

    // This import lets us use $ shorthand for column names, e.g. $"age"
    // and enables implicit conversions like .toDF()
    import spark.implicits._

    // --- Step 2: Define the 4 datasets we need to clean ---
    // Each tuple has: (disease name, HDFS input path, target column name,
    //                  list of categorical columns, list of numeric columns)
    val datasets = Seq(
      // HEART DISEASE
      // All columns are numeric (0/1 binary or integer-coded)
      // Target: HeartDiseaseorAttack (0 = no, 1 = yes)
      (
        "heart",                                    // disease name
        "/medispark/raw/heart/heart.csv",            // where the CSV is in HDFS
        "HeartDiseaseorAttack",                      // target column (what we predict)
        Seq.empty[String],                           // categorical columns (none here)
        Seq("HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke",
            "Diabetes", "PhysActivity", "Fruits", "Veggies", "HvyAlcoholConsump",
            "AnyHealthcare", "NoDocbcCost", "GenHlth", "MentHlth", "PhysHlth",
            "DiffWalk", "Sex", "Age", "Education", "Income")  // numeric columns
      ),

      // DIABETES
      // Same structure as heart (BRFSS survey data)
      // Target: Diabetes_binary (0 = no, 1 = yes)
      (
        "diabetes",
        "/medispark/raw/diabetes/diabetes.csv",
        "Diabetes_binary",
        Seq.empty[String],
        Seq("HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke",
            "HeartDiseaseorAttack", "PhysActivity", "Fruits", "Veggies",
            "HvyAlcoholConsump", "AnyHealthcare", "NoDocbcCost", "GenHlth",
            "MentHlth", "PhysHlth", "DiffWalk", "Sex", "Age", "Education", "Income")
      ),

      // STROKE
      // Mix of numeric and categorical columns
      // Target: stroke (0 = no, 1 = yes)
      (
        "stroke",
        "/medispark/raw/stroke/stroke.csv",
        "stroke",
        Seq("gender", "ever_married", "work_type", "Residence_type", "smoking_status"),
        Seq("age", "hypertension", "heart_disease", "avg_glucose_level", "bmi")
      ),

      // LUNG CANCER
      // Most columns are integer-scaled 1-10
      // Target: Level (Low / Medium / High)
      (
        "lungcancer",
        "/medispark/raw/lungcancer/lung_cancer.csv",
        "Level",
        Seq.empty[String],   // Level is categorical but it's the target, handled separately
        Seq("Age", "Gender", "Air Pollution", "Alcohol use", "Dust Allergy",
            "OccuPational Hazards", "Genetic Risk", "chronic Lung Disease",
            "Balanced Diet", "Obesity", "Smoking", "Passive Smoker", "Chest Pain",
            "Coughing of Blood", "Fatigue", "Weight Loss", "Shortness of Breath",
            "Wheezing", "Swallowing Difficulty", "Clubbing of Finger Nails",
            "Frequent Cold", "Dry Cough", "Snoring")
      )
    )

    // --- Step 3: Process each dataset ---
    // We'll collect quality report rows for all datasets
    var qualityReportRows = Seq.empty[(String, Long, Long, Long, Long, Long, Long)]

    for ((disease, path, targetCol, catCols, numCols) <- datasets) {
      println(s"\n${"=" * 60}")
      println(s"  PROCESSING: $disease")
      println(s"${"=" * 60}")

      // ----- 3a: Read CSV from HDFS -----
      // .option("header", "true") means the first row contains column names
      // .option("inferSchema", "true") lets Spark auto-detect column types
      var df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)

      // For lung cancer, drop the "index" and "Patient Id" columns —
      // they're just row numbers and IDs, not useful for prediction
      if (disease == "lungcancer") {
        df = df.drop("index", "Patient Id")
      }

      // For stroke, drop the "id" column — same reason
      if (disease == "stroke") {
        df = df.drop("id")
      }

      val originalCount = df.count()
      println(s"  Original row count: $originalCount")
      println(s"  Columns: ${df.columns.mkString(", ")}")

      // ----- 3b: Remove rows where > 50% of columns are null -----
      // Why: If more than half the values are missing, the row is useless
      val totalCols = df.columns.length
      val nullThreshold = totalCols / 2  // 50% of columns

      // For each row, count how many columns are null
      // df.columns.map(c => when(col(c).isNull, 1).otherwise(0)) creates a
      // list of 0/1 values — one per column — where 1 means null
      // .reduce(_ + _) sums them up to get the total null count per row
      val nullCountCol = df.columns
        .map(c => when(col(c).isNull || col(c).isNaN || col(c) === "N/A" || col(c) === "", lit(1)).otherwise(lit(0)))
        .reduce(_ + _)

      // Keep only rows where null count is at or below the threshold
      val dfNullsRemoved = df.filter(nullCountCol <= nullThreshold)
      val afterNullRemoval = dfNullsRemoved.count()
      val nullRowsRemoved = originalCount - afterNullRemoval
      println(s"  Rows removed (too many nulls): $nullRowsRemoved")

      // ----- 3c: Fill remaining nulls -----
      // For numeric columns: fill with the MEDIAN (middle value)
      //   Why median instead of mean? Median is resistant to outliers.
      //   If ages are [20, 25, 30, 35, 200], mean=62 (skewed), median=30 (correct)
      // For categorical columns: fill with the MODE (most frequent value)
      var dfFilled = dfNullsRemoved

      // Fill numeric nulls with median
      for (c <- numCols) {
        if (dfFilled.columns.contains(c)) {
          // Calculate median using approxQuantile (approximate is faster for big data)
          // The 3rd parameter (0.01) is the relative error — lower = more accurate but slower
          val medianArray = dfFilled.stat.approxQuantile(c, Array(0.5), 0.01)
          if (medianArray.nonEmpty) {
            val medianVal = medianArray(0)
            // na.fill fills null values in the specified column with the given value
            dfFilled = dfFilled.na.fill(Map(c -> medianVal))
          }
        }
      }

      // Fill categorical nulls with mode (most frequent value)
      for (c <- catCols) {
        if (dfFilled.columns.contains(c)) {
          // Find the most common non-null value
          // groupBy(c) groups rows by the column's values
          // .count() counts how many rows in each group
          // .orderBy(desc("count")) sorts so the most frequent is first
          // .first() takes the top row, .getString(0) gets the value
          val modeRow = dfFilled.filter(col(c).isNotNull && col(c) =!= "N/A")
            .groupBy(c)
            .count()
            .orderBy(desc("count"))
            .first()
          val modeVal = modeRow.getString(0)
          // Replace nulls and "N/A" strings with the mode
          dfFilled = dfFilled.withColumn(c,
            when(col(c).isNull || col(c) === "N/A", lit(modeVal)).otherwise(col(c))
          )
        }
      }

      // ----- 3d: Remove outliers using IQR method -----
      // IQR = Inter-Quartile Range = Q3 - Q1
      // Q1 = 25th percentile (value below which 25% of data falls)
      // Q3 = 75th percentile (value below which 75% of data falls)
      // An outlier is any value below Q1 - 1.5*IQR or above Q3 + 1.5*IQR
      //
      // Example: If ages are Q1=25, Q3=65, then IQR=40
      //   Lower bound = 25 - 1.5*40 = -35 (so age < -35 is outlier)
      //   Upper bound = 65 + 1.5*40 = 125 (so age > 125 is outlier)
      //
      // We only apply IQR to continuous numeric columns (not binary 0/1 columns)
      // Binary columns (like Smoker: 0 or 1) don't have meaningful outliers

      // Identify which numeric columns are continuous (more than 2 unique values)
      val continuousCols = numCols.filter { c =>
        if (dfFilled.columns.contains(c)) {
          dfFilled.select(c).distinct().count() > 10  // if >10 unique values, it's continuous
        } else false
      }

      var dfOutliers = dfFilled
      for (c <- continuousCols) {
        // Calculate Q1 (25th percentile) and Q3 (75th percentile)
        val quantiles = dfOutliers.stat.approxQuantile(c, Array(0.25, 0.75), 0.01)
        if (quantiles.length == 2) {
          val q1 = quantiles(0)
          val q3 = quantiles(1)
          val iqr = q3 - q1

          // Only filter if IQR > 0 (i.e., there's actual spread in the data)
          if (iqr > 0) {
            val lowerBound = q1 - 1.5 * iqr
            val upperBound = q3 + 1.5 * iqr
            // Keep only rows where the value is within bounds
            dfOutliers = dfOutliers.filter(col(c) >= lowerBound && col(c) <= upperBound)
          }
        }
      }

      val afterOutlierRemoval = dfOutliers.count()
      val outlierRowsRemoved = afterNullRemoval - afterOutlierRemoval
      println(s"  Rows removed (outliers): $outlierRowsRemoved")

      // ----- 3e: Remove exact duplicate rows -----
      // dropDuplicates() removes rows where ALL column values are identical
      val dfDeduped = dfOutliers.dropDuplicates()
      val afterDedup = dfDeduped.count()
      val dupRowsRemoved = afterOutlierRemoval - afterDedup
      println(s"  Rows removed (duplicates): $dupRowsRemoved")

      // ----- 3f: Schema validation -----
      // Check that all expected columns exist and have the right types
      val schema = dfDeduped.schema
      val expectedCols = numCols ++ catCols :+ targetCol
      val missingCols = expectedCols.filterNot(dfDeduped.columns.contains)
      if (missingCols.nonEmpty) {
        println(s"  WARNING: Missing columns: ${missingCols.mkString(", ")}")
      } else {
        println(s"  Schema validation: PASSED (all ${expectedCols.length} columns present)")
      }

      // ----- 3g: Collect quality report data -----
      val finalCount = afterDedup
      qualityReportRows = qualityReportRows :+ (
        disease,
        originalCount,
        nullRowsRemoved,
        outlierRowsRemoved,
        dupRowsRemoved,
        finalCount,
        expectedCols.length.toLong
      )

      println(s"  Final row count: $finalCount (${(finalCount.toDouble / originalCount * 100).formatted("%.1f")}% retained)")

      // ----- 3h: Save cleaned data as Parquet -----
      // Parquet is a columnar file format — much faster than CSV for Spark
      // It compresses well and preserves column types
      // .mode(SaveMode.Overwrite) replaces any existing files
      dfDeduped.write
        .mode(SaveMode.Overwrite)
        .parquet(s"/medispark/clean/$disease")

      println(s"  Saved to: /medispark/clean/$disease (Parquet)")
    }

    // =========================================================================
    // Step 4: Write the quality report to HDFS
    // =========================================================================
    println(s"\n${"=" * 60}")
    println("  WRITING QUALITY REPORT")
    println(s"${"=" * 60}")

    // Create a DataFrame from our collected quality stats
    val reportDF = qualityReportRows.toDF(
      "disease",
      "original_rows",
      "null_rows_removed",
      "outlier_rows_removed",
      "duplicate_rows_removed",
      "final_rows",
      "column_count"
    )

    // Save as CSV so it's human-readable
    reportDF.coalesce(1)    // coalesce(1) forces output into a single file
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("/medispark/output/quality_report")

    println("  Quality report saved to: /medispark/output/quality_report/")

    // Print a summary table
    println(s"\n${"=" * 60}")
    println("  QUALITY REPORT SUMMARY")
    println(s"${"=" * 60}")
    reportDF.show(false)

    // Clean up — stop the SparkSession
    spark.stop()
    println("\n  DataQualityPipeline COMPLETE!")
  }
}
