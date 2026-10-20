// ==============================================================================
// MediSpark — Stage 2: Data Cleaning & Quality
// Handles nulls, drops unnecessary columns, standardizes column names,
// casts types, removes outliers
// ==============================================================================
package medispark.cleaning

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataCleaning {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MediSpark-Stage2-Cleaning")
      .getOrCreate()

    import spark.implicits._

    println("=" * 70)
    println("  MEDISPARK — STAGE 2: DATA CLEANING & QUALITY")
    println("=" * 70)

    val inPath  = "hdfs://namenode:9000/medispark/ingested"
    val outPath = "hdfs://namenode:9000/medispark/cleaned"

    // ── 1. Heart Disease (UCI — 14 columns) ─────────────────────
    println("\n── Cleaning: heart ──")
    var heart = spark.read.parquet(s"$inPath/heart")
    val heartBefore = heart.count()

    // All columns are numeric — drop nulls
    heart = heart.na.drop()

    // Remove cholesterol outliers (chol < 50 or > 600)
    heart = heart.filter($"chol" >= 50.0 && $"chol" <= 600.0)
    // Remove resting blood pressure outliers
    heart = heart.filter($"trestbps" >= 60.0 && $"trestbps" <= 250.0)

    // Rename target for consistency
    heart = heart.withColumnRenamed("target", "label")

    printQuality("heart", heartBefore, heart)
    heart.write.mode("overwrite").parquet(s"$outPath/heart")

    // ── 2. Diabetes (Pima — 9 columns) ──────────────────────────
    println("\n── Cleaning: diabetes ──")
    var diabetes = spark.read.parquet(s"$inPath/diabetes")
    val diabetesBefore = diabetes.count()

    // Drop nulls
    diabetes = diabetes.na.drop()

    // Replace zeros with NaN for physiologically impossible zero values
    // then drop those rows (Glucose=0, BloodPressure=0, BMI=0 are invalid)
    diabetes = diabetes.filter($"Glucose" > 0 && $"BloodPressure" > 0 && $"BMI" > 0)

    // BMI outlier removal
    diabetes = diabetes.filter($"BMI" >= 10.0 && $"BMI" <= 70.0)

    // Rename target for consistency
    diabetes = diabetes.withColumnRenamed("Outcome", "label")

    printQuality("diabetes", diabetesBefore, diabetes)
    diabetes.write.mode("overwrite").parquet(s"$outPath/diabetes")

    // ── 3. Stroke ───────────────────────────────────────────────
    println("\n── Cleaning: stroke ──")
    var strokeDf = spark.read.parquet(s"$inPath/stroke")
    val strokeBefore = strokeDf.count()

    // Drop the id column (not a feature)
    strokeDf = strokeDf.drop("id")

    // Drop rows with "Other" gender (very few, avoids noise)
    strokeDf = strokeDf.filter($"gender" =!= "Other")

    // Fill null BMI with median
    val bmiMedian = strokeDf.filter($"bmi".isNotNull && !$"bmi".isNaN)
      .stat.approxQuantile("bmi", Array(0.5), 0.01)(0)
    println(s"  BMI median (for null fill): $bmiMedian")
    strokeDf = strokeDf.na.fill(bmiMedian, Seq("bmi"))

    // Fill null smoking_status with "Unknown"
    strokeDf = strokeDf.na.fill("Unknown", Seq("smoking_status"))

    // Drop remaining nulls
    strokeDf = strokeDf.na.drop()

    // Rename target
    strokeDf = strokeDf.withColumnRenamed("stroke", "label")
    strokeDf = strokeDf.withColumn("label", $"label".cast(DoubleType))

    printQuality("stroke", strokeBefore, strokeDf)
    strokeDf.write.mode("overwrite").parquet(s"$outPath/stroke")

    // ── 4. Lung Cancer ──────────────────────────────────────────
    println("\n── Cleaning: lung_cancer ──")
    var lung = spark.read.parquet(s"$inPath/lung_cancer")
    val lungBefore = lung.count()

    // Drop index and Patient_Id (not features)
    lung = lung.drop("index", "Patient_Id")

    // Drop nulls
    lung = lung.na.drop()

    // Level is the target — keep as string (will be indexed in Stage 3)
    lung = lung.withColumnRenamed("Level", "label_str")

    printQuality("lung_cancer", lungBefore, lung)
    lung.write.mode("overwrite").parquet(s"$outPath/lung_cancer")

    println("\n" + "=" * 70)
    println("  STAGE 2 COMPLETE — All datasets cleaned")
    println("=" * 70)

    spark.stop()
  }

  def printQuality(name: String, before: Long, df: DataFrame): Unit = {
    val after = df.count()
    val dropped = before - after
    val nulls = df.columns.map(c => df.filter(df(c).isNull || df(c).isNaN).count()).sum
    println(s"  Before:  $before rows")
    println(s"  After:   $after rows")
    println(s"  Dropped: $dropped rows (${if(before>0) f"${dropped*100.0/before}%.2f" else "0"}%)")
    println(s"  Remaining nulls: $nulls")
    println(s"  Columns: ${df.columns.mkString(", ")}")
  }
}
