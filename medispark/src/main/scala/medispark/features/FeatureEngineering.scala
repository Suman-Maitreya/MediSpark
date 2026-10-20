// ==============================================================================
// MediSpark — Stage 3: Feature Engineering
// StringIndexer for categoricals, VectorAssembler, StandardScaler
// ==============================================================================
package medispark.features

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, StandardScaler}
import org.apache.spark.ml.Pipeline

object FeatureEngineering {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MediSpark-Stage3-FeatureEngineering")
      .getOrCreate()

    import spark.implicits._

    println("=" * 70)
    println("  MEDISPARK — STAGE 3: FEATURE ENGINEERING")
    println("=" * 70)

    val inPath  = "hdfs://namenode:9000/medispark/cleaned"
    val outPath = "hdfs://namenode:9000/medispark/features"

    // ── 1. Heart Disease ────────────────────────────────────────
    println("\n── Features: heart ──")
    val heart = spark.read.parquet(s"$inPath/heart")
    val heartFeatureCols = heart.columns.filter(c => c != "label" && c != "_split")
    val heartEngineered = assembleAndScale(heart, heartFeatureCols, "label")
    heartEngineered.write.mode("overwrite").parquet(s"$outPath/heart")
    println(s"  Feature columns: ${heartFeatureCols.length}")
    println(s"  Output rows: ${heartEngineered.count()}")

    // ── 2. Diabetes ─────────────────────────────────────────────
    println("\n── Features: diabetes ──")
    val diabetes = spark.read.parquet(s"$inPath/diabetes")
    val diabetesFeatureCols = diabetes.columns.filter(c => c != "label" && c != "_split")
    val diabetesEngineered = assembleAndScale(diabetes, diabetesFeatureCols, "label")
    diabetesEngineered.write.mode("overwrite").parquet(s"$outPath/diabetes")
    println(s"  Feature columns: ${diabetesFeatureCols.length}")
    println(s"  Output rows: ${diabetesEngineered.count()}")

    // ── 3. Stroke (has categorical columns) ─────────────────────
    println("\n── Features: stroke ──")
    var strokeDf = spark.read.parquet(s"$inPath/stroke")

    // StringIndex categorical columns
    val catCols = Array("gender", "ever_married", "work_type",
                        "Residence_type", "smoking_status")
    val indexedCols = catCols.map(_ + "_idx")

    val indexers = catCols.zip(indexedCols).map { case (in, out) =>
      new StringIndexer()
        .setInputCol(in)
        .setOutputCol(out)
        .setHandleInvalid("keep")
    }

    // Apply indexers sequentially
    var strokeIndexed = strokeDf
    indexers.foreach { indexer =>
      strokeIndexed = indexer.fit(strokeIndexed).transform(strokeIndexed)
    }

    // Drop original categorical columns
    strokeIndexed = catCols.foldLeft(strokeIndexed)((df, col) => df.drop(col))

    // Numeric feature columns (excluding label)
    val strokeNumericCols = Array("age", "hypertension", "heart_disease",
                                  "avg_glucose_level", "bmi")
    val strokeFeatureCols = strokeNumericCols ++ indexedCols

    val strokeEngineered = assembleAndScale(strokeIndexed, strokeFeatureCols, "label")
    strokeEngineered.write.mode("overwrite").parquet(s"$outPath/stroke")
    println(s"  Categorical columns indexed: ${catCols.length}")
    println(s"  Total feature columns: ${strokeFeatureCols.length}")
    println(s"  Output rows: ${strokeEngineered.count()}")

    // ── 4. Lung Cancer (multi-class target) ─────────────────────
    println("\n── Features: lung_cancer ──")
    var lung = spark.read.parquet(s"$inPath/lung_cancer")

    // Index the multi-class target: Low=0, Medium=1, High=2
    val targetIndexer = new StringIndexer()
      .setInputCol("label_str")
      .setOutputCol("label")
      .setHandleInvalid("keep")
    lung = targetIndexer.fit(lung).transform(lung)
    lung = lung.drop("label_str")

    // Print target mapping
    println("  Target mapping:")
    lung.groupBy("label").count().orderBy("label").show()

    val lungFeatureCols = lung.columns.filter(c => c != "label" && c != "_split")
    val lungEngineered = assembleAndScale(lung, lungFeatureCols, "label")
    lungEngineered.write.mode("overwrite").parquet(s"$outPath/lung_cancer")
    println(s"  Feature columns: ${lungFeatureCols.length}")
    println(s"  Output rows: ${lungEngineered.count()}")

    println("\n" + "=" * 70)
    println("  STAGE 3 COMPLETE — All features engineered")
    println("=" * 70)

    spark.stop()
  }

  /** Assemble feature columns into a vector and apply StandardScaler */
  def assembleAndScale(df: org.apache.spark.sql.DataFrame,
                       featureCols: Array[String],
                       labelCol: String): org.apache.spark.sql.DataFrame = {

    // Cast all feature columns to Double
    var result = df
    featureCols.foreach { col =>
      result = result.withColumn(col, result(col).cast(DoubleType))
    }

    // Assemble features into a single vector
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features_raw")
      .setHandleInvalid("skip")

    result = assembler.transform(result)

    // Scale features
    val scaler = new StandardScaler()
      .setInputCol("features_raw")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    val scalerModel = scaler.fit(result)
    result = scalerModel.transform(result)

    // Keep label + features + the leakage-safe train/test split tag
    result.select(labelCol, "features", "_split")
  }
}
