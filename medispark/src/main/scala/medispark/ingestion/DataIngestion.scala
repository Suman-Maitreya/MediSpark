// ==============================================================================
// MediSpark — Stage 1: Data Ingestion
// Reads raw data from HDFS in multiple formats (CSV, JSON, Parquet),
// validates schemas, and writes unified Parquet for downstream stages.
// ==============================================================================
package medispark.ingestion

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

object DataIngestion {

  // ── Expected schemas for validation ───────────────────────────

  // Heart Disease UCI — 14 columns (source format: CSV)
  val heartSchema = StructType(Seq(
    StructField("age", DoubleType, nullable = true),
    StructField("sex", DoubleType, nullable = true),
    StructField("cp", DoubleType, nullable = true),
    StructField("trestbps", DoubleType, nullable = true),
    StructField("chol", DoubleType, nullable = true),
    StructField("fbs", DoubleType, nullable = true),
    StructField("restecg", DoubleType, nullable = true),
    StructField("thalach", DoubleType, nullable = true),
    StructField("exang", DoubleType, nullable = true),
    StructField("oldpeak", DoubleType, nullable = true),
    StructField("slope", DoubleType, nullable = true),
    StructField("ca", DoubleType, nullable = true),
    StructField("thal", DoubleType, nullable = true),
    StructField("target", DoubleType, nullable = true),
    StructField("_split", StringType, nullable = true)
  ))

  // Pima Indians Diabetes — 9 columns (source format: JSON)
  val diabetesSchema = StructType(Seq(
    StructField("Pregnancies", DoubleType, nullable = true),
    StructField("Glucose", DoubleType, nullable = true),
    StructField("BloodPressure", DoubleType, nullable = true),
    StructField("SkinThickness", DoubleType, nullable = true),
    StructField("Insulin", DoubleType, nullable = true),
    StructField("BMI", DoubleType, nullable = true),
    StructField("DiabetesPedigreeFunction", DoubleType, nullable = true),
    StructField("Age", DoubleType, nullable = true),
    StructField("Outcome", DoubleType, nullable = true),
    StructField("_split", StringType, nullable = true)
  ))

  // Stroke Prediction — 12 columns (source format: CSV)
  val strokeSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("age", DoubleType, nullable = true),
    StructField("hypertension", IntegerType, nullable = true),
    StructField("heart_disease", IntegerType, nullable = true),
    StructField("ever_married", StringType, nullable = true),
    StructField("work_type", StringType, nullable = true),
    StructField("Residence_type", StringType, nullable = true),
    StructField("avg_glucose_level", DoubleType, nullable = true),
    StructField("bmi", DoubleType, nullable = true),
    StructField("smoking_status", StringType, nullable = true),
    StructField("stroke", IntegerType, nullable = true),
    StructField("_split", StringType, nullable = true)
  ))

  // Lung Cancer — 26 columns (source format: Parquet)
  val lungCancerSchema = StructType(Seq(
    StructField("index", IntegerType, nullable = true),
    StructField("Patient_Id", StringType, nullable = true),
    StructField("Age", IntegerType, nullable = true),
    StructField("Gender", IntegerType, nullable = true),
    StructField("Air_Pollution", IntegerType, nullable = true),
    StructField("Alcohol_use", IntegerType, nullable = true),
    StructField("Dust_Allergy", IntegerType, nullable = true),
    StructField("OccuPational_Hazards", IntegerType, nullable = true),
    StructField("Genetic_Risk", IntegerType, nullable = true),
    StructField("chronic_Lung_Disease", IntegerType, nullable = true),
    StructField("Balanced_Diet", IntegerType, nullable = true),
    StructField("Obesity", IntegerType, nullable = true),
    StructField("Smoking", IntegerType, nullable = true),
    StructField("Passive_Smoker", IntegerType, nullable = true),
    StructField("Chest_Pain", IntegerType, nullable = true),
    StructField("Coughing_of_Blood", IntegerType, nullable = true),
    StructField("Fatigue", IntegerType, nullable = true),
    StructField("Weight_Loss", IntegerType, nullable = true),
    StructField("Shortness_of_Breath", IntegerType, nullable = true),
    StructField("Wheezing", IntegerType, nullable = true),
    StructField("Swallowing_Difficulty", IntegerType, nullable = true),
    StructField("Clubbing_of_Finger_Nails", IntegerType, nullable = true),
    StructField("Frequent_Cold", IntegerType, nullable = true),
    StructField("Dry_Cough", IntegerType, nullable = true),
    StructField("Snoring", IntegerType, nullable = true),
    StructField("Level", StringType, nullable = true),
    StructField("_split", StringType, nullable = true)
  ))

  // ── Format-specific readers ──────────────────────────────────

  /** Read a CSV file with header and explicit schema */
  def readCsv(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    println(s"  Format: CSV")
    spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(schema)
      .csv(path)
  }

  /** Read a JSON Lines file with explicit schema */
  def readJson(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    println(s"  Format: JSON")
    spark.read
      .schema(schema)
      .json(path)
  }

  /** Read a Parquet file (schema is embedded in the file itself) */
  def readParquet(spark: SparkSession, path: String): DataFrame = {
    println(s"  Format: Parquet")
    spark.read.parquet(path)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MediSpark-Stage1-Ingestion")
      .getOrCreate()

    println("=" * 70)
    println("  MEDISPARK — STAGE 1: DATA INGESTION")
    println("  Multi-format: CSV | JSON | Parquet")
    println("=" * 70)

    val basePath = "hdfs://namenode:9000/medispark/raw"
    val outPath  = "hdfs://namenode:9000/medispark/ingested"

    // ── Dataset definitions with format info ────────────────────
    // Each tuple: (name, format, path, schema_option)
    val datasets: Seq[(String, String)] = Seq(
      ("heart",       "csv"),
      ("diabetes",    "json"),
      ("stroke",      "csv"),
      ("lung_cancer", "parquet")
    )

    datasets.foreach { case (name, format) =>
      println(s"\n── Ingesting: $name ──")

      val df: DataFrame = (name, format) match {
        case ("heart", "csv") =>
          readCsv(spark, s"$basePath/heart_original.csv", heartSchema)

        case ("diabetes", "json") =>
          readJson(spark, s"$basePath/diabetes_original.json", diabetesSchema)

        case ("stroke", "csv") =>
          readCsv(spark, s"$basePath/stroke_original.csv", strokeSchema)

        case ("lung_cancer", "parquet") =>
          readParquet(spark, s"$basePath/lung_cancer_original.parquet")

        case _ =>
          throw new IllegalArgumentException(s"Unknown dataset/format: $name/$format")
      }

      val rowCount = df.count()
      val colCount = df.columns.length
      val nullCounts = df.columns.map(c => df.filter(df(c).isNull || df(c).isNaN).count())
      val totalNulls = nullCounts.sum

      println(s"  Rows:    $rowCount")
      println(s"  Columns: $colCount")
      println(s"  Nulls:   $totalNulls")
      println(s"  Schema:")
      df.printSchema()

      // Write as Parquet for efficient downstream processing
      // All formats converge to Parquet here — unified for Stages 2-6
      df.write.mode("overwrite").parquet(s"$outPath/$name")
      println(s"  Written to: $outPath/$name (Parquet)")
    }

    println("\n" + "=" * 70)
    println("  STAGE 1 COMPLETE — All datasets ingested to HDFS as Parquet")
    println("  Input formats: CSV (heart, stroke) | JSON (diabetes) | Parquet (lung_cancer)")
    println("=" * 70)

    spark.stop()
  }
}
