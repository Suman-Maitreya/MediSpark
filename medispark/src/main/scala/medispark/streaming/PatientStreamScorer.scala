// =============================================================================
// PatientStreamScorer.scala
// Stage 5 of MediSpark — Real-Time Streaming Risk Scorer
//
// What this does:
//   1. Loads all 4 trained RandomForest models from HDFS
//   2. Watches an HDFS directory for new patient JSON files
//   3. When a new file arrives (every 5 seconds from the Python generator):
//      a. Reads the patient data
//      b. Applies feature engineering (same as Stage 3) on-the-fly
//      c. Scores the patient against all 4 disease models
//      d. Converts predictions to risk levels (High/Medium/Low)
//      e. Outputs the multi-disease risk profile to the console
//
// This demonstrates the VELOCITY dimension of Big Data — processing
// streaming data in near real-time as it arrives.
// =============================================================================

package medispark.streaming

// --- IMPORTS ---
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger

// Spark ML imports
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler, StandardScalerModel}
import org.apache.spark.ml.linalg.Vector

object PatientStreamScorer {

  // =========================================================================
  // MAIN — entry point
  // =========================================================================
  def main(args: Array[String]): Unit = {

    // --- Step 1: Create SparkSession ---
    val spark = SparkSession.builder()
      .appName("MediSpark-PatientStreamScorer")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
      .getOrCreate()

    import spark.implicits._

    println(s"\n${"=" * 60}")
    println("  MediSpark — Real-Time Patient Risk Scorer")
    println(s"${"=" * 60}")

    // --- Step 2: Load all 4 trained models from HDFS ---
    // These were saved by ModelTrainer (Stage 4)
    println("\n  Loading trained models from HDFS...")

    val heartModel = RandomForestClassificationModel.load("/medispark/models/heart")
    println("    Heart disease model loaded")

    val diabetesModel = RandomForestClassificationModel.load("/medispark/models/diabetes")
    println("    Diabetes model loaded")

    val strokeModel = RandomForestClassificationModel.load("/medispark/models/stroke")
    println("    Stroke model loaded")

    val lungcancerModel = RandomForestClassificationModel.load("/medispark/models/lungcancer")
    println("    Lung cancer model loaded")

    println("  All 4 models loaded successfully!")

    // --- Step 3: Fit scalers on the training data ---
    // We need the same StandardScaler that was used in Stage 3.
    // We'll fit new scalers on the cleaned data (same result as Stage 3).
    println("\n  Fitting scalers on training data...")

    // Heart scaler
    val heartCols = Seq("HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke",
      "Diabetes", "PhysActivity", "Fruits", "Veggies", "HvyAlcoholConsump",
      "AnyHealthcare", "NoDocbcCost", "GenHlth", "MentHlth", "PhysHlth",
      "DiffWalk", "Sex", "Age", "Education", "Income")

    val heartClean = spark.read.parquet("/medispark/clean/heart")
    val heartAssembler = new VectorAssembler()
      .setInputCols(heartCols.toArray).setOutputCol("features_raw").setHandleInvalid("skip")
    val heartAssembled = heartAssembler.transform(
      heartCols.foldLeft(heartClean)((df, c) => df.withColumn(c, col(c).cast(DoubleType)))
    )
    val heartScaler = new StandardScaler()
      .setInputCol("features_raw").setOutputCol("features").setWithMean(true).setWithStd(true)
    val heartScalerModel = heartScaler.fit(heartAssembled)
    println("    Heart scaler fitted")

    // Diabetes scaler (same columns as heart but different target)
    val diabetesCols = Seq("HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke",
      "HeartDiseaseorAttack", "PhysActivity", "Fruits", "Veggies",
      "HvyAlcoholConsump", "AnyHealthcare", "NoDocbcCost", "GenHlth",
      "MentHlth", "PhysHlth", "DiffWalk", "Sex", "Age", "Education", "Income")

    val diabetesClean = spark.read.parquet("/medispark/clean/diabetes")
    val diabetesAssembler = new VectorAssembler()
      .setInputCols(diabetesCols.toArray).setOutputCol("features_raw").setHandleInvalid("skip")
    val diabetesAssembled = diabetesAssembler.transform(
      diabetesCols.foldLeft(diabetesClean)((df, c) => df.withColumn(c, col(c).cast(DoubleType)))
    )
    val diabetesScaler = new StandardScaler()
      .setInputCol("features_raw").setOutputCol("features").setWithMean(true).setWithStd(true)
    val diabetesScalerModel = diabetesScaler.fit(diabetesAssembled)
    println("    Diabetes scaler fitted")

    // Stroke scaler
    val strokeNumCols = Seq("age", "hypertension", "heart_disease", "avg_glucose_level", "bmi")
    val strokeCatCols = Seq("gender", "ever_married", "work_type", "Residence_type", "smoking_status")

    // We need the category mappings from training data for stroke
    val strokeClean = spark.read.parquet("/medispark/clean/stroke")

    // Build stroke indexer mappings (we'll apply them to streaming data)
    import org.apache.spark.ml.feature.StringIndexer
    import org.apache.spark.ml.Pipeline

    val strokeIndexers = strokeCatCols.map { c =>
      new StringIndexer().setInputCol(c).setOutputCol(s"${c}_idx").setHandleInvalid("keep")
    }
    val strokeIndexerPipeline = new Pipeline().setStages(strokeIndexers.toArray)
    val strokeIndexerModel = strokeIndexerPipeline.fit(strokeClean)

    val strokeFeatureCols = strokeNumCols ++ strokeCatCols.map(c => s"${c}_idx")
    val strokeAssembler = new VectorAssembler()
      .setInputCols(strokeFeatureCols.toArray).setOutputCol("features_raw").setHandleInvalid("skip")

    val strokeIndexed = strokeIndexerModel.transform(
      strokeNumCols.foldLeft(strokeClean)((df, c) => df.withColumn(c, col(c).cast(DoubleType)))
    )
    val strokeAssembled = strokeAssembler.transform(strokeIndexed)
    val strokeScaler = new StandardScaler()
      .setInputCol("features_raw").setOutputCol("features").setWithMean(true).setWithStd(true)
    val strokeScalerModel = strokeScaler.fit(strokeAssembled)
    println("    Stroke scaler fitted")

    // Lung cancer scaler
    val lungCols = Seq("Age", "Gender", "Air Pollution", "Alcohol use", "Dust Allergy",
      "OccuPational Hazards", "Genetic Risk", "chronic Lung Disease",
      "Balanced Diet", "Obesity", "Smoking", "Passive Smoker", "Chest Pain",
      "Coughing of Blood", "Fatigue", "Weight Loss", "Shortness of Breath",
      "Wheezing", "Swallowing Difficulty", "Clubbing of Finger Nails",
      "Frequent Cold", "Dry Cough", "Snoring")

    val lungClean = spark.read.parquet("/medispark/clean/lungcancer")
    val lungAssembler = new VectorAssembler()
      .setInputCols(lungCols.toArray).setOutputCol("features_raw").setHandleInvalid("skip")
    val lungAssembled = lungAssembler.transform(
      lungCols.foldLeft(lungClean)((df, c) => df.withColumn(c, col(c).cast(DoubleType)))
    )
    val lungScaler = new StandardScaler()
      .setInputCol("features_raw").setOutputCol("features").setWithMean(true).setWithStd(true)
    val lungScalerModel = lungScaler.fit(lungAssembled)
    println("    Lung cancer scaler fitted")

    println("  All scalers ready!")

    // --- Step 4: Define the JSON schema for incoming patient records ---
    // The Python generator sends JSON files with fields for all 4 diseases.
    // Each patient record contains all the fields needed across all diseases.
    val patientSchema = new StructType()
      .add("patient_id", StringType)
      .add("timestamp", StringType)
      // Heart/Diabetes shared fields (BRFSS survey format)
      .add("HighBP", DoubleType)
      .add("HighChol", DoubleType)
      .add("CholCheck", DoubleType)
      .add("BMI", DoubleType)
      .add("Smoker", DoubleType)
      .add("Stroke", DoubleType)
      .add("Diabetes", DoubleType)
      .add("HeartDiseaseorAttack", DoubleType)
      .add("PhysActivity", DoubleType)
      .add("Fruits", DoubleType)
      .add("Veggies", DoubleType)
      .add("HvyAlcoholConsump", DoubleType)
      .add("AnyHealthcare", DoubleType)
      .add("NoDocbcCost", DoubleType)
      .add("GenHlth", DoubleType)
      .add("MentHlth", DoubleType)
      .add("PhysHlth", DoubleType)
      .add("DiffWalk", DoubleType)
      .add("Sex", DoubleType)
      .add("Age", DoubleType)
      .add("Education", DoubleType)
      .add("Income", DoubleType)
      // Stroke-specific fields
      .add("gender", StringType)
      .add("age_stroke", DoubleType)
      .add("hypertension", DoubleType)
      .add("heart_disease", DoubleType)
      .add("ever_married", StringType)
      .add("work_type", StringType)
      .add("Residence_type", StringType)
      .add("avg_glucose_level", DoubleType)
      .add("bmi_stroke", DoubleType)
      .add("smoking_status", StringType)
      // Lung cancer-specific fields
      .add("Gender_lung", DoubleType)
      .add("Air_Pollution", DoubleType)
      .add("Alcohol_use", DoubleType)
      .add("Dust_Allergy", DoubleType)
      .add("OccuPational_Hazards", DoubleType)
      .add("Genetic_Risk", DoubleType)
      .add("chronic_Lung_Disease", DoubleType)
      .add("Balanced_Diet", DoubleType)
      .add("Obesity", DoubleType)
      .add("Smoking_lung", DoubleType)
      .add("Passive_Smoker", DoubleType)
      .add("Chest_Pain", DoubleType)
      .add("Coughing_of_Blood", DoubleType)
      .add("Fatigue", DoubleType)
      .add("Weight_Loss", DoubleType)
      .add("Shortness_of_Breath", DoubleType)
      .add("Wheezing", DoubleType)
      .add("Swallowing_Difficulty", DoubleType)
      .add("Clubbing_of_Finger_Nails", DoubleType)
      .add("Frequent_Cold", DoubleType)
      .add("Dry_Cough", DoubleType)
      .add("Snoring", DoubleType)

    // --- Step 5: Set up Structured Streaming ---
    // Read JSON files from /medispark/stream/input/ as they arrive
    println("\n  Starting Structured Streaming...")
    println("  Watching: /medispark/stream/input/")
    println("  Trigger: every 5 seconds")
    println(s"\n${"=" * 60}")
    println("  WAITING FOR PATIENT RECORDS...")
    println(s"${"=" * 60}\n")

    val streamDF = spark.readStream
      .schema(patientSchema)
      .option("maxFilesPerTrigger", 1)  // process 1 file per trigger
      .json("/medispark/stream/input/")

    // --- Step 6: Process each micro-batch ---
    // foreachBatch lets us run arbitrary DataFrame operations on each batch
    val query = streamDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        if (batchDF.isEmpty) {
          // No new files — skip silently
        } else {
          val patientCount = batchDF.count()
          println(s"\n  *** Batch $batchId: Processing $patientCount patient(s) ***")

          // Process each patient in the batch
          batchDF.collect().foreach { row =>
            val patientId = row.getAs[String]("patient_id")
            val timestamp = row.getAs[String]("timestamp")

            println(s"\n  Patient: $patientId | Time: $timestamp")
            println(s"  ${"─" * 50}")

            // === HEART DISEASE SCORING ===
            try {
              val heartData = Seq((
                row.getAs[Double]("HighBP"), row.getAs[Double]("HighChol"),
                row.getAs[Double]("CholCheck"), row.getAs[Double]("BMI"),
                row.getAs[Double]("Smoker"), row.getAs[Double]("Stroke"),
                row.getAs[Double]("Diabetes"), row.getAs[Double]("PhysActivity"),
                row.getAs[Double]("Fruits"), row.getAs[Double]("Veggies"),
                row.getAs[Double]("HvyAlcoholConsump"), row.getAs[Double]("AnyHealthcare"),
                row.getAs[Double]("NoDocbcCost"), row.getAs[Double]("GenHlth"),
                row.getAs[Double]("MentHlth"), row.getAs[Double]("PhysHlth"),
                row.getAs[Double]("DiffWalk"), row.getAs[Double]("Sex"),
                row.getAs[Double]("Age"), row.getAs[Double]("Education"),
                row.getAs[Double]("Income")
              )).toDF(heartCols: _*)

              val heartCasted = heartCols.foldLeft(heartData)((df, c) => df.withColumn(c, col(c).cast(DoubleType)))
              val heartFeatRaw = heartAssembler.transform(heartCasted)
              val heartFeat = heartScalerModel.transform(heartFeatRaw)
              val heartPred = heartModel.transform(heartFeat)
              val heartProb = heartPred.select("probability").first().getAs[Vector](0)
              val heartRisk = if (heartProb(1) > 0.6) "HIGH" else if (heartProb(1) > 0.3) "MEDIUM" else "LOW"
              println(f"    Heart Disease:  $heartRisk%-8s (prob: ${heartProb(1)}%.3f)")
            } catch {
              case e: Exception => println(s"    Heart Disease:  ERROR (${e.getMessage.take(50)})")
            }

            // === DIABETES SCORING ===
            try {
              val diabetesData = Seq((
                row.getAs[Double]("HighBP"), row.getAs[Double]("HighChol"),
                row.getAs[Double]("CholCheck"), row.getAs[Double]("BMI"),
                row.getAs[Double]("Smoker"), row.getAs[Double]("Stroke"),
                row.getAs[Double]("HeartDiseaseorAttack"), row.getAs[Double]("PhysActivity"),
                row.getAs[Double]("Fruits"), row.getAs[Double]("Veggies"),
                row.getAs[Double]("HvyAlcoholConsump"), row.getAs[Double]("AnyHealthcare"),
                row.getAs[Double]("NoDocbcCost"), row.getAs[Double]("GenHlth"),
                row.getAs[Double]("MentHlth"), row.getAs[Double]("PhysHlth"),
                row.getAs[Double]("DiffWalk"), row.getAs[Double]("Sex"),
                row.getAs[Double]("Age"), row.getAs[Double]("Education"),
                row.getAs[Double]("Income")
              )).toDF(diabetesCols: _*)

              val diabetesCasted = diabetesCols.foldLeft(diabetesData)((df, c) => df.withColumn(c, col(c).cast(DoubleType)))
              val diabetesFeatRaw = diabetesAssembler.transform(diabetesCasted)
              val diabetesFeat = diabetesScalerModel.transform(diabetesFeatRaw)
              val diabetesPred = diabetesModel.transform(diabetesFeat)
              val diabetesProb = diabetesPred.select("probability").first().getAs[Vector](0)
              val diabetesRisk = if (diabetesProb(1) > 0.6) "HIGH" else if (diabetesProb(1) > 0.3) "MEDIUM" else "LOW"
              println(f"    Diabetes:       $diabetesRisk%-8s (prob: ${diabetesProb(1)}%.3f)")
            } catch {
              case e: Exception => println(s"    Diabetes:       ERROR (${e.getMessage.take(50)})")
            }

            // === STROKE SCORING ===
            try {
              val strokeData = Seq((
                row.getAs[String]("gender"),
                row.getAs[Double]("age_stroke"),
                row.getAs[Double]("hypertension"),
                row.getAs[Double]("heart_disease"),
                row.getAs[String]("ever_married"),
                row.getAs[String]("work_type"),
                row.getAs[String]("Residence_type"),
                row.getAs[Double]("avg_glucose_level"),
                row.getAs[Double]("bmi_stroke"),
                row.getAs[String]("smoking_status")
              )).toDF("gender", "age", "hypertension", "heart_disease", "ever_married",
                "work_type", "Residence_type", "avg_glucose_level", "bmi", "smoking_status")

              val strokeCasted = strokeNumCols.foldLeft(strokeData)((df, c) => df.withColumn(c, col(c).cast(DoubleType)))
              val strokeIndexed2 = strokeIndexerModel.transform(strokeCasted)
              val strokeFeatRaw = strokeAssembler.transform(strokeIndexed2)
              val strokeFeat = strokeScalerModel.transform(strokeFeatRaw)
              val strokePred = strokeModel.transform(strokeFeat)
              val strokeProb = strokePred.select("probability").first().getAs[Vector](0)
              val strokeRisk = if (strokeProb(1) > 0.6) "HIGH" else if (strokeProb(1) > 0.3) "MEDIUM" else "LOW"
              println(f"    Stroke:         $strokeRisk%-8s (prob: ${strokeProb(1)}%.3f)")
            } catch {
              case e: Exception => println(s"    Stroke:         ERROR (${e.getMessage.take(50)})")
            }

            // === LUNG CANCER SCORING ===
            try {
              // Map from JSON field names (underscored) to original column names (with spaces)
              // Scala tuples max at 22 elements, lung cancer has 23 — so we use Row + schema
              val lungJsonFields = Seq("Age", "Gender_lung", "Air_Pollution", "Alcohol_use",
                "Dust_Allergy", "OccuPational_Hazards", "Genetic_Risk", "chronic_Lung_Disease",
                "Balanced_Diet", "Obesity", "Smoking_lung", "Passive_Smoker", "Chest_Pain",
                "Coughing_of_Blood", "Fatigue", "Weight_Loss", "Shortness_of_Breath",
                "Wheezing", "Swallowing_Difficulty", "Clubbing_of_Finger_Nails",
                "Frequent_Cold", "Dry_Cough", "Snoring")
              val lungValues = lungJsonFields.map(f => row.getAs[Double](f))
              val lungRow = Row.fromSeq(lungValues)
              val lungSchema = StructType(lungCols.map(c => StructField(c, DoubleType, true)))
              val lungData = spark.createDataFrame(
                spark.sparkContext.parallelize(Seq(lungRow)), lungSchema
              )
              val lungFeatRaw = lungAssembler.transform(lungData)
              val lungFeat = lungScalerModel.transform(lungFeatRaw)
              val lungPred = lungcancerModel.transform(lungFeat)
              val lungPredLabel = lungPred.select("prediction").first().getDouble(0)
              // Label mapping from Stage 3 StringIndexer: 0=High, 1=Medium, 2=Low
              val lungRisk = lungPredLabel match {
                case 0.0 => "HIGH"
                case 1.0 => "MEDIUM"
                case _ => "LOW"
              }
              val lungProb = lungPred.select("probability").first().getAs[Vector](0)
              println(f"    Lung Cancer:    $lungRisk%-8s (prob: H=${lungProb(0)}%.3f M=${lungProb(1)}%.3f L=${lungProb(2)}%.3f)")
            } catch {
              case e: Exception => println(s"    Lung Cancer:    ERROR (${e.getMessage.take(50)})")
            }

            println(s"  ${"─" * 50}")
          }
        }
      }
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // Keep the streaming query running until manually stopped (Ctrl+C)
    query.awaitTermination()
  }
}
