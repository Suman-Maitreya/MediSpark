// ==============================================================================
// MediSpark — Stage 5: Streaming Simulation
// Structured Streaming with file source — scores micro-batch CSVs
// using trained RandomForest models
// ==============================================================================
package medispark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}

object StreamScoring {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MediSpark-Stage5-Streaming")
      .getOrCreate()

    import spark.implicits._

    println("=" * 70)
    println("  MEDISPARK — STAGE 5: STREAMING SIMULATION")
    println("=" * 70)

    val modelPath  = "hdfs://namenode:9000/medispark/models"
    val streamIn   = "hdfs://namenode:9000/medispark/stream_input"
    val streamOut  = "hdfs://namenode:9000/medispark/stream_output"

    // ── Load all 4 models ───────────────────────────────────────
    println("\n  Loading trained models...")
    val heartModel    = RandomForestClassificationModel.load(s"$modelPath/heart")
    val diabetesModel = RandomForestClassificationModel.load(s"$modelPath/diabetes")
    val strokeModel   = RandomForestClassificationModel.load(s"$modelPath/stroke")
    val lungModel     = RandomForestClassificationModel.load(s"$modelPath/lung_cancer")
    println("  All 4 models loaded.")

    // ── Define streaming schema ─────────────────────────────────
    // The stream generator writes CSVs with these common features
    val streamSchema = StructType(Seq(
      StructField("patient_id", StringType),
      StructField("timestamp", StringType),
      StructField("age", DoubleType),
      StructField("gender", DoubleType),
      StructField("bmi", DoubleType),
      StructField("blood_pressure", DoubleType),
      StructField("cholesterol", DoubleType),
      StructField("glucose", DoubleType),
      StructField("smoking", DoubleType),
      StructField("exercise", DoubleType),
      StructField("hypertension", DoubleType),
      StructField("heart_disease_history", DoubleType)
    ))

    // ── Start structured streaming ──────────────────────────────
    println("\n  Starting structured streaming from: " + streamIn)
    println("  Writing scored output to: " + streamOut)
    println("  Waiting for micro-batch CSV files...")
    println("  (Run generate_stream.py to produce data)")
    println("  (Press Ctrl+C to stop)")

    val streamDf = spark.readStream
      .option("header", "true")
      .schema(streamSchema)
      .csv(streamIn)

    // ── Score each micro-batch ──────────────────────────────────
    val query = streamDf.writeStream
      .foreachBatch { (batchDf: org.apache.spark.sql.DataFrame, batchId: Long) =>

        if (batchDf.count() > 0) {
          println(s"\n  ── Micro-batch $batchId: ${batchDf.count()} patients ──")

          // --- Heart features (UCI: 13 features) ---
          // Map streaming fields to heart model features:
          // age, sex, cp, trestbps, chol, fbs, restecg, thalach, exang, oldpeak, slope, ca, thal
          val heartDf = batchDf
            .withColumn("sex", $"gender")
            .withColumn("cp", lit(0.0))   // default: typical angina
            .withColumn("trestbps", $"blood_pressure")
            .withColumn("chol", $"cholesterol")
            .withColumn("fbs", when($"glucose" > 120, 1.0).otherwise(0.0))
            .withColumn("restecg", lit(1.0))
            .withColumn("thalach", lit(150.0) - ($"age" * 0.5))
            .withColumn("exang", $"exercise".cast(DoubleType))
            .withColumn("oldpeak", lit(1.0))
            .withColumn("slope", lit(1.0))
            .withColumn("ca", lit(0.0))
            .withColumn("thal", lit(2.0))

          val heartFeatures = Array("age","sex","cp","trestbps","chol","fbs",
            "restecg","thalach","exang","oldpeak","slope","ca","thal")
          val heartAssembler = new VectorAssembler()
            .setInputCols(heartFeatures).setOutputCol("features")
            .setHandleInvalid("skip")
          val heartAssembled = heartAssembler.transform(heartDf)
          val heartPreds = heartModel.transform(heartAssembled)
            .withColumnRenamed("prediction", "heart_pred")
            .withColumnRenamed("probability", "heart_prob")
            .select("patient_id", "timestamp", "age", "bmi",
                    "heart_pred", "heart_prob")

          // --- Diabetes features (Pima: 8 features) ---
          val diabetesDf = batchDf
            .withColumn("Pregnancies", lit(1.0))
            .withColumn("Glucose", $"glucose")
            .withColumn("BloodPressure", $"blood_pressure")
            .withColumn("SkinThickness", lit(20.0))
            .withColumn("Insulin", lit(80.0))
            .withColumn("BMI_d", $"bmi")
            .withColumn("DiabetesPedigreeFunction", lit(0.5))
            .withColumn("Age_d", $"age")

          val diabetesFeatures = Array("Pregnancies","Glucose","BloodPressure",
            "SkinThickness","Insulin","BMI_d","DiabetesPedigreeFunction","Age_d")
          val diabetesAssembler = new VectorAssembler()
            .setInputCols(diabetesFeatures).setOutputCol("features")
            .setHandleInvalid("skip")
          val diabetesAssembled = diabetesAssembler.transform(diabetesDf)
          val diabetesPreds = diabetesModel.transform(diabetesAssembled)
            .select($"patient_id", $"prediction".as("diabetes_pred"))

          // --- Stroke features (10 features: 5 numeric + 5 indexed) ---
          val strokeDf = batchDf
            .withColumn("avg_glucose_level", $"glucose")
            .withColumn("heart_disease_s", $"heart_disease_history")
            .withColumn("gender_idx", $"gender")
            .withColumn("ever_married_idx", lit(1.0))
            .withColumn("work_type_idx", lit(0.0))
            .withColumn("Residence_type_idx", lit(0.0))
            .withColumn("smoking_status_idx", $"smoking")

          val strokeFeatures = Array("age","hypertension","heart_disease_s",
            "avg_glucose_level","bmi","gender_idx","ever_married_idx",
            "work_type_idx","Residence_type_idx","smoking_status_idx")
          val strokeAssembler = new VectorAssembler()
            .setInputCols(strokeFeatures).setOutputCol("features")
            .setHandleInvalid("skip")
          val strokeAssembled = strokeAssembler.transform(strokeDf)
          val strokePreds = strokeModel.transform(strokeAssembled)
            .select($"patient_id", $"prediction".as("stroke_pred"))

          // --- Lung Cancer features (23 features) ---
          val lungDf = batchDf
            .withColumn("Age_l", $"age")
            .withColumn("Gender_l", ($"gender" + 1.0))  // 1 or 2
            .withColumn("Air_Pollution", lit(4.0))
            .withColumn("Alcohol_use", lit(4.0))
            .withColumn("Dust_Allergy", lit(4.0))
            .withColumn("OccuPational_Hazards", lit(4.0))
            .withColumn("Genetic_Risk", lit(4.0))
            .withColumn("chronic_Lung_Disease", lit(4.0))
            .withColumn("Balanced_Diet", lit(4.0))
            .withColumn("Obesity_l", when($"bmi" > 30, 7.0).otherwise(3.0))
            .withColumn("Smoking_l", when($"smoking" > 0.5, 7.0).otherwise(2.0))
            .withColumn("Passive_Smoker", lit(4.0))
            .withColumn("Chest_Pain", lit(4.0))
            .withColumn("Coughing_of_Blood", lit(4.0))
            .withColumn("Fatigue_l", lit(4.0))
            .withColumn("Weight_Loss", lit(4.0))
            .withColumn("Shortness_of_Breath", lit(4.0))
            .withColumn("Wheezing", lit(4.0))
            .withColumn("Swallowing_Difficulty", lit(4.0))
            .withColumn("Clubbing_of_Finger_Nails", lit(4.0))
            .withColumn("Frequent_Cold", lit(4.0))
            .withColumn("Dry_Cough", lit(4.0))
            .withColumn("Snoring", lit(4.0))

          val lungFeatures = Array("Age_l","Gender_l","Air_Pollution","Alcohol_use",
            "Dust_Allergy","OccuPational_Hazards","Genetic_Risk","chronic_Lung_Disease",
            "Balanced_Diet","Obesity_l","Smoking_l","Passive_Smoker","Chest_Pain",
            "Coughing_of_Blood","Fatigue_l","Weight_Loss","Shortness_of_Breath",
            "Wheezing","Swallowing_Difficulty","Clubbing_of_Finger_Nails",
            "Frequent_Cold","Dry_Cough","Snoring")
          val lungAssembler = new VectorAssembler()
            .setInputCols(lungFeatures).setOutputCol("features")
            .setHandleInvalid("skip")
          val lungAssembled = lungAssembler.transform(lungDf)
          val lungPreds = lungModel.transform(lungAssembled)
            .select($"patient_id", $"prediction".as("lung_pred"))

          // Join all predictions
          val scored = heartPreds
            .join(diabetesPreds, "patient_id")
            .join(strokePreds, "patient_id")
            .join(lungPreds, "patient_id")

          scored.show(5, truncate = false)

          // Write scored batch
          scored.write.mode("append")
            .parquet(s"$streamOut/batch_$batchId")

          println(s"  Batch $batchId scored and saved.")
        }
      }
      .option("checkpointLocation", s"$streamOut/_checkpoint")
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .start()

    // Run for 2 minutes then stop
    query.awaitTermination(120000)
    query.stop()

    println("\n" + "=" * 70)
    println("  STAGE 5 COMPLETE — Streaming simulation finished")
    println("=" * 70)

    spark.stop()
  }
}
