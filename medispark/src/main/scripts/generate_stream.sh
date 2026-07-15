#!/bin/bash
# =============================================================================
# generate_stream.sh
# Stage 5 of MediSpark — Patient Stream Generator (Bash version)
#
# Generates a synthetic patient JSON every 5 seconds and writes it to HDFS.
# Runs inside the namenode container where hdfs commands are available.
#
# Usage: docker exec -it namenode bash /tmp/generate_stream.sh
# =============================================================================

echo "============================================================"
echo "  MediSpark — Patient Stream Generator"
echo "============================================================"
echo "  Generating a new patient record every 5 seconds"
echo "  Writing to: /medispark/stream/input/"
echo "  Press Ctrl+C to stop"
echo "============================================================"

NAMES=("Arun" "Priya" "Rahul" "Sneha" "Vikram" "Ananya" "Karthik" "Divya" "Suresh" "Meera" "Arjun" "Lakshmi" "Ravi" "Deepa" "Sanjay" "Kavitha" "Amit" "Pooja" "Rajesh" "Nandini")
LASTNAMES=("Sharma" "Patel" "Kumar" "Reddy" "Nair" "Iyer" "Gupta" "Singh" "Menon" "Pillai" "Rao" "Das" "Joshi" "Verma" "Mehta")
GENDERS=("Male" "Female")
MARRIED=("Yes" "No")
WORKTYPES=("Private" "Self-employed" "Govt_job" "children")
RESIDENCES=("Urban" "Rural")
SMOKESTATUS=("formerly smoked" "never smoked" "smokes" "Unknown")

# Helper: random integer in range [min, max]
rand_range() {
  echo $(( RANDOM % ($2 - $1 + 1) + $1 ))
}

# Helper: random float-like value (integer with one decimal)
rand_float() {
  local whole=$(rand_range $1 $2)
  local decimal=$(rand_range 0 9)
  echo "${whole}.${decimal}"
}

# Helper: pick random element from array
rand_pick() {
  local arr=("$@")
  echo "${arr[RANDOM % ${#arr[@]}]}"
}

# Helper: random binary 0.0 or 1.0
rand_binary() {
  if (( RANDOM % 2 == 0 )); then echo "0.0"; else echo "1.0"; fi
}

COUNT=1

while true; do
  # Generate patient ID
  FIRST=$(rand_pick "${NAMES[@]}")
  LAST=$(rand_pick "${LASTNAMES[@]}")
  PID="PAT-${FIRST}-${LAST}-$(rand_range 1000 9999)"
  TSTAMP=$(date "+%Y-%m-%d %H:%M:%S")

  # Shared heart/diabetes fields
  HIGHBP=$(rand_binary)
  HIGHCHOL=$(rand_binary)
  CHOLCHECK=$(rand_binary)
  BMI=$(rand_float 15 49)
  SMOKER=$(rand_binary)
  STROKE=$(rand_binary)
  DIABETES=$(rand_pick "0.0" "1.0" "2.0")
  HEARTATTACK=$(rand_binary)
  PHYSACT=$(rand_binary)
  FRUITS=$(rand_binary)
  VEGGIES=$(rand_binary)
  HVYALC=$(rand_binary)
  HEALTHCARE=$(rand_binary)
  NODOC=$(rand_binary)
  GENHLTH=$(rand_range 1 5).0
  MENTHLTH=$(rand_range 0 30).0
  PHYSHLTH=$(rand_range 0 30).0
  DIFFWALK=$(rand_binary)
  SEX=$(rand_binary)
  AGECAT=$(rand_range 1 13).0
  EDUCATION=$(rand_range 1 6).0
  INCOME=$(rand_range 1 8).0

  # Stroke fields
  GENDER=$(rand_pick "${GENDERS[@]}")
  AGE_STROKE=$(rand_float 20 84)
  HYPERTENSION=$(rand_binary)
  HEART_DIS=$(rand_binary)
  EVER_MARRIED=$(rand_pick "${MARRIED[@]}")
  WORK_TYPE=$(rand_pick "${WORKTYPES[@]}")
  RESIDENCE=$(rand_pick "${RESIDENCES[@]}")
  GLUCOSE=$(rand_float 55 279)
  BMI_STROKE=$(rand_float 15 49)
  SMOKE_STAT=$(rand_pick "${SMOKESTATUS[@]}")

  # Lung cancer fields (1-8 severity)
  GENDER_LUNG=$(rand_pick "1.0" "2.0")
  AIR_POLL=$(rand_range 1 8).0
  ALCOHOL=$(rand_range 1 8).0
  DUST=$(rand_range 1 8).0
  OCCUP=$(rand_range 1 8).0
  GENETIC=$(rand_range 1 8).0
  CHRONIC_LUNG=$(rand_range 1 7).0
  BALANCED=$(rand_range 1 7).0
  OBESITY=$(rand_range 1 7).0
  SMOKING_LUNG=$(rand_range 1 8).0
  PASSIVE=$(rand_range 1 8).0
  CHEST=$(rand_range 1 9).0
  COUGHING=$(rand_range 1 9).0
  FATIGUE=$(rand_range 1 9).0
  WEIGHT=$(rand_range 1 8).0
  BREATH=$(rand_range 1 9).0
  WHEEZE=$(rand_range 1 8).0
  SWALLOW=$(rand_range 1 8).0
  CLUBBING=$(rand_range 1 9).0
  COLD=$(rand_range 1 7).0
  DRY_COUGH=$(rand_range 1 7).0
  SNORING=$(rand_range 1 7).0

  # Build JSON
  JSON=$(cat <<EOF
{"patient_id":"${PID}","timestamp":"${TSTAMP}","HighBP":${HIGHBP},"HighChol":${HIGHCHOL},"CholCheck":${CHOLCHECK},"BMI":${BMI},"Smoker":${SMOKER},"Stroke":${STROKE},"Diabetes":${DIABETES},"HeartDiseaseorAttack":${HEARTATTACK},"PhysActivity":${PHYSACT},"Fruits":${FRUITS},"Veggies":${VEGGIES},"HvyAlcoholConsump":${HVYALC},"AnyHealthcare":${HEALTHCARE},"NoDocbcCost":${NODOC},"GenHlth":${GENHLTH},"MentHlth":${MENTHLTH},"PhysHlth":${PHYSHLTH},"DiffWalk":${DIFFWALK},"Sex":${SEX},"Age":${AGECAT},"Education":${EDUCATION},"Income":${INCOME},"gender":"${GENDER}","age_stroke":${AGE_STROKE},"hypertension":${HYPERTENSION},"heart_disease":${HEART_DIS},"ever_married":"${EVER_MARRIED}","work_type":"${WORK_TYPE}","Residence_type":"${RESIDENCE}","avg_glucose_level":${GLUCOSE},"bmi_stroke":${BMI_STROKE},"smoking_status":"${SMOKE_STAT}","Gender_lung":${GENDER_LUNG},"Air_Pollution":${AIR_POLL},"Alcohol_use":${ALCOHOL},"Dust_Allergy":${DUST},"OccuPational_Hazards":${OCCUP},"Genetic_Risk":${GENETIC},"chronic_Lung_Disease":${CHRONIC_LUNG},"Balanced_Diet":${BALANCED},"Obesity":${OBESITY},"Smoking_lung":${SMOKING_LUNG},"Passive_Smoker":${PASSIVE},"Chest_Pain":${CHEST},"Coughing_of_Blood":${COUGHING},"Fatigue":${FATIGUE},"Weight_Loss":${WEIGHT},"Shortness_of_Breath":${BREATH},"Wheezing":${WHEEZE},"Swallowing_Difficulty":${SWALLOW},"Clubbing_of_Finger_Nails":${CLUBBING},"Frequent_Cold":${COLD},"Dry_Cough":${DRY_COUGH},"Snoring":${SNORING}}
EOF
)

  # Write to temp file, then put to HDFS
  echo "$JSON" > /tmp/patient_${COUNT}.json
  hdfs dfs -put -f /tmp/patient_${COUNT}.json /medispark/stream/input/patient_${COUNT}.json 2>/dev/null
  rm -f /tmp/patient_${COUNT}.json

  echo ""
  echo "  [${TSTAMP}] Patient #${COUNT}"
  echo "    ID: ${PID}"
  echo "    Written to: /medispark/stream/input/patient_${COUNT}.json"

  COUNT=$((COUNT + 1))
  sleep 5
done
