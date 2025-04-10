#!/bin/bash

/entrypoint airflow version

echo "Setting up connection to Google Cloud Platform"
airflow connections add google_cloud \
    --conn-type google_cloud_platform \
    --conn-extra "{\"extra__google_cloud_platform__project\": \"${GCP_PROJECT_ID}\"}"

echo "Setting up Spark connection"
airflow connections add spark \
    --conn-type spark \
    --conn-host "spark://spark-master" \
    --conn-port 7077

# Set up AWS credentials if running on ECS
if [ -n "$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" ]; then
  echo "Running on ECS (Fargate)"
  
  CREDENTIALS_URL="http://169.254.170.2${AWS_CONTAINER_CREDENTIALS_RELATIVE_URI}"

  creds_json=$(curl -s $CREDENTIALS_URL)

  export AWS_ACCESS_KEY_ID=$(echo $creds_json | jq -r '.AccessKeyId')
  export AWS_SECRET_ACCESS_KEY=$(echo $creds_json | jq -r '.SecretAccessKey')
  export AWS_SESSION_TOKEN=$(echo $creds_json | jq -r '.Token')

  echo "AWS credentials have been set in the environment."
else
  echo "Running locally (not on ECS)"
fi

# Start Airflow scheduler
airflow scheduler &

# Start Airflow web server
# airflow webserver &

DAG_ID="monthly_etl_dag"

echo "Waiting for Airflow to start..."
sleep 10

echo "Triggering DAG $DAG_ID"
airflow dags unpause $DAG_ID
airflow dags trigger $DAG_ID

# Wait for the DAG to complete
while true; do
    STATUS=$(airflow dags list-runs -d $DAG_ID --output table | awk 'NR==3 {print $5}')
    
    echo "Current status of DAG run: $STATUS"
    
    if [[ "$STATUS" == "success" || "$STATUS" == "failed" ]]; then
        echo "DAG run completed with status: $STATUS"
        break
    fi
    
    sleep 10
done

echo "Exiting container..."
exit 0