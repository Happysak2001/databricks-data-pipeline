import json
import urllib3
import os

def lambda_handler(event, context):

    for record in event["Records"]:
        bucket   = record["s3"]["bucket"]["name"]
        file_key = record["s3"]["object"]["key"]
        print(f"New file detected: s3://{bucket}/{file_key}")

        databricks_url   = os.environ["DATABRICKS_URL"]
        databricks_token = os.environ["DATABRICKS_TOKEN"]
        job_id           = os.environ["DATABRICKS_JOB_ID"]

        http = urllib3.PoolManager()

        # Pass bucket and file_key as parameters to the Databricks job
        payload = {
            "job_id": int(job_id),
            "notebook_params": {
                "bucket_name": bucket,
                "file_key":    file_key
            }
        }

        response = http.request(
            "POST",
            f"{databricks_url}/api/2.1/jobs/run-now",
            headers={
                "Authorization": f"Bearer {databricks_token}",
                "Content-Type":  "application/json"
            },
            body=json.dumps(payload)
        )

        result = json.loads(response.data.decode("utf-8"))
        print(f"Databricks job triggered for {file_key}. Run ID: {result.get('run_id')}")

    return {
        "statusCode": 200,
        "body": f"Pipeline triggered. Run ID: {result.get('run_id')}"
    }
