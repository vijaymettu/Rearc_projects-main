import urllib3
import json
import boto3
import os

s3 = boto3.client("s3")
http = urllib3.PoolManager()

##url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"

url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

headers = {
    "User-Agent": "BLSDataSync/1.0 (vijayreddim@gmail.com)",
    "Accept": "application/json"
}

def lambda_handler(event, context):
    response = http.request("GET", url, headers=headers)

    print(response.status)
    if response.status == 200:
        data = json.loads(response.data.decode("utf-8"))
        print(data)
        records = data.get("data", [])


        # Prepare JSONL content
        jsonl_content = "\n".join(json.dumps(record) for record in records)
        # Upload to S3
        s3.put_object(
            Bucket="vm-rearc-data",
            Key="dipal_poc/rearc/data/population_data.jsonl",
            Body=jsonl_content,
            ContentType="application/json"
        )

        return {
            "statusCode": 200,
            "body": "Data successfully uploaded to S3."
        }
    else:
        return {
            "statusCode": response.status,
            "body": f"Request failed with status code {response.status}"
        }


print(lambda_handler('',''))