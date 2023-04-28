import boto3
import json

s3 = boto3.client("s3")


def handler(event, context):
    s = 0
    results = event["results"]
    bucket = results["ResultWriterDetails"]["Bucket"]
    key = results["ResultWriterDetails"]["Key"]

    data = s3.get_object(Bucket=bucket, Key=key)
    manifest = json.loads(data["Body"].read().decode("utf-8"))

    succeeded = manifest["ResultFiles"]["SUCCEEDED"]
    for obj in succeeded:
        data = s3.get_object(Bucket=bucket, Key=obj["Key"])
        successful_results = json.loads(data["Body"].read().decode("utf-8"))
        # print(result)
        for result in successful_results:
            payload = json.loads(result["Output"])
            s += int(payload["Payload"]["result"])

    event.update({"total_sum": s})
    return event
