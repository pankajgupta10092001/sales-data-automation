import functions_framework
from google.cloud import bigquery


# @functions_framework.cloud_event
# def hello_gcs(cloud_event):
#     data = cloud_event.data
#     print(data)
#     print(cloud_event)

#     # Correct way to access attributes
#     event_id = cloud_event.get("id")
#     event_type = cloud_event.get("type")

#     bucket = data.get("bucket")
#     filename = data.get("name")
#     metageneration = data.get("metageneration")
#     time_created = data.get("timeCreated")
#     updated = data.get("updated")

#     print(f"Event ID: {event_id}")
#     print(f"Event type: {event_type}")
#     print(f"Bucket: {bucket}")
#     print(f"File: {filename}")
#     print(f"Metageneration: {metageneration}")
#     print(f"Created: {time_created}")
#     print(f"Updated: {updated}")

#     return "OK"

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data
    proto = data.get("protoPayload", {})

    # Only process file upload events
    if proto.get("methodName") != "storage.objects.create":
        print("Not a file upload event")
        return "Ignored"

    resource = proto.get("resourceName", "")
    
    # Example:
    # projects/_/buckets/bkt-sales-data-automation/objects/australia.csv
    parts = resource.split("/")

    bucket = parts[3]
    file_name = parts[-1]

    print(f"Bucket: {bucket}")
    print(f"File: {file_name}")

    # GCS path
    uri = f"gs://{bucket}/{file_name}"

    # BigQuery setup
    client = bigquery.Client()
    table_id = "metal-dimension-402820.sales.orders"

    job_config = bigquery.LoadJobConfig(
       autodetect=True,
       source_format=bigquery.SourceFormat.CSV,
       skip_leading_rows=1,
       create_disposition="CREATE_IF_NEEDED",   
       write_disposition="WRITE_APPEND"   
    )

    # Load job
    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()

    print(" File loaded into BigQuery")

    return "Success"


