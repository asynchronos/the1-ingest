from google.cloud import storage
import io
import pyarrow.parquet as pq

def copy_parquet_blob(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name, source_project_id, destination_project_id):
    """Copies a Parquet blob from one GCS bucket to another, potentially across projects."""

    try:
        # Initialize clients for both projects using project IDs
        source_storage_client = storage.Client(project=source_project_id)
        destination_storage_client = storage.Client(project=destination_project_id)

        source_bucket = source_storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)

        destination_bucket = destination_storage_client.bucket(destination_bucket_name)
        destination_blob = destination_bucket.blob(destination_blob_name)

        # Download the source blob as bytes
        source_blob_bytes = source_blob.download_as_bytes()

        # Upload the bytes to the destination blob
        destination_blob.upload_from_file(io.BytesIO(source_blob_bytes), content_type="application/octet-stream")

        print(f"Parquet blob {source_blob_name} in bucket {source_bucket_name} copied to "
              f"{destination_blob_name} in bucket {destination_bucket_name} (projects: {source_project_id} -> {destination_project_id}).")

    except Exception as e:
        print(f"An error occurred: {e}")

def copy_parquet_directory(source_bucket_name, source_prefix, destination_bucket_name, destination_prefix, source_project_id, destination_project_id):
    """Copies a directory of Parquet blobs from one GCS bucket to another, potentially across projects."""
    try:
        source_storage_client = storage.Client(project=source_project_id)
        destination_storage_client = storage.Client(project=destination_project_id)

        source_bucket = source_storage_client.bucket(source_bucket_name)
        destination_bucket = destination_storage_client.bucket(destination_bucket_name)

        blobs = source_storage_client.list_blobs(source_bucket, prefix=source_prefix)

        for blob in blobs:
            if blob.name.endswith(".parquet"):
                relative_path = blob.name[len(source_prefix):] # get the part after the source prefix.
                destination_blob_name = destination_prefix + relative_path
                copy_parquet_blob(source_bucket_name, blob.name, destination_bucket_name, destination_blob_name, source_project_id, destination_project_id)

    except Exception as e:
        print(f"An error occurred: {e}")

def copy_csv_blob(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name, source_project_id, destination_project_id):
    """Copies a CSV blob from one GCS bucket to another, potentially across projects."""
    try:
        source_storage_client = storage.Client(project=source_project_id)
        destination_storage_client = storage.Client(project=destination_project_id)

        source_bucket = source_storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)

        destination_bucket = destination_storage_client.bucket(destination_bucket_name)
        destination_blob = destination_bucket.blob(destination_blob_name)

        source_blob_bytes = source_blob.download_as_bytes()
        destination_blob.upload_from_file(io.BytesIO(source_blob_bytes), content_type="text/csv") #setting content type to text/csv

        print(f"CSV blob {source_blob_name} in bucket {source_bucket_name} copied to "
              f"{destination_blob_name} in bucket {destination_bucket_name} (projects: {source_project_id} -> {destination_project_id}).")

    except Exception as e:
        print(f"An error occurred: {e}")

def copy_csv_directory(source_bucket_name, source_prefix, destination_bucket_name, destination_prefix, source_project_id, destination_project_id):
    """Copies a directory of CSV blobs from one GCS bucket to another, potentially across projects."""
    try:
        source_storage_client = storage.Client(project=source_project_id)
        destination_storage_client = storage.Client(project=destination_project_id)

        source_bucket = source_storage_client.bucket(source_bucket_name)
        destination_bucket = destination_storage_client.bucket(destination_bucket_name)

        blobs = source_storage_client.list_blobs(source_bucket, prefix=source_prefix)

        for blob in blobs:
            if blob.name.endswith(".csv"):
                relative_path = blob.name[len(source_prefix):]
                destination_blob_name = destination_prefix + relative_path
                copy_csv_blob(source_bucket_name, blob.name, destination_bucket_name, destination_blob_name, source_project_id, destination_project_id)

    except Exception as e:
        print(f"An error occurred: {e}")

# # Example usage (single Parquet blob):
# source_project_id = "cdg-data-uni-doff-prod"
# destination_project_id = "cdg-data-uni-dev"
# source_bucket_name = "cdg-data-uni-doff-the1-prod"
# destination_bucket_name = "cdg-data-uni-doff-the1-dev"

# source_blob_name = "path/to/source/file.parquet"
# destination_blob_name = "path/to/destination/file.parquet"

# # copy_parquet_blob(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name, source_project_id, destination_project_id)

# # Example usage (directory of Parquet blobs):
# table_name = "member_active_by_location_crc"
# source_prefix = f"{table_name}/par_month=201901/par_day=20190101"
# destination_prefix = source_prefix

# source_prefix_ctrl = f"{table_name+'_ctrl'}/par_month=201901/par_day=20190101"
# destination_prefix_ctrl = source_prefix_ctrl
# # copy_parquet_directory(source_bucket_name, source_prefix, destination_bucket_name, destination_prefix, source_project_id, destination_project_id)
    
def copy_the1_bucket_from_prod(table_name, partition):
    source_project_id = "cdg-data-uni-doff-prod"
    destination_project_id = "cdg-data-uni-dev"
    source_bucket_name = "cdg-data-uni-doff-the1-prod"
    destination_bucket_name = "cdg-data-uni-doff-the1-dev"

    source_prefix = f"{'/'.join([table_name,partition])}"
    source_prefix_ctrl = f"{'/'.join([table_name+'_ctrl',partition])}"
    destination_prefix = source_prefix
    destination_prefix_ctrl = source_prefix_ctrl

    print(f"{source_prefix + '-->' + destination_prefix} data coping..")
    copy_parquet_directory(source_bucket_name, source_prefix, destination_bucket_name, destination_prefix, source_project_id, destination_project_id)
    print(f"{source_prefix_ctrl + '-->' + destination_prefix_ctrl} ctrl coping..")
    copy_csv_directory(source_bucket_name, source_prefix_ctrl, destination_bucket_name, destination_prefix_ctrl, source_project_id, destination_project_id)


if __name__ == '__main__':

    copy_the1_bucket_from_prod("campaign_request","")
    copy_the1_bucket_from_prod("customer_request","")
    copy_the1_bucket_from_prod("member_active_by_location_crc","par_month=201901/par_day=20190101")
    copy_the1_bucket_from_prod("member_active_last_24m","")
    copy_the1_bucket_from_prod("member_active_total_crc","par_month=201901/par_day=20190101")

    copy_the1_bucket_from_prod("member_expat_crc","")
    copy_the1_bucket_from_prod("ms_branch","")
    copy_the1_bucket_from_prod("ms_member_merge","")
    copy_the1_bucket_from_prod("ms_point_balance_expire","")
    copy_the1_bucket_from_prod("ms_product","")
    
    copy_the1_bucket_from_prod("my_little_club_member_segment_hist","par_month=202411/par_day=20241111")
    copy_the1_bucket_from_prod("sales_sku","par_month=201901/par_day=20190101")
    copy_the1_bucket_from_prod("sales_tender","par_month=201901/par_day=20190101")
    copy_the1_bucket_from_prod("tran_earned","par_month=201901/par_day=20190101")
    copy_the1_bucket_from_prod("tran_redeem","par_month=201901/par_day=20190101")