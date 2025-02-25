import functions_framework
import time
import requests
import csv
import gcsfs
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage
from datetime import datetime, timedelta
import os

@functions_framework.cloud_event
def ingest_t1_to_raw(cloud_event):

    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    file_name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print("####  Cloud Event Variable value ####")
    print(f"------ Event ID: {event_id}")
    print(f"------ Event type: {event_type}")
    print(f"------ Bucket: {bucket}")
    print(f"------ Filename: {file_name}")
    print(f"------ Metageneration: {metageneration}")
    print(f"------ Created: {timeCreated}")
    print(f"------ Updated: {updated}")
    print("######################################")

    folder_name = os.path.dirname(file_name)
    filename_part = os.path.basename(file_name)
    print(f'------ folder_name: {folder_name}')
    print(f'------ file_name: {filename_part}')

    # folder_name = "ms_member_ctrl"
    param = get_config(folder_name.replace('_ctrl',''))
    for row in param:
        source_bucket = row['source_information']
        destination_bucket = row['destination_bucket']
        area = row['area']
        task = row['task']
        source_extension = row['source_extension']
        archive_bucket = row['archive_bucket']
        ctrl_extension = row['ctrl_extension']
        target_extension = row['target_extension']
        print(f"""List variable 
                  area : {area}
                  source_bucket : {source_bucket}
                  destination_bucket : {destination_bucket}
                  task : {task}
                  archvie_bucket : {archive_bucket}
                  ctrl_extension : {ctrl_extension}
                  target_extension : {target_extension}
                """)
        ## Start Set variable ##
        ctrl_folder = f"{task}_ctrl"
        process_status = ''
        err_message = ''
        ## End Set variable ##

        if check_success_file(source_bucket, task) == 0:
            total_records, list_log_file, number_log_file = rec_ctrl_files(source_bucket, ctrl_folder, ctrl_extension)
            src_total_records, data_file_detail, number_processed_data_file = rec_src_files(source_bucket, task, target_extension, archive_bucket)
            if total_records == src_total_records:
                try:
                    print(f'################# TRUE #################\n')
                    copy_data_files(source_bucket, destination_bucket, task, area, source_extension)
                    process_status = 'success'

                except ValueError as e:
                    print(f"Error: {e}")
                    err_message = e

            else:
                print("@@@@@@@@@@@@@@@@ FALSE @@@@@@@@@@@@@@@@\n")
                process_status = 'differ'

            archive_data_source(source_bucket, archive_bucket, task, process_status)
            delete_blobs_in_folder(source_bucket, task)

            write_log(source_bucket=source_bucket
            ,destination_bucket=destination_bucket
            ,table_name=task
            ,interface_name=area
            ,list_log_file=list_log_file
            ,number_log_file=number_log_file
            ,total_log_record=total_records
            ,number_processed_data_file=number_processed_data_file
            ,total_record_data_file=src_total_records
            ,data_file_detail=data_file_detail
            ,process_status=process_status
            ,err_message=err_message)  

    return 0

def get_config(value):
    config = []
    client = bigquery.Client()
    query = f"""SELECT source_information
                    , destination_bucket
                    , source_type
                    , area
                    , filename
                    , task
                    , source_extension
                    , ctrl_extension
                    , target_extension
                    , date_pattern
                    , archive_bucket 
    FROM `cdg-data-uni-std-prod.core_system.sys_ingest_configures` 
    where 1 = 1 AND is_current = 1 AND is_enable = '1' and area in ('the1') """
    sub_query = f"and task = '{value}' ORDER BY seq "
    query = query + sub_query
    query = query.replace('[', '(').replace(']', ')')
    try:
        query_job = client.query(query)
        results = query_job.result()
        config = [
            {key: value for key,value in row.items()}
            for row in results
        ]

    except GoogleAPIError as e:
        print(f"An error occurred: {e}")
    return config

def rec_ctrl_files(source_bucket, ctrl_folder, ctrl_extension):
    total_records = 0
    list_log_file = []
    fs = gcsfs.GCSFileSystem()
    pattern = f'gs://{source_bucket}/{ctrl_folder}/*{ctrl_extension}'
    csv_files = fs.glob(pattern)
    number_log_file = len(csv_files)
    for csv_file in csv_files:
        with fs.open(csv_file, mode='r') as file:
            reader = csv.reader(file)
            file_name = os.path.basename(csv_file)
            list_log_file.append(file_name)
            next(reader)
            cnt = 0
            for row in reader:
                cnt = cnt + 1
                if cnt == 2:
                    break
                try:
                    total_records += int(row[2])
                except (ValueError, IndexError):
                    continue
    print(f"total record of {ctrl_folder} : {total_records}\n")
    list_log_file = ','.join(list_log_file)

    return int(total_records), list_log_file, number_log_file

def rec_src_files(source_bucket, source_folder, source_extension, archive_bucket):
    # print("####################### function : rec_src_files - START #######################")
    src_total_records = 0
    data_file_detail = []

    fs = gcsfs.GCSFileSystem()
    files = fs.glob(f'gs://{source_bucket}/{source_folder}/*{source_extension}')
    cnt_files = len(files)
    # print(f"Number of files : {cnt_files}")
    cnt = 0
    for file in files:
        cnt = cnt+1
        print(f"On-Process number ({cnt}/{cnt_files})")
        parquet_file = pq.ParquetDataset(file, filesystem=fs)
        parquet_table = parquet_file.read().to_pandas()
        num_records = len(parquet_table)
        src_total_records += num_records

        file_name = os.path.basename(file)
        data_file_detail.append((file_name, num_records, archive_bucket))

    # print(f"total record of {source_folder} : {src_total_records}\n")

    return int(src_total_records), data_file_detail, cnt_files

def copy_data_files(source_bucket, destination_bucket,source_prefix, area, source_extension):
    client = storage.Client()
    ## Set variable 
    import_date = datetime.now().strftime('%Y-%m-%d')

    bucket_a = client.bucket(source_bucket)
    bucket_b = client.bucket(destination_bucket)

    blobs = bucket_a.list_blobs(prefix=f"{source_prefix}/")

    for blob in blobs:
        # Check if the blob ends with .parquet
        if blob.name.endswith(source_extension):
            destination_blob_name = f"{area}/{source_prefix}/import_date={import_date}/{os.path.basename(blob.name)}"
            new_blob = bucket_b.blob(destination_blob_name)
            new_blob.rewrite(blob)

            print(f'Copied {blob.name} to {new_blob.name}')
        print("\n")

def write_log(import_type="load"
              , source_bucket=None
              , destination_bucket=None
              , table_name=None
              , interface_name=None
              , list_log_file=None
              , number_log_file=0
              , total_log_record=0
              , number_processed_data_file=0
              , total_record_data_file=0
              , data_file_detail=None
              , is_keep_source_file=False
              , process_status=None
              , err_message=""):
    bq_client = bigquery.Client()
    insert_sql = f"""INSERT INTO `core_system.sys_ingest_logs` (
                    import_type
                    ,source_bucket
                    ,destination_bucket
                    ,table_name
                    ,interface_name
                    ,log_file
                    ,number_processed_data_file	
                    ,total_record_data_file	
                    ,data_file_detail		
                    ,is_keep_source_file	
                    ,process_status
                    ,err_message
                        ) 
                    VALUES ('{import_type}'
                    ,'{source_bucket}'
                    ,'{destination_bucket}'
                    ,'{table_name}'
                    ,'{interface_name}'
                    ,('{list_log_file}', {number_log_file}, {total_log_record})
                    ,{number_processed_data_file}
                    ,{total_record_data_file}
                    ,{data_file_detail}
                    ,{is_keep_source_file}
                    ,'{process_status}'
                    ,'{err_message}'
                    )"""
    # print(f"table_name: {table_name} , insert query: {insert_sql}")
    job = bq_client.query(insert_sql)
    result = job.result()
    return result

def archive_data_source(source_bucket_name, archive_bucket, source_prefix, status):
    gcs_client = storage.Client()
    source_bucket = gcs_client.bucket(source_bucket_name)
    archive_bucket = gcs_client.bucket(archive_bucket)
    import_date = datetime.now().strftime('%Y-%m-%d')
    blobs_main = source_bucket.list_blobs(prefix=f"{source_prefix}/")
    blobs_ctrl = source_bucket.list_blobs(prefix=f"{source_prefix}_ctrl/")
    blob_main_names = []
    blob_ctrl_names = []

    for blob in blobs_main:
        blob_main_names.append(blob.name)
        destination_blob_main = f"{source_prefix}/{import_date}/{status}/{blob.name}"
        print(f"destination_blob : {destination_blob_main}")
        source_bucket.copy_blob(blob, archive_bucket, destination_blob_main)

    for blob in blobs_ctrl:
        blob_ctrl_names.append(blob.name)
        destination_blob_ctrl = f"{source_prefix}/{import_date}/{status}/{blob.name}"
        print(f"destination_blob : {destination_blob_ctrl}")
        source_bucket.copy_blob(blob, archive_bucket, destination_blob_ctrl)
    
    return 0

def delete_blobs_in_folder(bucket_name, folder_prefix):
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)
    blobs_main = bucket.list_blobs(prefix=f"{folder_prefix}/")
    blobs_ctrl = bucket.list_blobs(prefix=f"{folder_prefix}_ctrl/")

    for blob in blobs_main:
        print(f"Deleting blob: {blob.name}")
        bucket.delete_blob(blob.name)

    for blob in blobs_ctrl:
        print(f"Deleting blob ctrl: {blob.name}")
        bucket.delete_blob(blob.name)
    
    return 0

def check_success_file(bucket_name, file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    data_file_name = f"{file_name}/_SUCCESS"
    ctrl_file_name = f"{file_name}_ctrl/_SUCCESS"
    blob = bucket.blob(data_file_name)
    blob_ctrl = bucket.blob(ctrl_file_name)
    alert_message = ''
    if blob.exists() and blob_ctrl.exists():
        return 0
    else:
        alert_message = f"The file '{data_file_name}' or ctrl_file '{ctrl_file_name}' does not found in bucket '{bucket_name}'.\n"
        print(alert_message)
        return 1
        