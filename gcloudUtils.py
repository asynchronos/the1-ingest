import os
import pandas as pd
import google.auth
import google.cloud.storage as gcs
import google.cloud.bigquery as gbq
import google.cloud.bigquery.table as tbl
import google.oauth2.service_account as serv

# # https://developers.google.com/identity/protocols/oauth2/scopes
# credentials = serv.Credentials.from_service_account_file(
#     os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
#     , scopes=["https://www.googleapis.com/auth/bigquery"
#               , "https://www.googleapis.com/auth/devstorage.full_control"])

# default ADC
credentials = google.auth.default()

def gcs_upload_from_string(bucketName, destinationBlobName, content) -> bool:
    raise NotImplementedError()

def gbq_job_query(sqlText) -> (tbl.RowIterator|tbl._EmptyRowIterator):
    client = gbq.Client(credentials=credentials)

    jobs = client.query(sqlText)
    result = jobs.result()

    return result

def gbq_job_query_as_dataframe(sqlText) -> pd.DataFrame:
    rows = gbq_job_query(sqlText)
    result = rows.to_dataframe()
    
    return result

def gbq_job_query_as_dictList(sqlText) -> list[dict]:
    df = gbq_job_query_as_dataframe(sqlText)
    result = df.to_dict(orient='records')
    
    return result