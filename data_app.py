# app.py
import streamlit as st
from google.cloud import storage
import pandas as pd
import pyarrow.parquet as pq
import io
import google.auth
import gcsfs

# Set your Google Cloud Storage credentials
# credentials_json = st.secrets["uni-trans-sa-dev@cdg-data-uni-dev.iam.gserviceaccount.com"]
# project_id = credentials_json["cdg-data-uni-dev"]

# default ADC
# credentials_json = google.auth.load_credentials_from_file(r"C:\Users\AQUE\workspaces\cdg-data-uni-dev-2efe7bf4f62f.json")
# project_id = "cdg-data-uni-dev"

credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])

def load_data_from_gcs(bucket_name, file_path, file_type):
    """Loads CSV, Excel, or Parquet data from Google Cloud Storage."""
    try:
        storage_client = storage.Client(project=project_id, credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)

        if file_type == "CSV":
            data_bytes = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(data_bytes))
            return df
        elif file_type == "Excel":
            data_bytes = blob.download_as_bytes()
            df = pd.read_excel(io.BytesIO(data_bytes))
            return df
        elif file_type == "Parquet":
            if blob.exists(): #single file
                data_bytes = blob.download_as_bytes()
                table = pq.read_table(io.BytesIO(data_bytes))
                df = table.to_pandas()
                return df
            else: #assume directory (partitioned data)
                blobs = storage_client.list_blobs(bucket_name, prefix=file_path)
                file_paths = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith(".parquet")]

                if not file_paths:
                    st.error("No Parquet files found in the specified path.")
                    return None

                # Create gcsfs filesystem.
                fs = gcsfs.GCSFileSystem(project=project_id, token=credentials)

                table = pq.read_table(file_paths, filesystem=fs)
                df = table.to_pandas()
                return df
        else:
            st.error("Unsupported file type.")
            return None

    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None

def count_transactions_per_day(df, date_column='date', transaction_column='transaction_id'):
    """Counts transactions per day from a DataFrame."""
    try:
        df[date_column] = pd.to_datetime(df[date_column])
        daily_counts = df.groupby(df[date_column].dt.date)[transaction_column].count().reset_index()
        daily_counts.columns = ['date', 'transaction_count']
        return daily_counts
    except KeyError:
        st.error(f"Error: Date column '{date_column}' or Transaction column '{transaction_column}' not found.")
        return None
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None
    
def main():
    st.title("Google Cloud Storage Data Viewer/Merger")

    bucket_name = st.text_input("Enter your Google Cloud Storage bucket name:")
    file_type = st.selectbox("Select file type:", ["CSV", "Excel", "Parquet"])

    file_path = st.text_input(f"Enter the {file_type} file path:")

    if st.button("Load Data"):
        if bucket_name and file_path:
            df = load_data_from_gcs(bucket_name, file_path, file_type)
            if df is not None:
                st.write("Data Preview(100):")
                st.dataframe(df.head(100))

                # if st.checkbox("Show column descriptions (if available)"):
                #     try:
                #         st.write(df.describe())
                #     except:
                #         st.write("Column Description unavailable")
                # if st.checkbox("Show raw data"):
                #     st.write(df.to_dict())

                # # Calculate Daily Counts
                # date_col = st.selectbox("Select date column:", df.columns, index=list(df.columns).index('date') if 'date' in df.columns else 0)
                # transaction_col = st.selectbox("Select transaction ID column:", df.columns, index=list(df.columns).index('transaction_id') if 'transaction_id' in df.columns else 0)

                # if st.button("Calculate Daily Counts"):
                #     daily_counts = count_transactions_per_day(df.copy(), date_col, transaction_col)

                #     if daily_counts is not None:
                #         st.write("Daily Transaction Counts:")
                #         st.dataframe(daily_counts)

                #         st.line_chart(daily_counts.set_index('date'))
                        
        else:
            st.warning("Please enter both bucket name and file path.")

        

if __name__ == "__main__":
    main()

    