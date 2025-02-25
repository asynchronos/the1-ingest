# CDG-DATA-UNIFICATION-DOFF

## Quick Start

### Setup Python Environment
```console
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### streamlit
```
streamlit run data_app.py
```

### Install and initialize the gcloud CLI
Please setup following this document 
- https://cloud.google.com/sdk/docs/install
- https://cloud.google.com/sdk/docs/initializing

### Setup Application Default Credentials

## Appendix
### Setup git commit user.name and user.email
```console
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```

### Get the size of all objects within a specific bucket:
```
gsutil du -s gs://your-bucket-name
```

### get the size of objects within a specific directory/prefix inside a bucket:
```
gsutil du -s gs://your-bucket-name/your-directory/
```