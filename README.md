# CDG-DATA-UNIFICATION-DOFF

## Quick Start

### Setup Python Environment
```console
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Install and initialize the gcloud CLI
Please setup following this document 
- https://cloud.google.com/sdk/docs/install
- https://cloud.google.com/sdk/docs/initializing

### Setup Application Default Credentials
- Create file .env from .env_template
- Set keyfile.json path to **GOOGLE_APPLICATION_CREDENTIALS**
```console
GOOGLE_APPLICATION_CREDENTIALS=D:\sa\keyfile.json
```

### Test your function locally
1. Run your function locally with the Functions Framework:

Syntax
```console
functions-framework-python --source ./PATH/TO/PYTHON/MAIN.PY --target METHOD_NAME
```
Example
```console
functions-framework-python --source ./extract_gdrive.py --target start
```
2. Test your function by visiting http://localhost:8080 in a browser or by running curl localhost:8080 from another window.
- windows command
```console
curl -i -X  POST -H "Content-Type: application/json" -d "{\"area\": [\"budget_target\"]}" http://localhost:8080
```
- bash command
```console
curl -i \
    -X  POST \
    -H "Content-Type: application/json" \
    -d '{"area": ["budget_target"]}' \
    http://localhost:8080
```

**Reference: https://cloud.google.com/functions/docs/running/function-frameworks**


## Appendix
### Setup git commit user.name and user.email
```console
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```