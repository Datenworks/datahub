from common import run

# See https://github.com/mxmzdlv/pybigquery/ for more details
URL = 'bigquery://dev-semesp-datascience/stage' # e.g. bigquery://project_id
OPTIONS = {"credentials_path": "/home/richard/datenworks/projects/datahub/metadata-ingestion/sql-etl/account.json"} # e.g. {"credentials_path": "/path/to/keyfile.json"}
PLATFORM = 'bigquery'

run(URL, OPTIONS, PLATFORM)