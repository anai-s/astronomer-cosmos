#!/bin/bash

set -e
set -x

AIRFLOW_HOME="$PWD/scripts/airflow3"
export AIRFLOW_HOME
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$AIRFLOW_HOME/logs"
export AIRFLOW__WEBSERVER__CONFIG_FILE="$AIRFLOW_HOME/webserver_config.py"
export AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY="$AIRFLOW_HOME/logs/scheduler"
# Comment below line to use the Postgres database backend.
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
# Uncomment below line to use the Postgres database backend.
# export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@localhost:5432/airflow_db
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__DAGBAG_IMPORT_ERROR_TRACEBACK_DEPTH=10
export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300
# export AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
