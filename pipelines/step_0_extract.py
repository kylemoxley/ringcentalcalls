# Databricks notebook source
import mlflow
#from your_project_name.constants import CONSTANTS

# Set database
DATABASE = CONSTANTS['global']['Database']

# Expierment name
EXPERIMENT_NAME = CONSTANTS['global']['mlflow']
DEV_STAGE = CONSTANTS['global']['dev_stage']
PRIMARY_MODEL_TYPE = CONSTANTS['global']['primary_model_type']
PRIMARY_DEPT = CONSTANTS['global']['primary_dept']
ML_ENGINEER = CONSTANTS['global']['ml_engineer']
DATA_SCIENTIST = CONSTANTS['global']['data_scientist']

# Save Datasets
SOURCE_SCRIPT = "<your script name>"#dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]

if __name__ == "__main__":

    mlflow.set_experiment(EXPERIMENT_NAME)
    with mlflow.start_run(run_name=SOURCE_SCRIPT):
        pass
        #<your code>

    mlflow.log_param("dev_stage", DEV_STAGE)
    mlflow.log_param("primary_model_type", PRIMARY_MODEL_TYPE)
    mlflow.log_param("primary_dept", PRIMARY_DEPT)
    mlflow.log_param("ml_engineer", ML_ENGINEER)
    mlflow.log_param("data_scientist", DATA_SCIENTIST)
