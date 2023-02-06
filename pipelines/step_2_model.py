# Databricks notebook source
import mlflow
#from your_project_name.constants import CONSTANTS

# Set database
DATABASE = CONSTANTS['global']['Database']

# Expierment name
EXPERIMENT_NAME = CONSTANTS['global']['mlflow']

# Save Datasets
SOURCE_SCRIPT = "<your script name>"#dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]

if __name__ == "__main__":

    mlflow.set_experiment(EXPERIMENT_NAME)
    with mlflow.start_run(run_name=SOURCE_SCRIPT):
        pass
        #<your code>
