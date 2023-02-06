import glob
import pandas as pd
from datetime import datetime
import json
from azure.storage.blob import BlobServiceClient
import mlflow


def databricks_mx_dir(path: str, depth: int) -> dict:
    """
    This function consumes a path that assumes there is structured in a date hierachy of year, month, day.

    Args:
    path: Azure datalake path
    depth: Assuming the data is structured in year/month/day 1 = year, 2 = year/month, 3 = year/month/date

    Returns:
    A dictionary containing a dataframe and the max date file path.
    """

    assert depth > 0, "Please assign depth a value greater than 0. If you do not \
    need to search paths recursively, then standard glob.glob should work."

    new_path = None
    if path[len(path) - 1] == "/":
        new_path = path + depth * "*/"
        new_path = new_path[:-1]
    else:
        new_path = path + depth * "/*"

    li = []
    for name in glob.glob(new_path):
        li.append(name.replace(path, "")[1:])

    if len(li) > 1:
        output = pd.DataFrame({"Path": path, "Dates": li})
    else:
        output = pd.DataFrame({"Path": path, "Dates": li}, [0])
    
    output['Formatted_Date'] = pd.to_datetime(output['Dates'], infer_datetime_format=True)
    output = output.sort_values("Formatted_Date", ascending=False)

    # create the file path add append /* for the actual files within that path
    output['FilePath'] = output['Path'] + output['Dates'] + "/*"

    d = {'DataFrame': output, 'MaxFilePath': output.iloc[0]['FilePath']}

    return d

def getting_the_mlflow_logs(container_name: str, blob_name: str, azure_storage_connection_string: str):
    """
    This function reads the experiment history in Databricks and save it .

    Args:
    container_name: the container name whe the experiment history will be stored
    blob_name: The name that will take the json file with the history
    azure_storage_connection_string: the connection string to connect to the Azure Storage Account

    Returns:
    It uploads a json file with the experiment history of the databricks instance to an Azure Storage Account.
    """
    def uploading_de_json_data(container_name_sub: str, blob_name_sub: str, azure_storage_connection_string_sub: str):
        """
        This function updates.

        Args:
        container_name: the container name whe the experiment history will be stored
        blob_name: The name that will take the json file with the history
        azure_storage_connection_string: the connection string to connect to the Azure Storage Account

        Returns:
        It uploads a json file with the experiment history of the databricks instance to an Azure Storage Account.
        """
        with open(blob_name_sub, "w+") as json_file:
            json_file.write(json_object)
            json_file.close()

        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string_sub)

        try:
            blob_service_client.create_container(container_name_sub)
        except Exception:
            print(f"The {container_name_sub} container already exists, it won't be created again.")

        blob_client = blob_service_client.get_blob_client(container=container_name_sub, blob=blob_name_sub)
        with open(blob_name_sub, "rb") as data:
            blob_client.upload_blob(data=data, overwrite=True)

    def getting_data_from_runs(experiment_id_sub: str):
        info_for_runs = []

        for current_run_info in mlflow.list_run_infos(experiment_id_sub):
            info_for_current_run = {}

            current_run_id = current_run_info.run_id
            info_for_current_run["id"] = current_run_id

            current_run_details = mlflow.get_run(current_run_id)
            current_run = current_run_details.data

            current_metrics = current_run.metrics
            current_params = current_run.params
            current_tags = current_run.tags

            current_startdate = ''
            current_enddate = ''
            current_status = ''
            current_duration = ''
            if current_run_details.info.start_time is not None:
                current_startdate = datetime.fromtimestamp(float(current_run_details.info.start_time / 1000.0))
            if current_run_details.info.end_time is not None:
                current_enddate = datetime.fromtimestamp(float(current_run_details.info.end_time / 1000.0))
            if (current_run_details.info.start_time is not None) and (current_run_details.info.end_time is not None):
                current_duration = current_enddate - current_startdate
            current_status = current_run_details.info.status

            info_for_current_run['start_time'] = str(current_startdate)
            info_for_current_run['end_time'] = str(current_enddate)
            info_for_current_run['status'] = str(current_status)
            info_for_current_run['duration'] = str(current_duration)

            user_tags = {}
            for current_key_name in current_tags.keys():
                if not current_key_name.startswith("mlflow."):
                    user_tags[current_key_name] = current_tags[current_key_name]

            info_for_current_run['metrics'] = current_metrics
            info_for_current_run['parameters'] = current_params
            info_for_current_run['user_tags'] = user_tags

            info_for_runs.append(info_for_current_run)

        return info_for_runs

    experiment_logs = {'experiments': []}

    for current_experiment in mlflow.list_experiments():
        current_experiment_logs = {}

        experiment_id = current_experiment.experiment_id
        experiment_name = current_experiment.name.split("/")[-1]
        current_experiment_logs['name'] = experiment_name
        current_experiment_logs['id'] = experiment_id

        ownerEmail = ''
        if "mlflow.ownerEmail" in current_experiment.tags.keys():
            ownerEmail = current_experiment.tags["mlflow.ownerEmail"]

        current_experiment_logs['ownerEmail'] = ownerEmail

        info_for_runs = getting_data_from_runs(experiment_id)

        current_experiment_logs['runs'] = info_for_runs

        experiment_logs['experiments'].append(current_experiment_logs)

    json_object = json.dumps(experiment_logs, indent=4)
    uploading_de_json_data(container_name, blob_name, azure_storage_connection_string)

