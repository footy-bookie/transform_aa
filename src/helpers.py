import google.auth
import pandas as pd
import pandas_gbq
import requests
from google.cloud import storage
from pandas import DataFrame


def get_vm_custom_envs(meta_key: str):
    response = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/attributes/{}".format(meta_key),
        headers={'Metadata-Flavor': 'Google'},
    )

    data = response.text

    return data


def read_bigquery(dataset: str, table_name: str):
    credentials, project_id = google.auth.default()
    df = pandas_gbq.read_gbq('select * from `{}.{}.{}`'.format(project_id, dataset, table_name),
                             project_id=project_id,
                             credentials=credentials,
                             location='europe-west3')

    return df


def write_data(df: DataFrame):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(get_vm_custom_envs("SINK"))
    # bucket = storage_client.get_bucket('dev-footy_aa_sink_dev')

    csv_name = "aa-{}.csv".format(str(pd.Timestamp.now()))
    bucket.blob(csv_name).upload_from_string(df.to_csv(header=1, index=0), "text/csv")
