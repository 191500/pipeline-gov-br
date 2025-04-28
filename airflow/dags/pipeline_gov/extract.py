from airflow.decorators import dag, task
from airflow.models import Variable

from minio import Minio
from minio.error import S3Error

import requests
from requests.exceptions import ConnectionError

import zipfile
import io
from datetime import datetime

PIPELINE_ENEM_URL = Variable.get("PIPELINE_ENEM_URL")
MINIO_ACESS_KEY = Variable.get("MINIO_ACESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")

@dag(
    dag_id="extract_enem_data",
    # schedule="*/10 * * * *",
    schedule=None,
    start_date=datetime(2025, 3, 23),
    catchup=False
)
def main():

    @task(task_id="get_latest_year")
    def get_latest_year_in_bucket()->str:
        """return the latest exame year in buckect"""

        client = Minio(
            endpoint="minio:9000",
            access_key=MINIO_ACESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        bucketName = "raw-data-enem"
        if not client.bucket_exists(bucketName):
            client.make_bucket(bucketName)
            return "1998"

        listObjs = client.list_objects(bucket_name=bucketName)

        years = []
        for i in listObjs:
            nameObj = i.object_name
            years.append(nameObj.split(".")[0].split("_")[2])

        if (years):
            return str(int(years[-1]) + 1)
        return "1998"

    @task(task_id="extract")
    def extract_from_gov(year: str)->None:
        while True:
            try:
                response = requests.get(url=PIPELINE_ENEM_URL.replace("year", year), stream=True) 
                response.raise_for_status()
                break
            except ConnectionError as error:
                print("Connection Error", error)

        arqZip = zipfile.ZipFile(io.BytesIO(response.content))
        
        for nameFile in arqZip.namelist():
            if nameFile.endswith(f"microdados_enem_{year}.csv"):
                break
        
        client = Minio(
            endpoint="minio:9000",
            access_key=MINIO_ACESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        with arqZip.open(nameFile) as csv_file:
            client.put_object(
                bucket_name="raw-data-enem",
                object_name=f"microdados_enem_{year}.csv",
                data=csv_file,
                length=-1,
                part_size=10*1024*1024, 
                content_type="text/csv",
                metadata={"status": "unprocessed"}
            ) 

    extract_from_gov(get_latest_year_in_bucket())
    
main()