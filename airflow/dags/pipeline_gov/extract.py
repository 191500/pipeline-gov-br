from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.models import Variable

from minio.commonconfig import Tags
from minio import Minio

from requests.exceptions import ConnectionError
import requests

import pyarrow.parquet as pq
import pyarrow as pa

from io import BytesIO, StringIO
import io

from datetime import datetime
import pandas as pd
import numpy as np
import zipfile

PIPELINE_ENEM_URL = Variable.get("PIPELINE_ENEM_URL")

MINIO_URL = Variable.get("MINIO_URL")
MINIO_ACESS_KEY = Variable.get("MINIO_ACESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")
MINIO_RAW_BUCKET_NAME = Variable.get("MINIO_RAW_BUCKET_NAME")
MINIO_SILVER_BUCKET_NAME = Variable.get("MINIO_SILVER_BUCKET_NAME")

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

        client = Minio(endpoint=MINIO_URL, access_key=MINIO_ACESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

        if not client.bucket_exists(MINIO_RAW_BUCKET_NAME):
            client.make_bucket(MINIO_RAW_BUCKET_NAME)
            return "1998"

        listObjs = client.list_objects(bucket_name=MINIO_RAW_BUCKET_NAME)

        years = []
        for i in listObjs:
            nameObj = i.object_name
            years.append(nameObj.split(".")[0].split("_")[2])

        if (years):
            return str(int(years[-1]) + 1)
        return "1998"

    @task(task_id="extract_from_gov_to_csv")
    def extract_from_gov(year: str)->str:
        for index in [1, 2]:
            try:
                response = requests.get(url=PIPELINE_ENEM_URL.replace("year", year), stream=True) 
                response.raise_for_status()
                break
            except ConnectionError as error:
                if index==2:
                    raise Exception

        arqZip = zipfile.ZipFile(io.BytesIO(response.content))

        nameFile = f"MICRODADOS_ENEM_{year}.csv"
        for nameFilesInZip in arqZip.namelist():
            if nameFilesInZip.endswith(nameFile) or nameFilesInZip.endswith(nameFile.lower()) :
                break
        
        client = Minio(endpoint=MINIO_URL, access_key=MINIO_ACESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

        tagsExtract = Tags(for_object=True)
        tagsExtract["upload_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tagsExtract["status"] = "unprocessed"
        tagsExtract["category"] = "enem"
        tagsExtract["year"] = year

        with arqZip.open(nameFilesInZip) as csv_file:
            client.put_object(
                bucket_name=MINIO_RAW_BUCKET_NAME,
                object_name=f"microdados_enem_{year}.csv",
                data=csv_file,
                length=-1,
                part_size=10*1024*1024, 
                content_type="text/csv",
                tags=tagsExtract
            )

        return year

    @task(task_id="transform_csv_in_parquet")
    def transform_csv_in_parquet(year: str)->str:
        client = Minio(endpoint=MINIO_URL, access_key=MINIO_ACESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

        response = client.get_object(
            bucket_name=MINIO_RAW_BUCKET_NAME,
            object_name=[file.object_name for file in client.list_objects(bucket_name=MINIO_RAW_BUCKET_NAME) if year in file.object_name][0]
        )

        parquet_buffer = BytesIO()
        writer = None
        colunas = None

        try:
            while True:
                dfChunk = response.read(1024 * 1024 * 30)
                if not dfChunk:
                    break
                
                bytesToStr = dfChunk.decode('utf-8')

                if colunas is None:
                    df = pd.read_csv(StringIO(bytesToStr), delimiter=";", encoding="latin1")
                    colunas = df.columns
                else:
                    df = pd.read_csv(StringIO(bytesToStr), delimiter=";", header=None, names=colunas, skiprows=1)

                notesCols = ['NU_NOTA_COMP1', 'NU_NOTA_COMP2', 'NU_NOTA_COMP3', 'NU_NOTA_COMP4', 'NU_NOTA_COMP5', 'NU_NOTA_REDACAO']

                for notes in notesCols:
                    df[notes] = df[notes].astype('float64')

                colsToConvertInStr = [
                    col for col in df.columns
                    if col not in notesCols and df[col].dtype != "object" and df[col].dtype != "string"
                ]

                if colsToConvertInStr:
                    df[colsToConvertInStr] = df[colsToConvertInStr].astype(str)

                table = pa.Table.from_pandas(df)
                
                if writer is None:
                    writer = pq.ParquetWriter(parquet_buffer, table.schema)
                writer.write_table(table)

        except Exception as error:
            print("ERRO", error)
        finally:
            response.close()
            response.release_conn()

            if writer:
                writer.close()

            if not client.bucket_exists(MINIO_SILVER_BUCKET_NAME):
                client.make_bucket(MINIO_SILVER_BUCKET_NAME)

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            tagsParquet = Tags(for_object=True)
            tagsParquet["category"] = "enem"
            tagsParquet["compressed_at"] = now 
            tagsParquet["status"] = "compressed"
            tagsParquet["year"] = year

            parquet_buffer.seek(0)
            client.put_object(
                bucket_name=MINIO_SILVER_BUCKET_NAME,
                object_name=f"microdados_enem_{year}.parquet",
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type="application/octet-stream",
                tags=tagsParquet
            )

            tagsExtract = client.get_object_tags(
                bucket_name=MINIO_RAW_BUCKET_NAME,
                object_name=f"microdados_enem_{year}.csv"
            )

            tagsExtract["status"] = "processed"
            tagsExtract["compressed_at"] = now

            client.set_object_tags(
                bucket_name=MINIO_RAW_BUCKET_NAME,
                object_name=f"microdados_enem_{year}.csv",
                tags=tagsExtract
            )

            return year

    @task(task_id="send_parquet_to_bd")
    def send_parquet_to_bd(year: str)->None:
        pgHook = PostgresHook(postgres_conn_id="DB_ETL")
        engine = pgHook.get_sqlalchemy_engine()

        COLS_BD = [
                "NU_INSCRICAO", "NU_ANO", "TP_FAIXA_ETARIA", "TP_SEXO", "TP_ESTADO_CIVIL", "TP_COR_RACA", "TP_NACIONALIDADE", "TP_ST_CONCLUSAO",
                "TP_ANO_CONCLUIU", "TP_ESCOLA", "TP_ENSINO", "IN_TREINEIRO", "CO_MUNICIPIO_ESC", "NO_MUNICIPIO_ESC", "CO_UF_ESC", "SG_UF_ESC", "TP_DEPENDENCIA_ADM_ESC",
                "TP_LOCALIZACAO_ESC", "TP_SIT_FUNC_ESC", "CO_MUNICIPIO_PROVA",  "NO_MUNICIPIO_PROVA", "CO_UF_PROVA", "SG_UF_PROVA", "TP_PRESENCA_CN",
                "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT", "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "TP_STATUS_REDACAO",
                "NU_NOTA_COMP1", "NU_NOTA_COMP2", "NU_NOTA_COMP3", "NU_NOTA_COMP4", "NU_NOTA_COMP5", "NU_NOTA_REDACAO" 
        ]

        client = Minio(endpoint=MINIO_URL,access_key=MINIO_ACESS_KEY,secret_key=MINIO_SECRET_KEY,secure=False)

        objectName = [file.object_name for file in client.list_objects(bucket_name=MINIO_SILVER_BUCKET_NAME) if year in file.object_name][0]
        response = client.get_object(bucket_name=MINIO_SILVER_BUCKET_NAME, object_name=objectName)

        stream = io.BytesIO()
        for chunk in response.stream(200 * 1024 * 1024):
            stream.write(chunk)
        stream.seek(0)

        parquetFile = pq.ParquetFile(stream)

        colsInParquet = parquetFile.schema.names

        rowsGroupParquet:int = parquetFile.num_row_groups

        tagsParquet = client.get_object_tags(bucket_name=MINIO_SILVER_BUCKET_NAME, object_name=f"microdados_enem_{year}.parquet")
        
        rowsGroupsInStg = tagsParquet.get("row_group_in_stg") or -1
        rowsGroupsInStg = int(rowsGroupsInStg)

        if (rowsGroupsInStg==rowsGroupParquet):
            return

        colsToAdd = list(set(COLS_BD) - set(colsInParquet))
        colsToGet = list(set(COLS_BD) & set(colsInParquet))

        def read_and_set_tag(index:int, tags:Tags):
            tags["row_group_in_stg"] = str(index)

            client.set_object_tags(
                bucket_name="silver-data-enem",
                object_name=f"microdados_enem_{year}.parquet",
                tags=tags
            )
        
        for indexRowGroup in range(rowsGroupsInStg+1, rowsGroupParquet):
            table = parquetFile.read_row_group(indexRowGroup, columns=colsToGet)
            
            df:pd.DataFrame = table.to_pandas()

            for col in colsToAdd: df[col] = np.nan
            
            df = df[COLS_BD]

            for col in COLS_BD:
                df = df.rename(columns={col: col.lower()})

            try:
                df.to_sql(name="stg_microdados_enem", con=engine, if_exists="append", index=False)
            except Exception as erro:
                read_and_set_tag(index=indexRowGroup-1, tags=tagsParquet)
        
        read_and_set_tag(index=rowsGroupParquet, tags=tagsParquet)

    send_parquet_to_bd(transform_csv_in_parquet(extract_from_gov(get_latest_year_in_bucket())))
    
main()