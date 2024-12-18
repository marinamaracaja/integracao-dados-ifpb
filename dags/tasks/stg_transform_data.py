import pandas as pd
import gzip
from io import BytesIO
from minio import Minio
from airflow.hooks.base import BaseHook

def stg_transform_data():
    """
    Processa arquivos JSON e GZIP no bucket 'raw', converte para Parquet e CSV,
    e salva os resultados no bucket 'staging' no MinIO.
    """
    try:
        print("[INFO] Conectando ao MinIO...")
        # Conexão com MinIO
        minio_connection = BaseHook.get_connection('minio')
        host = f"{minio_connection.host}:{minio_connection.port}"
        client = Minio(
            host,
            secure=False,
            access_key=minio_connection.login,
            secret_key=minio_connection.password
        )

        RAW_BUCKET = "raw"
        STAGING_BUCKET = "staging"

        # Verifica se o bucket 'staging' existe
        if not client.bucket_exists(STAGING_BUCKET):
            client.make_bucket(STAGING_BUCKET)
            print(f"[INFO] Bucket '{STAGING_BUCKET}' criado com sucesso.")
        else:
            print(f"[INFO] Bucket '{STAGING_BUCKET}' já existe.")

        # Processar arquivos no bucket 'raw'
        objects = list(client.list_objects(RAW_BUCKET))
        if not objects:
            print(f"[WARNING] Nenhum arquivo encontrado no bucket '{RAW_BUCKET}'.")
            return

        for obj in objects:
            try:
                print(f"[INFO] Carregando arquivo: {obj.object_name}")
                response = client.get_object(RAW_BUCKET, obj.object_name)

                # Verifica o tipo de arquivo
                if obj.object_name.endswith('.json'):
                    print("[INFO] Processando arquivo JSON...")
                    json_content = pd.read_json(response)
                elif obj.object_name.endswith('.json.gz'):
                    print("[INFO] Processando arquivo GZIP JSON...")
                    with gzip.GzipFile(fileobj=BytesIO(response.read())) as gz:
                        json_content = pd.read_json(gz)
                else:
                    print(f"[WARNING] Tipo de arquivo não suportado: {obj.object_name}")
                    continue

                # Salvar como CSV
                csv_buffer = BytesIO()
                json_content.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                csv_output_filename = obj.object_name.replace('.json.gz', '.csv').replace('.json', '.csv')
                client.put_object(
                    STAGING_BUCKET,
                    csv_output_filename,
                    csv_buffer,
                    length=csv_buffer.getbuffer().nbytes,
                    content_type='text/csv'
                )
                print(f"[SUCCESS] Arquivo CSV salvo: {csv_output_filename}")

                # Salvar como Parquet
                parquet_buffer = BytesIO()
                json_content.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)
                parquet_output_filename = obj.object_name.replace('.json.gz', '.parquet').replace('.json', '.parquet')
                client.put_object(
                    STAGING_BUCKET,
                    parquet_output_filename,
                    parquet_buffer,
                    length=parquet_buffer.getbuffer().nbytes,
                    content_type='application/octet-stream'
                )
                print(f"[SUCCESS] Arquivo Parquet salvo: {parquet_output_filename}")

            except Exception as e:
                print(f"[ERROR] Erro ao processar o arquivo {obj.object_name}: {e}")

    except Exception as e:
        print(f"[ERROR] Erro geral no processamento: {e}")
        raise
