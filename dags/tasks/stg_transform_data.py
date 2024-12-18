import pandas as pd
import gzip
from io import BytesIO
from minio import Minio
from airflow.hooks.base import BaseHook

def stg_transform_data():
    """
    Processa arquivos GZIP do bucket 'raw', transforma em formato tabular (CSV e Parquet),
    com suporte para JSON em formato array.
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

        # Processar arquivos do bucket 'raw'
        objects = list(client.list_objects(RAW_BUCKET))
        if not objects:
            print(f"[WARNING] Nenhum arquivo encontrado no bucket '{RAW_BUCKET}'.")
            return

        for obj in objects:
            try:
                print(f"[INFO] Carregando arquivo GZIP {obj.object_name} do bucket '{RAW_BUCKET}'...")
                response = client.get_object(RAW_BUCKET, obj.object_name)

                with response as data:
                    with gzip.GzipFile(fileobj=BytesIO(data.read())) as gz:
                        print(f"[INFO] Lendo e transformando arquivo GZIP em formato tabular...")

                        # Carregar o JSON como um array completo
                        json_content = pd.read_json(gz)  # Remove lines=True para array

                        # Salvar o arquivo como CSV
                        print(f"[INFO] Salvando dados em formato CSV...")
                        csv_buffer = BytesIO()
                        json_content.to_csv(csv_buffer, index=False)
                        csv_buffer.seek(0)

                        # Nome do arquivo CSV no bucket 'staging'
                        csv_output_filename = obj.object_name.replace('.json.gz', '.csv')

                        # Enviar CSV para MinIO
                        client.put_object(
                            STAGING_BUCKET,
                            csv_output_filename,
                            csv_buffer,
                            length=csv_buffer.getbuffer().nbytes,
                            content_type='text/csv'
                        )
                        print(f"[SUCCESS] Arquivo CSV salvo: {csv_output_filename}")

                        # Salvar o arquivo como Parquet
                        print(f"[INFO] Salvando dados em formato Parquet...")
                        parquet_buffer = BytesIO()
                        json_content.to_parquet(parquet_buffer, index=False)
                        parquet_buffer.seek(0)

                        # Nome do arquivo Parquet no bucket 'staging'
                        parquet_output_filename = obj.object_name.replace('.json.gz', '.parquet')

                        # Enviar Parquet para MinIO
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
        print(f"[ERROR] Ocorreu um erro geral: {e}")
        raise
