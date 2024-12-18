import pandas as pd
from io import BytesIO
from minio import Minio
from airflow.hooks.base import BaseHook

def application_view_data():
    """
    Converte arquivos Parquet específicos da camada TRUSTED para CSV comprimido (gzip)
    e salva no bucket APPLICATION.
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

        TRUSTED_BUCKET = "trusted"
        APPLICATION_BUCKET = "application"

        # Verifica se o bucket APPLICATION existe
        if not client.bucket_exists(APPLICATION_BUCKET):
            client.make_bucket(APPLICATION_BUCKET)
            print(f"[INFO] Bucket '{APPLICATION_BUCKET}' criado com sucesso.")
        else:
            print(f"[INFO] Bucket '{APPLICATION_BUCKET}' já existe.")

        # Lista os arquivos específicos para processar
        files_to_process = ["mapbiomas_combined_trusted.parquet", "other_data_combined_trusted.parquet"]

        for file_name in files_to_process:
            try:
                print(f"[INFO] Processando arquivo: {file_name}...")
                # Baixa o arquivo Parquet
                response = client.get_object(TRUSTED_BUCKET, file_name)
                df = pd.read_parquet(BytesIO(response.read()))

                # Converte para CSV comprimido (gzip)
                gzip_buffer = BytesIO()
                df.to_csv(gzip_buffer, index=False, compression="gzip")
                gzip_buffer.seek(0)

                # Gera o nome do arquivo GZIP
                gzip_file_name = file_name.replace(".parquet", ".csv.gz")

                # Salva no MinIO
                client.put_object(
                    APPLICATION_BUCKET,
                    gzip_file_name,
                    gzip_buffer,
                    length=gzip_buffer.getbuffer().nbytes,
                    content_type="application/gzip"
                )
                print(f"[SUCCESS] Arquivo '{gzip_file_name}' salvo com sucesso no bucket '{APPLICATION_BUCKET}'.")

            except Exception as e:
                print(f"[ERROR] Erro ao processar o arquivo {file_name}: {e}")

    except Exception as e:
        print(f"[ERROR] Erro geral: {e}")
        raise
