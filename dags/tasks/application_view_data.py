import pandas as pd
from io import BytesIO
from minio import Minio
from airflow.hooks.base import BaseHook

def application_view_data():
    """
    Processa o arquivo combinado na camada TRUSTED, remove colunas específicas e
    salva no bucket APPLICATION nos formatos CSV comprimido (gzip) e Parquet com compressão.
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
            print(f"[INFO] Bucket '{APPLICATION_BUCKET}' já existe. Limpando conteúdo...")
            objects = client.list_objects(APPLICATION_BUCKET, recursive=True)
            for obj in objects:
                client.remove_object(APPLICATION_BUCKET, obj.object_name)
            print(f"[INFO] Todo conteúdo do bucket '{APPLICATION_BUCKET}' foi deletado.")

        # Carregar o arquivo combinado da camada TRUSTED
        objects = list(client.list_objects(TRUSTED_BUCKET))
        if not objects:
            print("[WARNING] Nenhum arquivo encontrado na camada TRUSTED.")
            return

        # Procura o arquivo combinado Parquet
        for obj in objects:
            if obj.object_name.endswith("combined_data_trusted.parquet"):
                print(f"[INFO] Processando o arquivo {obj.object_name}...")
                response = client.get_object(TRUSTED_BUCKET, obj.object_name)
                combined_df = pd.read_parquet(BytesIO(response.read()))

                # Remover colunas específicas
                columns_to_remove = ['country', 'municipality - state', 'feature_id', 'geocode']
                combined_df = combined_df.drop(columns=[col for col in columns_to_remove if col in combined_df.columns])

                # Salvar como CSV comprimido (gzip)
                print("[INFO] Salvando como CSV comprimido (gzip)...")
                csv_buffer = BytesIO()
                combined_df.to_csv(csv_buffer, index=False, compression='gzip')
                csv_buffer.seek(0)

                client.put_object(
                    APPLICATION_BUCKET,
                    "processed_data.csv.gz",
                    csv_buffer,
                    length=csv_buffer.getbuffer().nbytes,
                    content_type="application/gzip"
                )
                print("[SUCCESS] Arquivo 'processed_data.csv.gz' salvo com sucesso no bucket 'application'.")

                # Salvar como Parquet com compressão
                print("[INFO] Salvando como Parquet comprimido...")
                parquet_buffer = BytesIO()
                combined_df.to_parquet(parquet_buffer, index=False, compression='snappy')
                parquet_buffer.seek(0)

                client.put_object(
                    APPLICATION_BUCKET,
                    "processed_data.parquet",
                    parquet_buffer,
                    length=parquet_buffer.getbuffer().nbytes,
                    content_type="application/octet-stream"
                )
                print("[SUCCESS] Arquivo 'processed_data.parquet' salvo com sucesso no bucket 'application'.")

                return

        print("[WARNING] Arquivo combinado 'combined_data_trusted.parquet' não encontrado na camada TRUSTED.")

    except Exception as e:
        print(f"[ERROR] Erro geral: {e}")
        raise
