import pandas as pd
from io import BytesIO
from minio import Minio
from airflow.hooks.base import BaseHook

def trusted_refine_data():
    """
    Combina arquivos Parquet do bucket 'staging' em um único arquivo Parquet comprimido
    no bucket 'trusted'. Inclui normalização e padronização.
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

        STAGING_BUCKET = "staging"
        TRUSTED_BUCKET = "trusted"

        # Verifica se o bucket TRUSTED existe
        if not client.bucket_exists(TRUSTED_BUCKET):
            client.make_bucket(TRUSTED_BUCKET)
            print(f"[INFO] Bucket '{TRUSTED_BUCKET}' criado com sucesso.")
        else:
            print(f"[INFO] Bucket '{TRUSTED_BUCKET}' já existe. Limpando conteúdo...")
            objects = client.list_objects(TRUSTED_BUCKET, recursive=True)
            for obj in objects:
                client.remove_object(TRUSTED_BUCKET, obj.object_name)
            print(f"[INFO] Todo conteúdo do bucket '{TRUSTED_BUCKET}' foi deletado.")

        # Lista para armazenar todos os DataFrames
        all_dfs = []

        # Processar apenas arquivos Parquet do bucket 'staging'
        objects = client.list_objects(STAGING_BUCKET)
        for obj in objects:
            try:
                if obj.object_name.endswith('.parquet'):
                    print(f"[INFO] Processando arquivo Parquet {obj.object_name}...")
                    response = client.get_object(STAGING_BUCKET, obj.object_name)
                    df = pd.read_parquet(BytesIO(response.read()))
                    all_dfs.append(df)
                    print(f"[SUCCESS] Arquivo {obj.object_name} carregado com sucesso.")
                else:
                    print(f"[WARNING] Arquivo {obj.object_name} ignorado (não é Parquet).")
            except Exception as e:
                print(f"[ERROR] Erro ao processar o arquivo {obj.object_name}: {e}")

        # Combina todos os DataFrames
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)

            # Normalização e Padronização
            print("[INFO] Normalizando dados...")
            combined_df.columns = [col.lower().replace('.', '_') for col in combined_df.columns]
            combined_df.fillna("Desconhecido", inplace=True)

            # Salva como Parquet comprimido no bucket TRUSTED
            print("[INFO] Salvando arquivo combinado em formato Parquet comprimido...")
            parquet_buffer = BytesIO()
            combined_df.to_parquet(parquet_buffer, index=False, compression="snappy")  # Compressão com Snappy
            parquet_buffer.seek(0)
            client.put_object(
                TRUSTED_BUCKET,
                "combined_data_trusted.parquet",
                parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            print("[SUCCESS] Arquivo combinado salvo como Parquet comprimido no bucket 'trusted'.")
        else:
            print("[WARNING] Nenhum arquivo Parquet válido encontrado para processamento.")

    except Exception as e:
        print(f"[ERROR] Erro geral: {e}")
        raise
