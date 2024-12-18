import pandas as pd
from io import BytesIO
from minio import Minio
from airflow.hooks.base import BaseHook

def trusted_refine_data():
    """
    Combina arquivos Parquet do MapBiomas e outros arquivos em dois arquivos finais distintos,
    salvando no bucket 'trusted'. Inclui normalização e tratamento de valores inconsistentes.
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

        # Verifica ou cria o bucket TRUSTED
        if not client.bucket_exists(TRUSTED_BUCKET):
            client.make_bucket(TRUSTED_BUCKET)
            print(f"[INFO] Bucket '{TRUSTED_BUCKET}' criado com sucesso.")
        else:
            print(f"[INFO] Bucket '{TRUSTED_BUCKET}' já existe.")

        # Listas de DataFrames para MapBiomas e Outros
        mapbiomas_dfs = []
        other_dfs = []

        # Processar arquivos Parquet do bucket STAGING
        objects = client.list_objects(STAGING_BUCKET)
        for obj in objects:
            try:
                if obj.object_name.endswith('.parquet'):
                    print(f"[INFO] Processando arquivo Parquet: {obj.object_name}...")
                    response = client.get_object(STAGING_BUCKET, obj.object_name)
                    df = pd.read_parquet(BytesIO(response.read()))

                    # Normalizar dados problemáticos
                    for col in df.columns:
                        if df[col].apply(type).nunique() > 1:
                            print(f"[WARNING] Coluna '{col}' contém dados mistos. Convertendo para string.")
                            df[col] = df[col].astype(str)

                    # Separar os DataFrames
                    if "mapbiomas" in obj.object_name.lower():
                        mapbiomas_dfs.append(df)
                    else:
                        other_dfs.append(df)

                    print(f"[SUCCESS] Arquivo {obj.object_name} carregado com sucesso.")
                else:
                    print(f"[WARNING] Arquivo {obj.object_name} ignorado (não é Parquet).")
            except Exception as e:
                print(f"[ERROR] Erro ao processar o arquivo {obj.object_name}: {e}")

        # Função para combinar, normalizar e salvar DataFrames
        def salvar_combined_parquet(dfs, output_name):
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)

                # Normalização de colunas
                combined_df.columns = [col.lower().replace('.', '_') for col in combined_df.columns]

                # Tratamento de valores inconsistentes
                for col in combined_df.columns:
                    if combined_df[col].apply(type).nunique() > 1:
                        print(f"[WARNING] Coluna '{col}' contém dados mistos. Convertendo para string.")
                        combined_df[col] = combined_df[col].astype(str)

                # Salvar como Parquet comprimido
                print(f"[INFO] Salvando arquivo combinado {output_name}...")
                parquet_buffer = BytesIO()
                combined_df.to_parquet(parquet_buffer, index=False, compression="snappy")
                parquet_buffer.seek(0)
                client.put_object(
                    TRUSTED_BUCKET,
                    output_name,
                    parquet_buffer,
                    length=parquet_buffer.getbuffer().nbytes,
                    content_type="application/octet-stream"
                )
                print(f"[SUCCESS] Arquivo salvo: {output_name}")
            else:
                print(f"[WARNING] Nenhum dado encontrado para {output_name}.")

        # Salvar arquivos combinados
        salvar_combined_parquet(mapbiomas_dfs, "mapbiomas_combined_trusted.parquet")
        salvar_combined_parquet(other_dfs, "other_data_combined_trusted.parquet")

    except Exception as e:
        print(f"[ERROR] Erro geral: {e}")
        raise
