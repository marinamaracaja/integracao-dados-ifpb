import pandas as pd
from minio import Minio
from io import BytesIO
from airflow.hooks.base import BaseHook


def application_view_data():
    minio_connection = BaseHook.get_connection('minio')
    host = f"{minio_connection.host}:{minio_connection.port}"
    client = Minio(host, secure=False, access_key=minio_connection.login,
                   secret_key=minio_connection.password)

    TRUSTED_BUCKET = "trusted"
    APPLICATION_BUCKET = "application"

    # Criar bucket APPLICATION se não existir
    if not client.bucket_exists(APPLICATION_BUCKET):
        client.make_bucket(APPLICATION_BUCKET)

    # Listar objetos no bucket TRUSTED
    objects = client.list_objects(TRUSTED_BUCKET)
    dataframes = []
    for obj in objects:
        try:
            # Carregar o arquivo Parquet do MinIO
            response = client.get_object(TRUSTED_BUCKET, obj.object_name)
            df = pd.read_parquet(BytesIO(response.data))

            # Adicionar ao DataFrame final
            dataframes.append(df)

        except Exception as e:
            print(f"Erro ao processar o arquivo {obj.object_name}: {e}")

    # Concatenar todos os DataFrames
    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)

        # Salvar o DataFrame final como Parquet no bucket APPLICATION
        parquet_buffer = BytesIO()
        final_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        client.put_object(
            APPLICATION_BUCKET,
            "final_data.parquet",
            parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        print("Dados combinados e salvos na camada APPLICATION.")
    else:
        print("Nenhum arquivo encontrado na camada TRUSTED.")


# Exemplo de chamada da função
application_view_data()
