def stg_transform_data():
    import pandas as pd
    import json
    from minio import Minio
    from airflow.hooks.base import BaseHook
    from io import BytesIO

    minio_connection = BaseHook.get_connection('minio')
    host = f"{minio_connection.host}:{minio_connection.port}"
    client = Minio(host, secure=False, access_key=minio_connection.login,
                   secret_key=minio_connection.password)

    RAW_BUCKET = "raw"
    STAGING_BUCKET = "staging"

    # Verifica se o bucket existe
    if not client.bucket_exists(STAGING_BUCKET):
        client.make_bucket(STAGING_BUCKET)
        print(f"Bucket '{STAGING_BUCKET}' criado com sucesso.")
    else:
        print(f"Bucket '{STAGING_BUCKET}' já existe. Limpando conteúdo...")
        # Lista os objetos no bucket e exclui todos
        objects = client.list_objects(STAGING_BUCKET, recursive=True)
        for obj in objects:
            client.remove_object(STAGING_BUCKET, obj.object_name)
        print(f"Todo conteúdo do bucket '{STAGING_BUCKET}' foi deletado.")
        
    # Listar objetos no bucket RAW
    objects = client.list_objects(RAW_BUCKET)
    for obj in objects:
        try:
            # Carregar o arquivo JSON do MinIO
            print("Carregando arquivos brutos")
            response = client.get_object(RAW_BUCKET, obj.object_name)
            with response as data:
                json_data = json.load(data)

            # Normalizar o JSON para um DataFrame do Pandas
            # Normaliza para estrutura tabular
            print("Normalizando json")
            df = pd.json_normalize(json_data)

            # Salvar como Parquet no bucket STAGING
            print("Salvando parquet no Bucket")
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            # Salvar o arquivo Parquet no MinIO
            output_filename = obj.object_name.replace('.json', '.parquet')
            client.put_object(
                STAGING_BUCKET,
                output_filename,
                parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            print(f"Arquivo {output_filename} salvo com sucesso no bucket '{
                  STAGING_BUCKET}'.")
        except Exception as e:
            print(f"Erro ao processar o arquivo {obj.object_name}: {e}")