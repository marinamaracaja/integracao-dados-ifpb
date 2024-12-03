def trusted_refine_data():
    from minio import Minio
    import pandas as pd
    from io import BytesIO
    from airflow.hooks.base import BaseHook

    minio_connection = BaseHook.get_connection('minio')
    host = f"{minio_connection.host}:{minio_connection.port}"
    client = Minio(host, secure=False, access_key=minio_connection.login,
                   secret_key=minio_connection.password)

    STAGING_BUCKET = "staging"
    TRUSTED_BUCKET = "trusted"

    # Criar bucket TRUSTED se não existir
    if not client.bucket_exists(TRUSTED_BUCKET):
        client.make_bucket(TRUSTED_BUCKET)
        print(f"Bucket '{TRUSTED_BUCKET}' criado com sucesso.")
    else:
        print(f"Bucket '{TRUSTED_BUCKET}' já existe. Limpando conteúdo...")
        # Lista os objetos no bucket e exclui todos
        objects = client.list_objects(TRUSTED_BUCKET, recursive=True)
        for obj in objects:
            client.remove_object(TRUSTED_BUCKET, obj.object_name)
        print(f"Todo conteúdo do bucket '{TRUSTED_BUCKET}' foi deletado.")

    # Listar objetos no bucket STAGING
    objects = client.list_objects(STAGING_BUCKET)
    for obj in objects:
        try:
            # Carregar o arquivo Parquet do MinIO
            response = client.get_object(STAGING_BUCKET, obj.object_name)
            df = pd.read_parquet(BytesIO(response.data))

            # Listando as colunas a serem mantidas
            colunas_necessarias = [
                'data.aqi', 'data.dominentpol',
                'data.iaqi.pm10.v', 'data.iaqi.pm25.v',
                'data.iaqi.no2.v', 'data.iaqi.co.v',
                'data.iaqi.so2.v', 'data.iaqi.o3.v',
                'data.iaqi.t.v', 'data.iaqi.h.v',
                'data.iaqi.w.v', 'data.time.iso',
                'data.city.name', 'data.city.geo'
            ]

            # filtando para ficar apenas as colunas necessárias
            for coluna in df.columns:
                if coluna not in colunas_necessarias:
                    df = df.drop(columns=[coluna])

            # Filtrar dados
            # df = df[df['nome_da_coluna'] > 0]

            # Salvando o arquivo refinado como Parquet no bucket TRUSTED
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            output_filename = obj.object_name.replace(
                '.parquet', '_trusted.parquet')
            client.put_object(
                TRUSTED_BUCKET,
                output_filename,
                parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            print(f"Arquivo {output_filename} salvo com sucesso no bucket '{
                  TRUSTED_BUCKET}'.")

        except Exception as e:
            print(f"Erro ao processar o arquivo {obj.object_name}: {e}")