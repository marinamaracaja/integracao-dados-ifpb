def trusted_refine_data():
    from minio import Minio
    import pandas as pd
    from io import BytesIO
    from airflow.hooks.base import BaseHook

    # Configuração da conexão com o MinIO
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

    # Criar bucket TRUSTED se não existir
    if not client.bucket_exists(TRUSTED_BUCKET):
        client.make_bucket(TRUSTED_BUCKET)
        print(f"Bucket '{TRUSTED_BUCKET}' criado com sucesso.")
    else:
        print(f"Bucket '{TRUSTED_BUCKET}' já existe. Limpando conteúdo...")
        # Excluir todos os objetos do bucket TRUSTED
        objects = client.list_objects(TRUSTED_BUCKET, recursive=True)
        for obj in objects:
            client.remove_object(TRUSTED_BUCKET, obj.object_name)
        print(f"Todo conteúdo do bucket '{TRUSTED_BUCKET}' foi deletado.")

    # Lista para armazenar todos os DataFrames
    all_dfs = []

    # Lista todos os objetos no bucket STAGING
    objects = client.list_objects(STAGING_BUCKET)
    for obj in objects:
        try:
            # Carregar o arquivo Parquet do MinIO
            response = client.get_object(STAGING_BUCKET, obj.object_name)
            df = pd.read_parquet(BytesIO(response.data))

            # Adicionar o DataFrame à lista
            all_dfs.append(df)
            print(f"Arquivo {obj.object_name} processado com sucesso.")
            print(f"Colunas no arquivo {obj.object_name}: {df.columns.tolist()}")

        except Exception as e:
            print(f"Erro ao processar o arquivo {obj.object_name}: {e}")

    # Combinar todos os DataFrames em um único DataFrame
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)

        # Normalização e Padronização
        print("Normalizando dados...")
        
        # Renomear colunas para um formato padrão
        combined_df.columns = [col.lower().replace('.', '_') for col in combined_df.columns]
        
        # Preencher valores ausentes
        combined_df.fillna({'state': 'Desconhecido', 'city': 'Desconhecido', 'biome': 'Desconhecido'}, inplace=True)
        
        # Converter datas para formato datetime
        if 'detectedat' in combined_df.columns:
            combined_df['detectedat'] = pd.to_datetime(combined_df['detectedat'], errors='coerce')
        if 'publishedat' in combined_df.columns:
            combined_df['publishedat'] = pd.to_datetime(combined_df['publishedat'], errors='coerce')

        # Verificar e remover colunas irrelevantes ou duplicadas
        combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]

        print(f"Total de registros combinados: {len(combined_df)}")
        print("Colunas após a normalização:")
        print(combined_df.columns.tolist())

        # Salvar como Parquet no bucket TRUSTED
        parquet_buffer = BytesIO()
        combined_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        client.put_object(
            TRUSTED_BUCKET,
            "combined_data_trusted.parquet",
            parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        print("Arquivo 'combined_data_trusted.parquet' salvo com sucesso no bucket 'trusted'.")

        # Salvar como Excel no bucket TRUSTED
        excel_buffer = BytesIO()
        combined_df.to_excel(excel_buffer, index=False, sheet_name="Dados Combinados")
        excel_buffer.seek(0)

        client.put_object(
            TRUSTED_BUCKET,
            "combined_data_trusted.xlsx",
            excel_buffer,
            length=excel_buffer.getbuffer().nbytes,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        print("Arquivo 'combined_data_trusted.xlsx' salvo com sucesso no bucket 'trusted'.")
    else:
        print("Nenhum DataFrame foi encontrado no bucket STAGING.")
