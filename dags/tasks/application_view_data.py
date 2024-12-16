def application_view_data():
    import pandas as pd
    from minio import Minio
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

    TRUSTED_BUCKET = "trusted"
    APPLICATION_BUCKET = "application"

    # Criar bucket APPLICATION se não existir
    if not client.bucket_exists(APPLICATION_BUCKET):
        client.make_bucket(APPLICATION_BUCKET)
        print(f"Bucket '{APPLICATION_BUCKET}' criado com sucesso.")
    else:
        print(f"Bucket '{APPLICATION_BUCKET}' já existe. Limpando conteúdo...")
        objects = client.list_objects(APPLICATION_BUCKET, recursive=True)
        for obj in objects:
            client.remove_object(APPLICATION_BUCKET, obj.object_name)
        print(f"Todo conteúdo do bucket '{APPLICATION_BUCKET}' foi deletado.")

    # Listar objetos no bucket TRUSTED
    objects = client.list_objects(TRUSTED_BUCKET)
    dataframes = []
    for obj in objects:
        try:
            response = client.get_object(TRUSTED_BUCKET, obj.object_name)
            df = pd.read_parquet(BytesIO(response.data))
            dataframes.append(df)
            print(f"Arquivo {obj.object_name} carregado com sucesso.")
        except Exception as e:
            print(f"Erro ao processar o arquivo {obj.object_name}: {e}")

    # Concatenar e filtrar os DataFrames
    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        print(f"Total de registros combinados: {len(final_df)}")

        # Filtrar registros relacionados ao Rio de Janeiro
        city_columns = ['city', 'data_city_name', 'data_city_location', 'data_alerts_rankingbycity']
        existing_city_columns = [col for col in city_columns if col in final_df.columns]

        rio_df = final_df[
            final_df[existing_city_columns].apply(
                lambda row: row.str.contains('Rio de Janeiro', na=False, case=False).any(), axis=1
            )
        ]

        # Salvar os dados filtrados
        if not rio_df.empty:
            print(f"Total de registros relacionados ao Rio de Janeiro: {len(rio_df)}")
            print("Exemplo de registros relacionados ao Rio de Janeiro:")
            print(rio_df.head())

            # Salvar como Excel no bucket APPLICATION
            excel_buffer = BytesIO()
            rio_df.to_excel(excel_buffer, index=False, sheet_name='RioDeJaneiroData')
            excel_buffer.seek(0)

            client.put_object(
                APPLICATION_BUCKET,
                "rio_de_janeiro_data.xlsx",
                excel_buffer,
                length=excel_buffer.getbuffer().nbytes,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            print("Arquivo 'rio_de_janeiro_data.xlsx' salvo com sucesso no bucket 'application'.")

            # Salvar como Parquet no bucket APPLICATION (se possível)
            try:
                parquet_buffer = BytesIO()
                rio_df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                client.put_object(
                    APPLICATION_BUCKET,
                    "rio_de_janeiro_data.parquet",
                    parquet_buffer,
                    length=parquet_buffer.getbuffer().nbytes,
                    content_type="application/octet-stream"
                )
                print("Arquivo 'rio_de_janeiro_data.parquet' salvo com sucesso no bucket 'application'.")
            except Exception as e:
                print(f"Erro ao salvar o arquivo Parquet: {e}")
        else:
            print("Nenhum dado encontrado relacionado ao Rio de Janeiro.")
    else:
        print("Nenhum arquivo encontrado na camada TRUSTED.")
