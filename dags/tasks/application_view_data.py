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
    else:
        # Limpar o conteúdo do bucket
        objects = client.list_objects(APPLICATION_BUCKET, recursive=True)
        for obj in objects:
            client.remove_object(APPLICATION_BUCKET, obj.object_name)

    # Listar objetos no bucket TRUSTED
    objects = client.list_objects(TRUSTED_BUCKET)
    dataframes = []
    for obj in objects:
        try:
            response = client.get_object(TRUSTED_BUCKET, obj.object_name)
            df = pd.read_parquet(BytesIO(response.data))
            dataframes.append(df)
        except Exception as e:
            print(f"Erro ao processar o arquivo {obj.object_name}: {e}")

    # Combinar DataFrames
    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)

        # Remover colunas vazias, totalmente nulas ou preenchidas apenas com "Desconhecido"
        def is_empty_or_unknown(col):
            return col.isnull().all() or (col.astype(str) == "Desconhecido").all()
        
        final_df = final_df.loc[:, ~final_df.apply(is_empty_or_unknown)]

        # Filtrar registros relacionados ao Rio de Janeiro
        city_columns = ['city', 'data_city_name', 'data_city_location', 'data_alerts_rankingbycity']
        existing_city_columns = [col for col in city_columns if col in final_df.columns]
        
        rio_df = final_df[
            final_df[existing_city_columns].apply(
                lambda row: row.str.contains('Rio de Janeiro', na=False, case=False).any(), axis=1
            )
        ]

        if rio_df.empty:
            print("Nenhum dado encontrado relacionado ao Rio de Janeiro.")
            return

        print(f"Total de registros relacionados ao Rio de Janeiro: {len(rio_df)}")

        # Remover colunas específicas solicitadas
        columns_to_remove = [
            'state', 'biome', 'data_aqi', 'status', 'data_attributions', 
            'data_city_geo', 'data_city_url', 'data_dominentpol', 'data_city_location', 'data_aqi'
        ]
        rio_df = rio_df.drop(columns=[col for col in columns_to_remove if col in rio_df.columns])

        # Renomear colunas específicas com prefixo 'mapbiomas_'
        rename_columns = {
            'city': 'mapbiomas_city',
            'alertstotal': 'mapbiomas_alertstotal',
            'areatotal': 'mapbiomas_areatotal'
        }
        rio_df.rename(columns=rename_columns, inplace=True)

        # Adicionar prefixo 'aqicn_' para as demais colunas
        already_renamed = ['mapbiomas_city', 'mapbiomas_alertstotal', 'mapbiomas_areatotal']
        for col in rio_df.columns:
            if col not in already_renamed:
                rio_df.rename(columns={col: f"aqicn_{col}"}, inplace=True)

        # Normalizar colunas com arrays de dicionários
        def normalize_column(df, col_name):
            normalized_data = pd.json_normalize(df[col_name].dropna().explode())
            normalized_data.columns = [f"{col_name}_{key}" for key in normalized_data.columns]
            return normalized_data

        columns_to_normalize = ['aqicn_data_forecast_daily_o3', 'aqicn_data_forecast_daily_pm10', 'aqicn_data_forecast_daily_pm25']
        for col in columns_to_normalize:
            if col in rio_df.columns:
                normalized_df = normalize_column(rio_df, col)
                rio_df = pd.concat([rio_df.drop(columns=[col]), normalized_df], axis=1)

        # Remover novamente colunas que contenham apenas "Desconhecido" após normalização
        rio_df = rio_df.loc[:, ~rio_df.apply(is_empty_or_unknown)]

        # Salvar como Excel no bucket APPLICATION
        excel_buffer = BytesIO()
        rio_df.to_excel(excel_buffer, index=False, sheet_name='ProcessedData_Rio')
        excel_buffer.seek(0)

        client.put_object(
            APPLICATION_BUCKET,
            "rio_de_janeiro_processed_data.xlsx",
            excel_buffer,
            length=excel_buffer.getbuffer().nbytes,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        print("Arquivo 'rio_de_janeiro_processed_data.xlsx' salvo com sucesso no bucket 'application'.")

        # Salvar como Parquet no bucket APPLICATION
        parquet_buffer = BytesIO()
        rio_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        client.put_object(
            APPLICATION_BUCKET,
            "rio_de_janeiro_processed_data.parquet",
            parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        print("Arquivo 'rio_de_janeiro_processed_data.parquet' salvo com sucesso no bucket 'application'.")

        # Salvar como CSV no bucket APPLICATION
        csv_buffer = BytesIO()
        rio_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        client.put_object(
            APPLICATION_BUCKET,
            "rio_de_janeiro_processed_data.csv",
            csv_buffer,
            length=csv_buffer.getbuffer().nbytes,
            content_type="text/csv"
        )
        print("Arquivo 'rio_de_janeiro_processed_data.csv' salvo com sucesso no bucket 'application'.")

    else:
        print("Nenhum arquivo encontrado na camada TRUSTED.")
