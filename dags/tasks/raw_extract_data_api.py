import requests
import json
import pandas as pd
from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO

# Função para obter o token do Mapbiomas
def obter_token():
    connection = BaseHook.get_connection('mapbiomas_login')
    url = connection.host
    auth_query = """
    mutation signIn($email: String!, $password: String!) {
      signIn(email: $email, password: $password) {
        token
      }
    }
    """
    variables_signin = {
        "email": connection.login,
        "password": connection.password
    }
    headers_auth = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload_auth = {
        "query": auth_query,
        "variables": variables_signin
    }
    auth_response = requests.post(url, json=payload_auth, headers=headers_auth)
    if auth_response.status_code == 200:
        auth_data = auth_response.json()
        return auth_data['data']['signIn']['token']
    else:
        raise Exception(f"Erro na autenticação: {auth_response.status_code} - {auth_response.text}")

# Função para salvar dados como JSON no MinIO
def salvar_dados_minio(client, bucket_name, file_name, data):
    json_object = json.dumps(data, indent=2).encode("utf-8")
    print(f"Salvando arquivo {file_name} no MinIO...")  # Log antes de salvar
    with BytesIO(json_object) as byte_data:
        try:
            client.put_object(
                bucket_name,
                file_name,
                data=byte_data,
                length=len(json_object),
                content_type="application/json"
            )
            print(f"Arquivo {file_name} salvo com sucesso.")  # Log após salvar
        except Exception as e:
            print(f"Erro ao salvar o arquivo {file_name}: {e}")

# Função para extrair e processar dados da API Mapbiomas e salvar DataFrames no MinIO
def raw_extract_mapbiomas_api():
    minio_connection = BaseHook.get_connection('minio')
    host = minio_connection.host + ':' + str(minio_connection.port)
    client = Minio(host, secure=False, access_key=minio_connection.login, secret_key=minio_connection.password)
    BUCKET = "raw"
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    
    # Obter dados da API Mapbiomas
    token = obter_token()
    url = "https://plataforma.alerta.mapbiomas.org/api/v2/graphql"
    headers_with_token = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }
    query_data = """
    {
      alerts {
        collection {
          alertCode
          areaHa
          detectedAt
          publishedAt
          sources
        }
        rankingByBiome {
          biome
          alertsTotal
          areaTotal
        }
        rankingByCity {
          city
          alertsTotal
          areaTotal
        }
        rankingByState {
          state
          alertsTotal
          areaTotal
        }
        summary {
          total
          area
          ruralPropertiesCount
          settlementsCount
          indigenousLandCount
          conservationUnitsCount
        }
      }
    }
    """
    response_data = requests.post(url, json={"query": query_data}, headers=headers_with_token)
    
    if response_data.status_code == 200:
        data = response_data.json()
        print(f"Dados da API Mapbiomas: {json.dumps(data, indent=2)[:500]}")  # Exibe os primeiros 500 caracteres
        if 'errors' in data:
            print("Erro na resposta da API Mapbiomas:")
            print(json.dumps(data['errors'], indent=2))
        else:
            print("Dados retornados com sucesso pela API Mapbiomas")
            salvar_dados_minio(client, BUCKET, "mapbiomas_data_raw.json", data)

            # Processar dados e criar DataFrames
            dados_alertas = data['data']['alerts']
            df_alertas = pd.DataFrame(dados_alertas['collection'])
            df_biomas = pd.DataFrame(dados_alertas['rankingByBiome'])
            df_cidades = pd.DataFrame(dados_alertas['rankingByCity'])
            df_estados = pd.DataFrame(dados_alertas['rankingByState'])
            df_resumo = pd.DataFrame([dados_alertas['summary']])

            # Salvar DataFrames processados como JSON no MinIO
            salvar_dados_minio(client, BUCKET, "df_alertas.json", df_alertas.to_dict(orient="records"))
            salvar_dados_minio(client, BUCKET, "df_biomas.json", df_biomas.to_dict(orient="records"))
            salvar_dados_minio(client, BUCKET, "df_cidades.json", df_cidades.to_dict(orient="records"))
            salvar_dados_minio(client, BUCKET, "df_estados.json", df_estados.to_dict(orient="records"))
            salvar_dados_minio(client, BUCKET, "df_resumo.json", df_resumo.to_dict(orient="records"))
    else:
        print(f"Erro ao obter os dados Mapbiomas: {response_data.status_code}")
        print(response_data.text)

# Função para consultar a API AQICN e processar dados de qualidade do ar
def raw_extract_aqicn_api(cidade):
    minio_connection = BaseHook.get_connection('minio')
    host = minio_connection.host + ':' + str(minio_connection.port)
    client = Minio(host, secure=False, access_key=minio_connection.login, secret_key=minio_connection.password)
    BUCKET = "raw"
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    token_aqicn = "3d2600ab777bcf8d09fbb8f87dd6bcb27713544d"
    # token_aqicn = BaseHook.get_connection('aqicn_api').password
    url_aqicn = f"https://api.waqi.info/feed/{cidade}/?token={token_aqicn}"
    response_aqicn = requests.get(url_aqicn)

    if response_aqicn.status_code == 200:
        data = response_aqicn.json()
        print(f"Dados da API AQICN: {json.dumps(data, indent=2)[:500]}")  # Exibe os primeiros 500 caracteres
        if data['status'] == 'ok':
            print(f"Dados de qualidade do ar para {cidade} obtidos com sucesso.")
            salvar_dados_minio(client, BUCKET, f"{cidade}_aqicn_data_raw.json", data)
            
            # Processar previsões de qualidade do ar
            forecast_data = data['data']['forecast']['daily']
            forecast_dfs = []
            for pollutant in ['o3', 'pm10', 'pm25', 'uvi']:
                if pollutant in forecast_data:
                    df = pd.DataFrame(forecast_data[pollutant])
                    df['pollutant'] = pollutant.upper()
                    forecast_dfs.append(df)
            if forecast_dfs:
                forecast_df = pd.concat(forecast_dfs, ignore_index=True)
                salvar_dados_minio(client, BUCKET, f"{cidade}_forecast_df.json", forecast_df.to_dict(orient="records"))

            # Processar dados atuais de qualidade do ar
            iaqi_data = data['data']['iaqi']
            current_data = {
                'pollutant': [key.upper() for key in iaqi_data.keys()],
                'value': [value['v'] for value in iaqi_data.values()]
            }
            current_df = pd.DataFrame(current_data)
            salvar_dados_minio(client, BUCKET, f"{cidade}_current_df.json", current_df.to_dict(orient="records"))
        else:
            print("Erro na resposta da API AQICN:", data['data'])
    else:
        print(f"Erro ao obter dados da API AQICN: {response_aqicn.status_code}")
        print(response_aqicn.text)
    

# Executar funções de extração e processamento
try:
    raw_extract_mapbiomas_api()
    raw_extract_aqicn_api("vitoria")
except Exception as e:
    print(e)
