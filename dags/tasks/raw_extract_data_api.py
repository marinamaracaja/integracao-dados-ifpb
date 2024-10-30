import requests
import json
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from minio import Minio
from io import BytesIO

# Obter o token do mapbiomas
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

try:
    token = obter_token()
    print(f"Token obtido: {token}")
except Exception as e:
    print(e)

# Extrair json da API
def raw_extract_data_api():
    # Configurações do MinIO no Docker
    minio_connection = BaseHook.get_connection('minio')
    host = minio_connection.host + ':' + str(minio_connection.port)
    client = Minio(host, secure=False, access_key=minio_connection.login, secret_key=minio_connection.password)
    BUCKET = "raw"
    buckets = client.list_buckets()
    buckets = [i.name for i in buckets]
    if BUCKET not in buckets:
        client.make_bucket(BUCKET)
    
    # Acessar API
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
    payload_data = {
        "query": query_data
    }
    response_data = requests.post(url, json=payload_data, headers=headers_with_token)
    if response_data.status_code == 200:
        data = response_data.json()
        if 'errors' in data:
            print("Erro na resposta da API:")
            print(json.dumps(data['errors'], indent=2))
        else:
            print("Dados retornados com sucesso pela API")
            bucket_name = "raw"
            file_name = "data_raw.json"
            json_object = json.dumps(data, indent=2).encode("utf-8")
            
            # Salvar dados na API
            client.put_object(
              bucket_name,
              file_name,
              data=BytesIO(json_object),
              length=len(json_object),
              content_type="application/json"
            )

            print(f"Arquivo '{file_name}' salvo no bucket '{bucket_name}' do MinIO com sucesso.")
    else:
        print(f"Erro ao obter os dados: {response_data.status_code}")
        print(response_data.text)

try:
    raw_extract_data_api()
except Exception as e:
    print(e)