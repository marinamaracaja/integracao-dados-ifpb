import requests
import json
import pandas as pd

# Obter o token do mapbiomas
def obter_token():
    url = "https://plataforma.alerta.mapbiomas.org/api/v2/graphql"
    auth_query = """
    mutation signIn($email: String!, $password: String!) {
      signIn(email: $email, password: $password) {
        token
      }
    }
    """
    variables_signin = {
        "email": "fabiana.beda@gmail.com",
        "password": "IFPB&dados2024"
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
    from minio import Minio
    from io import BytesIO

    # Configurações do MinIO no Docker
    minio_client = Minio(
        "localhost:9000",
        access_key="test",
        secret_key="test12334567",
        secure=False
    )

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
            print("Dados retornados pela API:")
            # print(json.dumps(data, indent=2))
            bucket_name = "RAW"
            file_name = "data_raw.json"
            json_bytes = json.dumps(data, indent=4).encode("utf-8")

            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)
            
            # Enviando o JSON bruto para o MinIO
            minio_client.put_object(
                bucket_name,
                file_name,
                data=BytesIO(json_bytes),
                length=len(json_bytes),
                content_type="application/json"
            )
            print(f"Arquivo '{file_name}' salvo no bucket '{bucket_name}' com sucesso.")
    else:
        print(f"Erro ao obter os dados: {response_data.status_code}")
        print(response_data.text)

try:
    raw_extract_data_api()
except Exception as e:
    print(e)