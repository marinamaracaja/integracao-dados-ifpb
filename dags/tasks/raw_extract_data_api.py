import os
from minio import Minio
from airflow.hooks.base import BaseHook
# Função para obter o token do Mapbiomas
def obter_token():
    import requests
    import json
    from airflow.hooks.base import BaseHook

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

    # Validar o status da resposta
    if auth_response.status_code == 200:
        try:
            auth_data = auth_response.json()
            # Verificar se o token está presente
            if auth_data and 'data' in auth_data and auth_data['data'] and 'signIn' in auth_data['data']:
                return auth_data['data']['signIn']['token']
            else:
                raise Exception(f"Erro na estrutura da resposta: {json.dumps(auth_data, indent=2)}")
        except json.JSONDecodeError:
            raise Exception(f"Erro ao decodificar a resposta JSON: {auth_response.text}")
    else:
        raise Exception(f"Erro na autenticação: {auth_response.status_code} - {auth_response.text}")


# Função para salvar dados como JSON no MinIO
def salvar_dados_minio(client, bucket_name, file_name, data):
    import json
    from io import BytesIO

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
    import requests
    import json
    import pandas as pd
    from airflow.hooks.base import BaseHook
    from minio import Minio

    minio_connection = BaseHook.get_connection('minio')
    mapbiomas_connection = BaseHook.get_connection('mapbiomas_login')
    host = f"{minio_connection.host}:{minio_connection.port}"
    client = Minio(host, secure=False, access_key=minio_connection.login, secret_key=minio_connection.password)
    BUCKET = "raw"

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    
    # Obter dados da API Mapbiomas
    token = obter_token()
    url = mapbiomas_connection.host
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

    # Validar a resposta da API
    if response_data.status_code == 200:
        try:
            data = response_data.json()
            print(f"Dados da API Mapbiomas: {json.dumps(data, indent=2)[:500]}")  # Exibe os primeiros 500 caracteres
            if 'errors' in data:
                raise Exception(f"Erro na resposta da API Mapbiomas: {json.dumps(data['errors'], indent=2)}")
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
        except json.JSONDecodeError:
            raise Exception(f"Erro ao decodificar a resposta JSON da API: {response_data.text}")
    else:
        raise Exception(f"Erro ao obter os dados Mapbiomas: {response_data.status_code} - {response_data.text}")




def processar_planilha_mapbiomas():
    """
    Função para enviar arquivos GZIP de todos os estados do Nordeste para o MinIO.
    Lida com nomes específicos de arquivos e rotas definidas.
    """
    try:
        # Dicionário contendo os arquivos GZIP e suas rotas
        arquivos_gzip = {
            "Sergipe": "/opt/airflow/data/mapbiomas_def_secveg_Sergipe.json.gz",
            "Rio_Grande_do_Norte": "/opt/airflow/data/mapbiomas_def_secveg_Rio Grande do Norte.json.gz",
            "Pernambuco": "/opt/airflow/data/mapbiomas_def_secveg_Pernambuco.json.gz",
            "Maranhao": "/opt/airflow/data/mapbiomas_def_secveg_Maranhão.json.gz",
            "Piaui": "/opt/airflow/data/mapbiomas_def_secveg_Piauí.json.gz",
            "Bahia": "/opt/airflow/data/mapbiomas_def_secveg_Bahia.json.gz",
            "Ceara": "/opt/airflow/data/mapbiomas_def_secveg_Ceará.json.gz",
            "Alagoas": "/opt/airflow/data/mapbiomas_def_secveg_Alagoas.json.gz",
            "Paraiba": "/opt/airflow/data/mapbiomas_def_secveg_Paraíba.json.gz"
        }

        # Conexão com o MinIO
        print("[INFO] Conectando ao MinIO...")
        minio_connection = BaseHook.get_connection('minio')
        host = minio_connection.host + ':' + str(minio_connection.port)
        client = Minio(host, secure=False, access_key=minio_connection.login, secret_key=minio_connection.password)

        # Nome do bucket
        BUCKET = "raw"

        # Verifica se o bucket existe e limpa o conteúdo, se necessário
        if not client.bucket_exists(BUCKET):
            client.make_bucket(BUCKET)
            print(f"Bucket '{BUCKET}' criado com sucesso.")
        else:
            print(f"Bucket '{BUCKET}' já existe. Limpando conteúdo...")
            objects = client.list_objects(BUCKET, recursive=True)
            for obj in objects:
                client.remove_object(BUCKET, obj.object_name)
            print(f"Todo conteúdo do bucket '{BUCKET}' foi deletado.")

        # Processa arquivos conforme as rotas especificadas
        for estado, caminho_arquivo_gzip in arquivos_gzip.items():
            # Verifica se o arquivo existe
            if not os.path.exists(caminho_arquivo_gzip):
                print(f"[WARNING] Arquivo não encontrado para {estado}: {caminho_arquivo_gzip}")
                continue

            print(f"[INFO] Arquivo GZIP encontrado para {estado}. Preparando para upload...")

            # Nome do arquivo no MinIO
            nome_arquivo_no_minio = os.path.basename(caminho_arquivo_gzip)

            # Upload do arquivo
            with open(caminho_arquivo_gzip, "rb") as arquivo_gzip:
                print(f"[INFO] Enviando {nome_arquivo_no_minio} para o bucket '{BUCKET}'...")
                client.put_object(
                    BUCKET,
                    nome_arquivo_no_minio,
                    arquivo_gzip,
                    length=os.path.getsize(caminho_arquivo_gzip),
                    content_type="application/json"
                )

            print(f"[SUCCESS] Upload do arquivo '{nome_arquivo_no_minio}' concluído com sucesso no bucket '{BUCKET}'.")

    except Exception as e:
        print(f"[ERROR] Ocorreu um erro: {e}")
        raise
