import os
from minio import Minio
from airflow.hooks.base import BaseHook

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
