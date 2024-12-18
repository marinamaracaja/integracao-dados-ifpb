# Usar uma imagem base que já contém o Google Chrome e o ChromeDriver
FROM selenium/standalone-chrome:latest as chrome-base

# Continuar com a imagem do Apache Airflow
FROM apache/airflow:2.10.2 as apache-airflow-python

# Copiar o Chrome e ChromeDriver da imagem do Selenium
COPY --from=chrome-base /opt/google/chrome /opt/google/chrome
COPY --from=chrome-base /usr/bin/chromedriver /usr/bin/chromedriver
COPY --from=chrome-base /usr/bin/google-chrome /usr/bin/google-chrome

# Atualizar o PATH
ENV PATH="/opt/google/chrome:${PATH}"

# Adicionar os requisitos do projeto
ADD /dags/requirements.txt .

# Atualizar como root e instalar dependências
USER root
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libnss3 \
    libxss1 \
    libappindicator1 \
    libappindicator3-1 \
    libgbm-dev \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Verificar versões instaladas
RUN echo "Google Chrome versão:" && google-chrome --version
RUN chromedriver --version

# Voltar para o usuário airflow para executar pip install
USER airflow

# Instalar dependências do Python
RUN pip install -r requirements.txt
RUN pip install selenium openpyxl
