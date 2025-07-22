# Arquitetura de Software - Pipeline de Dados Criptomoedas

Este projeto implementa um pipeline de dados utilizando AWS Glue e Lambda para coletar, processar e armazenar informações de criptomoedas da API CoinGecko.

## Estrutura do Projeto

- **Lambda.py**: Função Lambda responsável por coletar dados da API CoinGecko, salvar no S3 e orquestrar os jobs do Glue, acionada a cada hora através do AWS CloudWatch
- **Notebooks Glue**:
  - **Trusted - API Criptomoeda.ipynb**: Processa os dados brutos do S3, realiza limpeza e transformação, e salva na camada trusted.
  - **Refined - API Criptomoeda.ipynb**: Refina os dados trusted, criando tabelas fato e dimensão, e salva na camada refined.
  - **Load - API Criptomoeda.ipynb**: Carrega os dados refined para um banco de dados PostgreSQL.
- **arquitetura_de_software.drawio.png**: Diagrama da arquitetura do pipeline.

## Pipeline

1. **Coleta**: A função Lambda acessa a API CoinGecko, coleta os dados e salva arquivos JSON no S3 (camada raw).
2. **Trusted**: O notebook Trusted lê os dados brutos, realiza tratamento e salva no S3 (camada trusted).
3. **Refined**: O notebook Refined cria tabelas analíticas (fato e dimensão) e salva no S3 (camada refined).
4. **Load**: O notebook Load carrega os dados refinados para o banco de dados relacional.
