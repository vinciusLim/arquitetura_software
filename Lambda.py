import json
import urllib3
import boto3
import time
from datetime import datetime

http = urllib3.PoolManager()
s3 = boto3.client('s3')
glue = boto3.client('glue')

BUCKET_NAME = 'arquitetura-software-datalake'
PREFIX = 'raw/coingecko/'
GLUE_JOB_TRUSTED = 'Trusted - API Criptomoeda'
GLUE_JOB_REFINED = 'Refined - API Criptomoeda'
GLUE_JOB_LOAD = 'Load - API Criptomoeda'

PER_PAGE = 250
MAX_PAGES = 10

def wait_for_job_completion(glue_client, job_name, job_run_id, wait_interval=10, max_attempts=60):
    attempts = 0
    while attempts < max_attempts:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response['JobRun']['JobRunState']
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            return status
        time.sleep(wait_interval)
        attempts += 1
    return 'TIMEOUT'

def lambda_handler(event, context):
    try:
        all_data = []
        for page in range(1, MAX_PAGES + 1):
            url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=brl&per_page={PER_PAGE}&page={page}"
            headers = {
                "accept": "application/json",
                "x-cg-demo-api-key": "CG-P1BziDYYV5UZ4EQ5Vhm3LKam"
            }

            response = http.request("GET", url, headers=headers)

            if response.status != 200:
                raise Exception(f"Erro na API CoinGecko (status {response.status}): {response.data.decode('utf-8')}")

            decoded = response.data.decode("utf-8").strip()
            if not decoded:
                break

            page_data = json.loads(decoded)
            if not page_data:
                break

            all_data.extend(page_data)

        if not all_data:
            raise Exception("Nenhum dado retornado da API CoinGecko.")

        now = datetime.utcnow()
        s3_key = f"{PREFIX}{now.strftime('%Y/%m/%d/%H')}/{now.strftime('%Y%m%d_%H%M%S')}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(all_data),
            ContentType='application/json'
        )

        # Executar jobs do Glue sequencialmente
        trusted_run = glue.start_job_run(JobName=GLUE_JOB_TRUSTED)
        trusted_status = wait_for_job_completion(glue, GLUE_JOB_TRUSTED, trusted_run['JobRunId'])
        if trusted_status != 'SUCCEEDED':
            raise Exception(f"Job {GLUE_JOB_TRUSTED} falhou com status {trusted_status}")

        refined_run = glue.start_job_run(JobName=GLUE_JOB_REFINED)
        refined_status = wait_for_job_completion(glue, GLUE_JOB_REFINED, refined_run['JobRunId'])
        if refined_status != 'SUCCEEDED':
            raise Exception(f"Job {GLUE_JOB_REFINED} falhou com status {refined_status}")

        load_run = glue.start_job_run(JobName=GLUE_JOB_LOAD)
        load_status = wait_for_job_completion(glue, GLUE_JOB_LOAD, load_run['JobRunId'])
        if load_status != 'SUCCEEDED':
            raise Exception(f"Job {GLUE_JOB_LOAD} falhou com status {load_status}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Dados salvos',
                's3_key': s3_key
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
