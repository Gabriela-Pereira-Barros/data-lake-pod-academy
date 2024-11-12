from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

from datetime import datetime, timedelta
from configparser import ConfigParser
import boto3
import time
import pandas as pd
from io import StringIO
from io import BytesIO



# Argumentos que serão considerados para todas as tasks da DAG
default_args = {
    'owner': 'Gabriela',
    'email': 'gabrielapbarros15@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}



# Função para conectar ao s3 na AWS
def get_s3_client():
    config = ConfigParser()
    config_file_dir = '/opt/airflow/config/aws.cfg'
    config.read_file(open(config_file_dir))
    
    aws_access_key_id = config.get('s3', 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('s3', 'AWS_SECRET_ACCESS_KEY')
    
    try:
        return boto3.client('s3', region_name='us-east-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    except Exception as e:
        raise Exception(f'Erro ao conectar com AWS! Error: {e}')



# Função que lê os dados da camada transient, filtra apenas os novos registros e salva em parquet na camada raw (fluxo)
def _prgm_raw_fluxo():
    
    camada = 'raw'
    assunto = 'fluxo'
    fluxo_raw_state = Variable.get("fluxo_raw_state")
    dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')
    
    s3_client = get_s3_client()
    bucket_name = '905418122144-pod-academy-data-lake'
    
    csv_obj = s3_client.get_object(Bucket=bucket_name, Key='01_transient/volume-radar-via040-UTF8.csv')
    csv_content = csv_obj['Body'].read().decode('utf-8')
    df_fluxo = pd.read_csv(StringIO(csv_content), delimiter=";", parse_dates=["data_da_passagem"], dayfirst=True)
    
    df_fluxo["data_da_passagem"] = df_fluxo["data_da_passagem"].dt.strftime("%Y-%m-%d")
    df_fluxo = df_fluxo[df_fluxo["data_da_passagem"] > fluxo_raw_state]
    
    print(df_fluxo["data_da_passagem"].dtype)  # Verifica o tipo da coluna
    print(type(fluxo_raw_state)) 
    
    fluxo_raw_state = df_fluxo["data_da_passagem"].max()
    fluxo_raw_state_name = datetime.strptime(fluxo_raw_state, "%Y-%m-%d")
    fluxo_raw_state_name = fluxo_raw_state_name.strftime('%Y%m%d')
    Variable.set("fluxo_raw_state", fluxo_raw_state)
    
    controle_buffer = BytesIO()
    df_fluxo.to_parquet(controle_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=f'02_raw/tb_{assunto}/tb_{assunto}_{fluxo_raw_state_name}_{dt_proc}.parquet', Body=controle_buffer.getvalue())
    
    # Carregamento Base Controle
    df_controle_fluxo = pd.DataFrame({
        "tb_name": [f'tb_{camada}_{assunto}'],
        "dt_proc": [dt_proc],
        "qtd_registros": [len(df_fluxo)]
    })
    controle_buffer = BytesIO()
    df_controle_fluxo.to_parquet(controle_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=f'05_controle/{camada}/tb_{camada}_{assunto}_{dt_proc}.parquet', Body=controle_buffer.getvalue())
  
  

# Função que lê os dados da camada transient, filtra apenas os novos registros e salva em parquet na camada raw (acidentes)
def _prgm_raw_acidentes():
    
    camada = 'raw'
    assunto = 'acidentes'
    acidentes_raw_state = Variable.get("acidentes_raw_state")
    dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')
    
    s3_client = get_s3_client()
    bucket_name = '905418122144-pod-academy-data-lake'
    
    csv_obj = s3_client.get_object(Bucket=bucket_name, Key='01_transient/datatran2024-UTF8.csv')
    csv_content = csv_obj['Body'].read().decode('utf-8')
    df_acidentes = pd.read_csv(StringIO(csv_content), delimiter=";")
    
    df_acidentes = df_acidentes[df_acidentes["data_inversa"] > acidentes_raw_state]
    
    acidentes_raw_state = df_acidentes["data_inversa"].max()
    acidentes_raw_state_name = datetime.strptime(acidentes_raw_state, "%Y-%m-%d")
    acidentes_raw_state_name = acidentes_raw_state_name.strftime('%Y%m%d')
    Variable.set("acidentes_raw_state", acidentes_raw_state)

    controle_buffer = BytesIO()
    df_acidentes.to_parquet(controle_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=f'02_raw/tb_{assunto}/tb_{assunto}_{acidentes_raw_state_name}_{dt_proc}.parquet', Body=controle_buffer.getvalue())
    
    # Carregamento Base Controle
    df_controle_acidentes = pd.DataFrame({
        "tb_name": [f'tb_{camada}_{assunto}'],
        "dt_proc": [dt_proc],
        "qtd_registros": [len(df_acidentes)]
    })
    controle_buffer = BytesIO()
    df_controle_acidentes.to_parquet(controle_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=f'05_controle/{camada}/tb_{camada}_{assunto}_{dt_proc}.parquet', Body=controle_buffer.getvalue())
        
    
    
# Função para limpar arquivos da cadamada transient
def _transient_cleanup():
    bucket_name = '905418122144-pod-academy-data-lake'
    files_to_delete = ['01_transient/volume-radar-via040-UTF8.csv', '01_transient/datatran2024-UTF8.csv']
    
    s3_client = get_s3_client()
    for file_key in files_to_delete:
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            print(f'Arquivo {file_key} apagado com sucesso!')
        except Exception as e:
            print(f'Erro ao apagar o arquivo {file_key}: {e}')



# Função para conectar ao EMR na AWS
def get_emr_client():
    config = ConfigParser()
    config_file_dir = '/opt/airflow/config/aws.cfg'
    config.read_file(open(config_file_dir))
    
    aws_access_key_id = config.get('EMR', 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('EMR', 'AWS_SECRET_ACCESS_KEY')
    
    try:
        return boto3.client('emr', region_name='us-east-1', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    except Exception as e:
        raise Exception(f'Erro ao conectar com AWS! Error: {e}')



# Função que cria o cluster na AWS
def _create_emr_cluster(ti):
    emr_client = get_emr_client()
    cluster_name = 'Cluster Data Lake PoD Academy'
    log_uri = 's3://aws-logs-905418122144-us-east-1/elasticmapreduce/'
    release_label = 'emr-7.3.0'
    instance_type = 'm5.xlarge'
    
    response = emr_client.run_job_flow(
        Name=cluster_name,
        LogUri=log_uri,
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'InstanceRole': 'MASTER',
                    'InstanceType': instance_type,
                    'InstanceCount': 1,
                },
            ],
            'KeepJobFlowAliveWhenNoSteps': True
        },
        Applications=[{'Name': 'Spark'}],
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole'
    )
    
    cluster_id = response['JobFlowId']
    
    print(f'Cluster criado! ID: {cluster_id}')
    
    ti.xcom_push(key='cluster_id', value=cluster_id)

    
    
# Função para adcionar step job que executa script python de transformação dos dados da camada raw para a camada trusted (fluxo)
def _add_step_job_trusted_fluxo(ti):
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = get_emr_client()
    step_name = 'prgm_trusted_fluxo'
    script_path = 's3://905418122144-pod-academy-codes/scripts_data_lake_edj/prgm_trusted_fluxo.py'
    
    step = {
        'Name': step_name,
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'client', script_path]
        }
    }
    
    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    
    step_job_id = response['StepIds'][0]
    
    print(f'{step_name} adicionado ao cluster! STEP_JOB_ID: {step_job_id}')
    
    ti.xcom_push(key='step_job_id', value=step_job_id)
    
    
    
# Função para adcionar step job que executa script python de transformação dos dados da camada raw para a camada trusted (acidentes)
def _add_step_job_trusted_acidentes(ti):
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = get_emr_client()
    step_name = 'prgm_trusted_acidentes'
    script_path = 's3://905418122144-pod-academy-codes/scripts_data_lake_edj/prgm_trusted_acidentes.py'
    
    step = {
        'Name': step_name,
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'client', script_path]
        }
    }
    
    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    
    step_job_id = response['StepIds'][0]
    
    print(f'{step_name} adicionado ao cluster! STEP_JOB_ID: {step_job_id}')
    
    ti.xcom_push(key='step_job_id', value=step_job_id)
    
    
    
# Função para adcionar step job que executa script python de transformação dos dados da camada raw para a camada trusted (acidentes)
def _add_step_job_refined(ti):
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = get_emr_client()
    step_name = 'prgm_refined'
    script_path = 's3://905418122144-pod-academy-codes/scripts_data_lake_edj/prgm_refined.py'
    
    step = {
        'Name': step_name,
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'client', script_path]
        }
    }
    
    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    
    step_job_id = response['StepIds'][0]
    
    print(f'{step_name} adicionado ao cluster! STEP_JOB_ID: {step_job_id}')
    
    ti.xcom_push(key='step_job_id', value=step_job_id)
    
    
    
# Função que aguarda execução dos steps para que o próximo possa ser adcionado ao cluster, e no final aguarda a execução do último step para encerrar o cluster
def _wait_step_job(ti):
    emr_client = get_emr_client()
    cluster_id = ti.xcom_pull(key='cluster_id')
    step_id = ti.xcom_pull(key='step_job_id')
    
    while True:
        
        response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = response['Step']['Status']['State']
        
        if state in ['PENDING','RUNNING','CANCELLED']:
            print(f'Executando step job... Estado: {state}')
            time.sleep(10)
            
        elif state == 'COMPLETED':
            print(f'Step job finalizado... Estado: {state}')
            break
        
        else:
            raise Exception(f'Error! Estado: {state}')
        
        
        
# Função para criar a task com um novo task_id
def create_task_wait_step_job(task_id):
    return PythonOperator(
        task_id=task_id,
        python_callable=_wait_step_job,
    )
        
        
        
# Função que termina o cluster
def _terminate_emr_cluster(ti):
    cluster_id = ti.xcom_pull(key='cluster_id')
    emr_client = get_emr_client()
    
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    print('Cluster terminado!')
    
    
    
with DAG(
    'pipeline_dl',
    tags=['podacademy'],
    start_date=datetime(2024, 11, 6),
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False
) as dag:
    
    prgm_raw_fluxo = PythonOperator(
        task_id='prgm_raw_fluxo',
        python_callable=_prgm_raw_fluxo
    )
    
    prgm_raw_acidentes = PythonOperator(
        task_id='prgm_raw_acidentes',
        python_callable=_prgm_raw_acidentes
    )

    transient_cleanup = PythonOperator(
        task_id='transient_cleanup',
        python_callable=_transient_cleanup
    )

    create_emr_cluster = PythonOperator(
        task_id='create_emr_cluster',
        python_callable=_create_emr_cluster
    )
    
    add_step_job_trusted_fluxo = PythonOperator(
        task_id='add_step_job_trusted_fluxo',
        python_callable=_add_step_job_trusted_fluxo
    )    
    
    wait_step_job_trusted_fluxo = create_task_wait_step_job(
        'wait_step_job_trusted_fluxo'
    )
    
    add_step_job_trusted_acidentes = PythonOperator(
        task_id='add_step_job_trusted_acidentes',
        python_callable=_add_step_job_trusted_acidentes
    )    
    
    wait_step_job_trusted_acidentes = create_task_wait_step_job(
        'wait_step_job_trusted_acidentes'
    )
    
    add_step_job_refined = PythonOperator(
        task_id='add_step_job_refined',
        python_callable=_add_step_job_refined
    )    
    
    wait_step_job_refined = create_task_wait_step_job(
        'wait_step_job_refined'
    )
    
    terminate_emr_cluster = PythonOperator(
        task_id='terminate_emr_cluster',
        python_callable=_terminate_emr_cluster
    )
    
    
    
    (
        [prgm_raw_fluxo, prgm_raw_acidentes] >> transient_cleanup >> 
        create_emr_cluster >> 
        add_step_job_trusted_fluxo >> wait_step_job_trusted_fluxo >> 
        add_step_job_trusted_acidentes >> wait_step_job_trusted_acidentes >> 
        add_step_job_refined >> wait_step_job_refined >> 
        terminate_emr_cluster
    )
    