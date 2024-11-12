from pyspark.sql import SparkSession
from datetime import datetime

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

# Criando função de log
def log():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' >>>'

#--------------------------------- Início Processamento Camada Trusted ------------------------------------------#

# Lendo dados do csv que estão no bucket s3 na camada raw
df_acidentes = spark.read.parquet('s3://905418122144-pod-academy-data-lake/02_raw/tb_acidentes/')
df_acidentes.createOrReplaceTempView('df_acidentes')

# Armazenando quantidade de registros
qtd_registros = df_acidentes.count()

# Ajustando tipos dos dados e adcionando colunas de data de processamento e referência
df_acidentes_formated = spark.sql(f"""
    select
        cast(id as integer) as id_acidente
        , '{dt_proc}' as dt_proc
        , substring(replace(data_inversa,'-',''),1,6) as ref
        , cast(data_inversa as date) as data_acidente
        , dia_semana as dia_semana_acidente
        , horario as horario_acidente
        , uf as uf_acidente
        , br as br_acidente
        , cast(replace(km, ',', '.') as decimal(14,3)) as km_acidente
        , municipio as municipio_acidente
        , causa_acidente
        , tipo_acidente
        , classificacao_acidente
        , fase_dia as fase_dia_acidente
        , sentido_via as sentido_acidente
        , condicao_metereologica as condicao_metereologica_acidente
        , tipo_pista as tipo_pista_acidente
        , tracado_via as tracado_via_acidente
        , uso_solo
        , pessoas
        , mortos
        , feridos_leves
        , feridos_graves
        , ilesos
        , ignorados
        , feridos
        , veiculos as veiculos_acidente
        , cast(regexp_replace(latitude, ',', '.') as double) as latitude_acidente
        , cast(regexp_replace(longitude, ',', '.') as double) as longitude_acidente
        , regional
        , delegacia
        , uop
    from df_acidentes
""")

df_acidentes_formated.createOrReplaceTempView('df_acidente_formated')

# Salvando os dados na camada trusted em formato parquet, particionados pela referência (mês)
# e em modo append
df_acidentes_formated.write.mode('append').partitionBy('ref').parquet('s3://905418122144-pod-academy-data-lake/03_trusted/tb_acidentes/')

#--------------------------------- Início Carregamento Base Controle -------------------------------------------#

# Criando dados de controle
df_controle_acidentes = spark.sql(f"""
    select
        'tb_trusted_acidentes' as tb_name
        , {dt_proc} as dt_proc
        , {qtd_registros} as qtd_registros
""")

# Salvando dados de controle na camada trusted em formato parquet
df_controle_acidentes.write.mode('append').parquet('s3://905418122144-pod-academy-data-lake/05_controle/trusted/')
