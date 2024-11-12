from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import functions as F

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

# Criando função de log
def log():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' >>>'

#--------------------------------- Início Processamento Camada Trusted ------------------------------------------#

# Lendo dados do csv que estão no bucket s3 na camada raw
df_fluxo = spark.read.parquet('s3://905418122144-pod-academy-data-lake/02_raw/tb_fluxo/')
df_fluxo.createOrReplaceTempView('df_fluxo')

# Cria uma nova coluna 'id_registro' usando uma função hash sobre as colunas que formam a chave única
df_fluxo_id = df_fluxo.withColumn("id_registro", F.hash(F.concat(F.col("data_da_passagem"), F.col("tipo_de_veiculo"), F.col("velocidade"),F.col("km_m"),F.col("municipio"),F.col("sentido_da_passagem"))))
df_fluxo_id.createOrReplaceTempView('df_fluxo_id')

# Armazenando quantidade de registros
qtd_registros = df_fluxo.count()

# Ajustando tipos dos dados e adcionando colunas de data de processamento e referência
df_fluxo_formated = spark.sql(f"""
    select
        id_registro as id_fluxo
        , '{dt_proc}' as dt_proc
        , substring(replace(data_da_passagem,'-',''),1,6) as ref
        , concessionaria as concessionaria_fluxo
        , identificador as identificador_fluxo
        , cast(substring(rodovia,'4') as int) as br_fluxo
        , uf as uf_fluxo
        , cast(km_m as decimal (14,3)) as km_fluxo
        , municipio as municipio_fluxo
        , tipo_de_pista
        , latitude as latitude_fluxo
        , longitude as longitude_fluxo
        , cast(data_da_passagem as date) as data_fluxo
        , sentido_da_passagem as sentido_fluxo
        , faixa_da_passagem as faixa_da_passagem_fluxo
        , velocidade as velocidade_fluxo
        , tipo_de_veiculo as tipo_de_veiculo_fluxo
        , volume_total as volume_total_fluxo
    from df_fluxo_id
""")

df_fluxo_formated.createOrReplaceTempView('df_fluxo_formated')

# Salvando os dados na camada trusted em formato parquet, particionados pela referência (mês)
# e em modo append
df_fluxo_formated.write.mode('append').partitionBy('ref').parquet('s3://905418122144-pod-academy-data-lake/03_trusted/tb_fluxo/')

#--------------------------------- Início Carregamento Base Controle -------------------------------------------#

# Criando dados de controle
df_controle_fluxo = spark.sql(f"""
    select
        'tb_trusted_fluxo' as tb_name
        , {dt_proc} as dt_proc
        , {qtd_registros} as qtd_registros
""")

# Salvando dados de controle na camada trusted em formato parquet
df_controle_fluxo.write.mode('append').parquet('s3://905418122144-pod-academy-data-lake/05_controle/trusted/')