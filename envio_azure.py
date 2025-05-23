import pandas as pd
from pyspark.sql.functions import col, regexp_replace, split
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProjetoEnriquecimento").getOrCreate()

path = spark.createDataFrame([
    {"name": "arquivo_exemplo_20250522.xlsx"}
])

arquivo_path = list(path.select("name").toPandas()["name"])[0]
caminho_arquivo = f"dbfs:/mnt/adls_dev/BRAZIL/Legacy/ufs-br-udl/input/projeto_star_schema/Arquivos_enriquecimento/{arquivo_path}"

anexo_pd = pd.read_excel(caminho_arquivo, header=0, engine="openpyxl")

anexo = spark.createDataFrame(anexo_pd) \
    .withColumn("cnpj", regexp_replace(col("cnpj"), "[^0-9]", ""))

anexo.createOrReplaceTempView("anexo")

id_anexo = path.withColumn("id", split(col("name"), "_").getItem(0)).select("name", "id")
id_anexo.createOrReplaceTempView("id_anexo")

df_respostas_pd = pd.read_excel(
    "/dbfs/mnt/adls_dev/BRAZIL/Legacy/ufs-br-udl/input/projeto_star_schema/Arquivos_enriquecimento/Respostas/Arquivo-forms.xlsx",
    engine="openpyxl"
)

df_respostas = spark.createDataFrame(df_respostas_pd) \
    .withColumn(
        "Selecione os campos para enriquecimento dos CNPJs",
        regexp_replace(
            col("Selecione os campos para enriquecimento dos CNPJs"),
            r"[\[\]]",
            ""
        )
    )

df_respostas.createOrReplaceTempView("arg_respostas")