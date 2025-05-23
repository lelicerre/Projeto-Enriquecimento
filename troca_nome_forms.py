from pyspark.sql.functions import col, regexp_replace
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProjetoEnriquecimento").getOrCreate()

substituicoes = {
    "Distribuidor Principal": "DISTRIBUIDOR_PRINCIPAL",
    "Distribuidor Mais Próximo": "DISTRIBUIDOR_MAIS_PROXIMO",
    "Classificação EOTM": "CLASSIFICACAO_EOTM",
    "OG Foco": "OG_FOCO",
    "Cesta Sugerida": "CESTA_SUGERIDA",
    "Ativo Receita": "ATIVO_RECEITA",
    "Endereço Completo": "ENDERECO_COMPLETO",
    "Tem SKU Potenciais": "TEM_SKU_POTENCIAIS",
    "Cadastro CFS": "CADASTRO_CFS",
    "Sub OG": "SUB_OG",
    "Nome Fantasia": "NOME_FANTASIA",
    "Razão Social": "RAZAO_SOCIAL",
    "Consultor GD Atual": "CONSULTOR_GD_ATUAL",
    "Cluster Atual": "CLUSTER_ATUAL",
    "Carteirizado Atual": "CARTEIRIZADO_ATUAL",
    "Descrição CNAE": "DESCRICAO_CNAE",
    "Ability To Win": "ABILITY_TO_WIN",
    "Quantidade SKU Potenciais": "QUANTIDADE_SKUS_POTENCIAIS",
    "DSR Principal Últimos 3 Meses": "DSR_PRINCIPAL_ULTIMOS_3_MESES",
    "DSR Principal Últimos 12 Meses": "DSR_PRINCIPAL_ULTIMOS_12_MESES",
    "DSR Principal Desde Sempre": "DSR_PRINCIPAL_DESDE_SEMPRE",
    "Data Última Compra FS": "DATA_ULTIMA_COMPRA_FS",
    "Status Operador FS": "STATUS_OPERADOR_FS",
    "Cesta Últimos 12 Meses": "CESTA_ULTIMOS_12_MESES",
    "Cesta Últimos 3 Meses": "CESTA_ULTIMOS_3_MESES",
    "Carteirizado Históricamente": "CARTEIRIZADO_HISTORICAMENTE",
    "Mesorregião": "MESORREGIAO",
    "Município": "MUNICIPIO",
    "Risco Churn": "RISCO_CHURN",
    "Contatos": "CONTATOS",
    "Tons Último 12 Meses": "TONS_ULTIMOS_12_MESES",
    "Tons Últimos 3 Meses": "TONS_ULTIMOS_3_MESES",
    "Faturamento Últimos 12 Meses": "FATURAMENTO_ULTIMOS_12_MESES",
    "Faturamento Últimos 3 Meses": "FATURAMENTO_ULTIMOS_3_MESES",
    "Selecionar Todas As Colunas": "*"
}

coluna = col('Selecione os campos para enriquecimento dos CNPJs')

from pyspark.sql import DataFrame

def aplica_substituicoes(df: DataFrame, coluna_nome: str, substituicoes: dict) -> DataFrame:
    coluna_expr = col(coluna_nome)
    for original, substituto in substituicoes.items():
        coluna_expr = regexp_replace(coluna_expr, original, substituto)
    return df.withColumn(coluna_nome, coluna_expr)

campos = spark.sql('''
    SELECT a.`Selecione os campos para enriquecimento dos CNPJs`
    FROM arq_respostas a
    LEFT JOIN id_anexo b ON a.id = b.id
    WHERE a.id = b.id
''')

campos = aplica_substituicoes(campos, 'Selecione os campos para enriquecimento dos CNPJs', substituicoes)

campos.createOrReplaceTempView("campos")
print(campos.show())

id_email = spark.sql('''
SELECT a.Email
FROM arq_respostas a
LEFT JOIN id_anexo b
ON a.id = b.id
WHERE a.id = b.id
''')

id_email.createOrReplaceTempView("id_email")

id_email.show()