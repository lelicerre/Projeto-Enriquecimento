from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DSR Example") \
    .master("local[*]") \
    .getOrCreate()

spark = SparkSession.builder.getOrCreate()

dsr_3months_query = """
SELECT *
FROM (
    SELECT
        operador AS operador_id,
        dsr AS dsr_3m,
        SUM(volume) AS tons_3m,
        ROW_NUMBER() OVER (PARTITION BY operador ORDER BY SUM(volume) DESC) AS ranking_3m
    FROM
        schema_base.tabela_fatos_bi
    WHERE
        data >= DATE_ADD(CURRENT_DATE(), -90)
    GROUP BY
        operador, dsr
)
WHERE ranking_3m = 1
"""

dsr_3months = spark.sql(dsr_3months_query)
dsr_3months.createOrReplaceTempView("dsr_3months")

#####

dsr_12months_query = """
SELECT *
FROM (
    SELECT
        operador AS operador_id,
        dsr AS dsr_12m,
        SUM(volume) AS tons_12m,
        ROW_NUMBER() OVER (PARTITION BY operador ORDER BY SUM(volume) DESC) AS ranking_12m
    FROM
        schema_base.tabela_fatos_bi
    WHERE
        data >= DATE_ADD(CURRENT_DATE(), -365)
    GROUP BY
        operador,
        dsr
)
WHERE ranking_12m = 1
"""

dsr_12months = spark.sql(dsr_12months_query)
dsr_12months.createOrReplaceTempView("dsr_12months")

#####

dsr_ever_query = """
SELECT operador AS operador_id, dsr AS dsr_ever
FROM (
    SELECT *
    FROM (
        SELECT
            operador,
            dsr,
            MAX(volume) AS tons_principal,
            ROW_NUMBER() OVER (PARTITION BY operador ORDER BY SUM(volume) DESC) AS ranking_principal
        FROM
            schema_base.tabela_fatos_bi
        WHERE
            YEAR(data) >= 2022
        GROUP BY
            operador,
            dsr
    )
    WHERE ranking_principal = 1
)
"""

dsr_ever = spark.sql(dsr_ever_query)
dsr_ever.createOrReplaceTempView("dsr_ever")

#####

dt_ultima_compra_fs_query = """
SELECT
    a.operador AS operador_id,
    MAX(a.data) AS dt_ultima_compra_fs
FROM
    schema_base.tabela_fatos_bi AS a
LEFT JOIN
    schema_base.tabela_dim_produto AS b
ON
    a.sku_ean = b.codigo_produto_universal
WHERE
    b.commodity = 'FS'
GROUP BY
    a.operador
"""

dt_ultima_compra_fs = spark.sql(dt_ultima_compra_fs_query)
dt_ultima_compra_fs.createOrReplaceTempView("dt_ultima_compra_fs")

#####

cache_query = """
SELECT 
    operador AS operador_id, 
    c.commodity, 
    '202504' AS dt_ref, 
    b.ano_mes AS safra
FROM 
    schema_base.tabela_fatos_bi a
LEFT JOIN 
    schema_base.tabela_dim_date b ON a.data = b.data_db_pk
LEFT JOIN
    schema_base.tabela_dim_produto c ON a.sku_ean = c.codigo_produto_universal
WHERE
    c.commodity = 'FS'
"""

spark.sql(f"CACHE TABLE bd_sell_out AS {cache_query}")

#####

bd_sell_out_df = spark.table("bd_sell_out")

dim_date_sub36_df = spark.table("schema_base.tabela_dim_date_sub36months")

result_df = bd_sell_out_df.alias("a") \
    .join(
        dim_date_sub36_df.alias("b"),
        bd_sell_out_df.dt_ref == dim_date_sub36_df.anomes,
        "left"
    ) \
    .select(
        "a.operador",
        "b.anomes",
        "b.anomes_sub02",
        "b.anomes_sub11"
    )

result_df.show()

#####

spark.catalog.dropTempView("tabela_aux")

bd_sell_out_df = spark.table("bd_sell_out")
dim_date_sub36_df = spark.table("schema_base.tabela_dim_date_sub36months")

tabela_aux_df = bd_sell_out_df.alias("a") \
    .join(
        dim_date_sub36_df.alias("b"),
        bd_sell_out_df.dt_ref == dim_date_sub36_df.anomes,
        "left"
    ) \
    .select(
        "a.operador",
        "a.dt_ref",
        "a.safra",
        "b.anomes",
        "b.anomes_sub02",
        "b.anomes_sub11"
    )

tabela_aux_df.cache()
tabela_aux_df.createOrReplaceTempView("tabela_aux")

#####

if "tabela_aux" in spark.catalog.listTables():
    spark.catalog.dropTempView("tabela_aux")

bd_sell_out_df = spark.table("bd_sell_out")
dim_date_sub36_df = spark.table("schema_base.tabela_dim_date_sub36months")

tabela_aux_df = bd_sell_out_df.alias("a") \
    .join(
        dim_date_sub36_df.alias("b"),
        bd_sell_out_df.dt_ref == dim_date_sub36_df.anomes,
        "left"
    ) \
    .select(
        "a.operador",
        "a.dt_ref",
        "a.safra",
        "b.anomes",
        "b.anomes_sub02",
        "b.anomes_sub11"
    )

tabela_aux_df.cache()
tabela_aux_df.createOrReplaceTempView("tabela_aux")

#####

from pyspark.sql.functions import when, col

tabela_aux_df = spark.table("tabela_aux")

tabela_aux2_df = tabela_aux_df.groupBy(
    "operador",
    "dt_ref",
    "safra",
    "anomes_sub02",
    "anomes_sub11"
).agg(
)

tabela_aux_distinct = tabela_aux_df.select(
    "operador",
    "dt_ref",
    "safra",
    "anomes_sub02",
    "anomes_sub11"
).distinct()

# Criar colunas calculadas
tabela_aux2_df = tabela_aux_distinct.withColumn(
    "compra_12m",
    when(
        (col("safra") <= col("dt_ref")) & (col("safra") >= col("anomes_sub11")),
        1
    ).otherwise(0)
).withColumn(
    "compra_3m",
    when(
        (col("safra") <= col("dt_ref")) & (col("safra") >= col("anomes_sub02")),
        1
    ).otherwise(0)
)

# Cache e criar view temporária
tabela_aux2_df.cache()
tabela_aux2_df.createOrReplaceTempView("tabela_aux2")

#####

status_operador_query = """
SELECT
    operador AS operador_id,
    CASE
        WHEN SUM(compra_3m) > 0 THEN 'Ativo'
        WHEN SUM(compra_3m) = 0 AND SUM(compra_12m) > 0 THEN 'Churn'
        ELSE 'Inativo'
    END AS status
FROM tabela_aux2
GROUP BY operador
"""

status_operador = spark.sql(status_operador_query)
status_operador.createOrReplaceTempView("status_operador")

#####

status_operador_query = """
SELECT
    operador AS operador_id,
    CASE
        WHEN SUM(compra_3m) > 0 THEN 'Ativo'
        WHEN SUM(compra_3m) = 0 AND SUM(compra_12m) > 0 THEN 'Churn'
        ELSE 'Inativo'
    END AS status
FROM tabela_aux2
GROUP BY operador
"""

status_operador = spark.sql(status_operador_query)
status_operador.createOrReplaceTempView("status_operador")

#####

cesta_I12months_query = """
SELECT 
    a.operador AS operador_id,
    array_join(collect_set(b.categoria), ' | ') AS cesta_I12m
FROM
    schema_base.tabela_fatos_bi a
LEFT JOIN
    schema_base.tabela_dim_produto b
ON
    a.sku_ean = b.codigo_produto_universal
WHERE
    a.data >= add_months(current_date(), -12)
AND
    b.categoria IN (
        'FS - MAIONESE HELLMANNS', 
        'FS - CALDOS KNORR', 
        'FS - DOYPACK HELLMANNS', 
        'FS - PORTION PACK HELLMANNS', 
        'FS - TOMATE DESIDRATADO', 
        'FS - PURE BATATA KNORR',
        'FS - OUTROS'
    )
GROUP BY
    a.operador
"""

cesta_I12months = spark.sql(cesta_I12months_query)
cesta_I12months.createOrReplaceTempView("cesta_I12months")

#####

cesta_I3months_query = """
SELECT 
    a.operador AS operador_id,
    array_join(collect_set(b.categoria), ' | ') AS cesta_I3m
FROM
    schema_base.tabela_fatos_bi a
LEFT JOIN
    schema_base.tabela_dim_produto b
ON
    a.sku_ean = b.codigo_produto_universal
WHERE
    a.data >= add_months(current_date(), -3)
AND
    b.categoria IN (
        'FS - MAIONESE HELLMANNS', 
        'FS - CALDOS KNORR', 
        'FS - DOYPACK HELLMANNS', 
        'FS - PORTION PACK HELLMANNS', 
        'FS - TOMATE DESIDRATADO', 
        'FS - PURE BATATA KNORR',
        'FS - OUTROS'
    )
GROUP BY
    a.operador
"""

cesta_I3months = spark.sql(cesta_I3months_query)
cesta_I3months.createOrReplaceTempView("cesta_I3months")

#####

tons_3months_query = """
SELECT *
FROM (
    SELECT
        operador AS operador_id,
        SUM(volume) AS tons_3m
    FROM 
        schema_base.tabela_fatos_bi
    WHERE 
        data >= DATE_ADD(CURRENT_DATE(), -90)
    GROUP BY 
        operador
)
"""

tons_3months = spark.sql(tons_3months_query)
tons_3months.createOrReplaceTempView("tons_3months")

#####

tons_12months_query = """
SELECT *
FROM (
    SELECT
        operador AS operador_id,
        SUM(volume) AS tons_12m
    FROM
        schema_base.tabela_fatos_bi
    WHERE
        data >= DATE_ADD(CURRENT_DATE(), -365)
    GROUP BY
        operador
)
"""

tons_12months = spark.sql(tons_12months_query)
tons_12months.createOrReplaceTempView("tons_12months")

#####

fat_12months_query = """
SELECT *
FROM (
    SELECT
        operador AS operador_id,
        SUM(valor_s_boni) AS fat_12m
    FROM
        schema_base.tabela_fatos_bi
    WHERE
        data >= DATE_ADD(CURRENT_DATE(), -365)
    GROUP BY
        operador
)
"""

fat_12months = spark.sql(fat_12months_query)
fat_12months.createOrReplaceTempView("fat_12months")

#####

fat_3months_query = """
SELECT *
FROM (
    SELECT
        operador AS operador_id,
        SUM(valor_s_boni) AS fat_3m
    FROM
        schema_base.tabela_fatos_bi
    WHERE
        data >= DATE_ADD(CURRENT_DATE(), -90)
    GROUP BY
        operador
)
"""

fat_3months = spark.sql(fat_3months_query)
fat_3months.createOrReplaceTempView("fat_3months")

#####

from pyspark.sql.functions import col

fatos_df = spark.table("schema_base.tabela_fatos_bi")
dim_produto_df = spark.table("schema_base.tabela_dim_produto")

resultado_df = fatos_df.alias("a") \
    .join(
        dim_produto_df.alias("b"),
        col("a.sku_ean") == col("b.codigo_produto_universal"),
        "left"
    ) \
    .filter(
        (col("a.operador") == "28429315000178") &
        (col("b.commodity") == "FS")
    ) \
    .select(
        col("a.operador").alias("operador_id"),
        col("b.commodity"),
        col("a.data")
    )

resultado_df.show()