campos_selecionados = spark.sql("SELECT `Selecione os campos para enriquecimento dos CNPJs` FROM campos").collect()

coluna_excluir = "CNPJ_INSERIDO"

if campos_selecionados:
    lista_campos = [campo.strip() for campo in campos_selecionados[0][0].split(",")]
else:
    lista_campos = []

if "*" in lista_campos:
    todas_colunas = [coluna[0] for coluna in spark.sql("DESCRIBE select_final").collect()]
    lista_campos = [coluna for coluna in todas_colunas if coluna != coluna_excluir]

campos_para_selecionar = ["a.cnpj"] + [f"b.`{campo}`" for campo in lista_campos]

query = f"""
SELECT {", ".join(campos_para_selecionar)}
FROM anexo a
JOIN select_final b ON a.cnpj = b.cnpj_inserido
"""

base_final = spark.sql(query)
base_final.createOrReplaceTempView("base_personalizada")

base_final.show()
