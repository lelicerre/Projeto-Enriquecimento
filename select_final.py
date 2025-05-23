select_final_query = """
SELECT DISTINCT
  a.cnpj AS cnpj_inserido,
  b.grupo_operador AS grupo,
  b.classificacao_eotm,
  b.risco_churn,
  b.mei,
  b.grupo_foco,
  b.uf,
  b.distribuidor_principal,
  b.distribuidor_mais_proximo,
  b.cesta_sugerida,
  b.ativo_receita,
  b.mesorregiao,
  b.municipio,
  b.cep,
  b.bairro,
  b.endereco_completo,
  b.regional,
  b.tem_skus_potenciais,
  b.rfv,
  b.cadastro_cfs,
  b.sub_grupo,
  b.nome_fantasia,
  b.razao_social,
  b.consultor,
  b.cluster,
  b.carteirizado_atual,
  b.descricao_cnae,
  b.ability_to_win,
  b.quantidade_skus_potenciais,
  b.dsr_ultimos_3m,
  b.dsr_ultimos_12m,
  b.dsr_desde_sempre,
  b.contatos,
  b.tons_ultimos_3m,
  b.tons_ultimos_12m,
  b.fat_ultimos_12m,
  b.fat_ultimos_3m,
  b.data_ultima_compra_fs,
  b.status_operador_fs,
  b.churn,
  b.cesta_ultimos_12m,
  b.cesta_ultimos_3m,
  b.carteirizado_historicamente
FROM anexo a
LEFT JOIN tabela_final b
  ON LPAD(a.cnpj, 14, '0') = b.cnpj
"""

select_final = spark.sql(select_final_query)
select_final.createOrReplaceTempView("select_final")
