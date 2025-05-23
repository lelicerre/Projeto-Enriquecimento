tabela_final_query = """
SELECT
  a.cnpj AS operador_id,
  a.grupo_operador AS grupo,
  a.classificacao_eotm,
  a.classificacao_churn AS risco_churn,
  CASE WHEN a.flag_mei = 'Y' THEN 'Sim' ELSE 'Não' END AS mei,
  CASE WHEN a.flag_novo_grupo_foco = 'Y' THEN 'Sim' ELSE 'Não' END AS grupo_foco,
  a.uf,
  a.distribuidor_principal,
  a.distribuidor_proximo_1 AS distribuidor_mais_proximo,
  CONCAT_WS(' | ', v.cesta_sugerida) AS cesta_sugerida,
  CASE WHEN a.flag_ativa = 'Y' THEN 'Sim' ELSE 'Não' END AS ativo_receita,
  a.mesorregiao,
  a.municipio,
  a.cep,
  a.bairro,
  CONCAT_WS(' - ', a.logradouro, a.numero) AS endereco_completo,
  p.regiao_23 AS regional,
  CASE WHEN a.flag_sku_potencial = 'Y' THEN 'Sim' ELSE 'Não' END AS tem_skus_potenciais,
  a.classificacao_rfv AS rfv,
  CASE WHEN a.flag_cadastro_cfs = 'Y' THEN 'Sim' ELSE 'Não' END AS cadastro_cfs,
  a.sub_grupo_operador AS sub_grupo,
  b.nome_fantasia,
  b.razao_social,
  c.consultor,
  c.cluster,
  c.cnpj AS carteirizado_atual,
  n.descricao_cnae,
  d.ability_to_win,
  d.qtd_skus_potenciais,
  f.dsr_3m AS dsr_ultimos_3m,
  g.dsr_12m AS dsr_ultimos_12m,
  h.dsr_ever AS dsr_desde_sempre,
  i.data_ultima_compra_fs,
  NVL(j.status, a.status_operador) AS status_operador_fs,
  CASE WHEN j.status = 'Churn' THEN 'Sim' ELSE 'Não' END AS churn,
  k.cesta_I12m,
  l.cesta_I3m,
  CASE 
    WHEN m.cnpj_total IN (
      SELECT DISTINCT LPAD(cnpj, 14, '0') 
      FROM schema_base.tabela_carteira_hist
    ) THEN 'Sim' ELSE 'Não'
  END AS carteirizado_historicamente,
  q.contatos,
  r.tons_12m,
  s.tons_3m,
  t.fat_12m,
  u.fat_3m
FROM
  schema_base.tabela_base_ipiranga AS a
LEFT JOIN schema_base.tabela_operador AS b ON b.operador * 1 = a.cnpj * 1
LEFT JOIN schema_base.tabela_carteira_atual AS c ON a.cnpj * 1 = c.cnpj * 1
LEFT JOIN schema_base.tabela_ability_score AS d ON a.cnpj * 1 = d.cnpj * 1
LEFT JOIN schema_base.tabela_sintegra AS e ON a.cnpj * 1 = e.cnpj * 1
LEFT JOIN dsr_3months AS f ON f.operador_id * 1 = a.cnpj * 1
LEFT JOIN dsr_12months AS g ON g.operador_id * 1 = a.cnpj * 1
LEFT JOIN dsr_ever AS h ON h.operador_id * 1 = a.cnpj * 1
LEFT JOIN dt_ultima_compra_fs AS i ON i.operador_id * 1 = a.cnpj * 1
LEFT JOIN status_operador AS j ON j.operador_id * 1 = a.cnpj * 1
LEFT JOIN cesta_I12months AS k ON k.operador_id * 1 = a.cnpj * 1
LEFT JOIN cesta_I3months AS l ON l.operador_id * 1 = a.cnpj * 1
LEFT JOIN schema_base.tabela_base_receita AS m ON a.cnpj * 1 = m.cnpj_total * 1
LEFT JOIN schema_base.tabela_cnae AS n ON m.cnae_fiscal_principal = n.codigo_cnae
LEFT JOIN schema_base.tabela_regiao AS p ON m.codigo_siafi = p.codigo_siafi
LEFT JOIN schema_base.tabela_contato_top3 AS q ON q.cnpj * 1 = a.cnpj * 1
LEFT JOIN tons_12months AS r ON r.operador_id * 1 = a.cnpj * 1
LEFT JOIN tons_3months AS s ON s.operador_id * 1 = a.cnpj * 1
LEFT JOIN fat_12months AS t ON t.operador_id * 1 = a.cnpj * 1
LEFT JOIN fat_3months AS u ON u.operador_id * 1 = a.cnpj * 1
LEFT JOIN schema_base.tabela_cesta_sugerida AS v ON v.cnpj = a.cnpj
"""

tabela_final = spark.sql(tabela_final_query)
tabela_final.createOrReplaceTempView("tabela_final")

tabela_final.show()