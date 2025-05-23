id_email = spark.sql('''
SELECT a.Email
FROM arq_respostas a
LEFT JOIN id_anexo b
ON a.id = b.id
WHERE a.id = b.id
''')

id_email.createOrReplaceTempView("id_email")

id_email.show()