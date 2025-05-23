file_path = f"dbfs:/mnt/adls_dev/BRAZIL/Legacy/ufs-br-udl/input/projeto_star_schema/Arquivos_enriquecimento/{list(path.collect())[0][0]}"

dbutils.fs.rm(file_path, recurse=False)

print(f"Arquivo {file_path} removido com sucesso.")

# Esse comando é específico para Databricks e serve para reiniciar o notebook atual
dbutils.notebook.run(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(), 0)
