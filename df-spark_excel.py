from shutil import copyfile

temp_file = f"/tmp/{list(path.collect())[0][0]}"

caminho = f"dbfs:/mnt/adls_dev/BRAZIL/Legacy/ufs-br-udl/output/Arquivos_enriquecidos/{list(path.collect())[0][0]}"

base_final.toPandas().to_excel(temp_file, engine='openpyxl', index=False)

copyfile(temp_file, caminho)
