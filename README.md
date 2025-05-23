# Projeto de Enriquecimento de Dados Unilever

Este projeto foi desenvolvido para automatizar o processo de enriquecimento de dados solicitado via formulário interno da Unilever, utilizando tecnologias como **Databricks**, **Azure Blob Storage**, **Power Automate** e **PySpark**.

---

## ⚠️ Aviso importante

- **Este código foi desenvolvido para rodar exclusivamente no ambiente Databricks** com acesso configurado ao Azure Data Lake e DBFS.  
- **Não é compatível com execução em ambientes locais ou IDEs tradicionais** sem adaptações específicas.  
- O uso de `dbutils`, `spark` e a integração com Azure Blob Storage depende de serviços e permissões disponíveis somente na infraestrutura interna da Unilever.

---

## Estrutura do projeto

O fluxo de enriquecimento de dados segue a seguinte sequência:

1. ✅ O solicitante preenche um **formulário via Forms** inserindo um arquivo com os CNPJs que deseja enriquecer e os campos desejados.
2. 📤 Ao ser enviado, **o formulário aciona automaticamente um fluxo** que gera um arquivo `.xlsx` com os dados solicitados e o envia para o **Azure Blob Storage** em um diretório monitorado (`Arquivos_enriquecimento`).
3. ⏱️ Em um horário programado, o **código roda no Databricks**, acessa esse diretório e **lê o arquivo mais recente enviado**.
4. 📊 O código **processa e enriquece os dados automaticamente**, unindo diferentes fontes internas para compor a base final com os campos solicitados.
5. 🔁 **Após a geração da base**, o código se reexecuta para verificar se há novos arquivos; **não encontrando nenhum, remove os arquivos antigos** da pasta de entrada.
6. 📥 As bases enriquecidas geradas são **armazenadas em outra pasta** no Azure (`Arquivos_enriquecidos`) e o Power Automate é acionado para **enviar automaticamente a base por e-mail ao solicitante** com base no ID e e-mail coletados no formulário.

---

## Como utilizar

> ⚠️ **Este projeto é interno e automatizado. A execução é agendada e dependente de integrações com serviços da Unilever.**

1. Verifique se o formulário foi preenchido corretamente e enviado.
2. O fluxo será iniciado automaticamente pelo Power Automate.
3. O código será executado no Databricks conforme o agendamento.
4. O solicitante receberá o arquivo final por e-mail assim que o processo for concluído.

---

## Imagens do funcionamento

### 1. Arquivos recebidos no Azure Blob via Forms
![image](https://github.com/user-attachments/assets/8a27933a-0498-4a1d-b095-b4f6fd6696f0)

---

### 2. Base enriquecida armazenada no destino
![image](https://github.com/user-attachments/assets/0bd80c65-6ef7-4c58-966b-a634e76bfb3f)

---

### 3. Envio automático por e-mail via Power Automate
![image](https://github.com/user-attachments/assets/246b41f0-1aa3-40f0-b074-059eb81d76f7)


---

## Limitações

- O código depende integralmente do ambiente Databricks e da infraestrutura Azure da Unilever.
- O processo é automatizado e não deve ser executado manualmente sem autorização.
