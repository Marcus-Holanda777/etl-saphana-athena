# SAP HANA para Athena

Este projeto permite a exportação de tabelas do SAP HANA para arquivos Parquet temporários e, em seguida, a importação desses dados para o AWS Athena. A interface do projeto é construída com [Textualize](https://textual.textualize.io/), proporcionando uma experiência interativa no terminal.

## Tecnologias Utilizadas

- [Textualize](https://github.com/Textualize/textual) - Interface de usuário no terminal
- [SQLAlchemy](https://www.sqlalchemy.org/) - ORM para interagir com bancos de dados
- [SQLAlchemy-HANA](https://github.com/SAP/sqlalchemy-hana) - Driver do SQLAlchemy para SAP HANA
- [Pandas](https://pandas.pydata.org/) - Manipulação e exportação de dados
- [AWS Athena](https://aws.amazon.com/athena/) - Consulta de dados no S3 com SQL

## Funcionalidades

- Conexão com o SAP HANA para leitura de tabelas
- Exportação de dados para arquivos Parquet de forma temporária
- Upload dos arquivos Parquet para o S3
- Registro da tabela no AWS Athena para consulta
- Interface interativa para configuração e execução das exportações

## Capturas de Tela

Adicione aqui as imagens das telas do aplicativo:

![config SAP](img/config_sap.png)
![config ATHENA](img/config_athena.png)
![insert](img/insert.png)
![execute](img/execute.png)

## Como Usar

### 1. Instalar dependências

```bash
pip install -r requirements.txt
```

### 2. Configurar Conexões

As configurações de conexão são armazenadas em um arquivo JSON. O usuário poderá inserir os dados pela interface na primeira execução.

Exemplo de `config.json`:

```json
{
    "sap_hana": {
        "host": "hostname",
        "port": 30015,
        "user": "username",
        "password": "password"
    },
    "aws_athena": {
        "s3_bucket": "seu-bucket",
        "database": "seu-database"
    }
}
```

### 3. Executar o Aplicativo

```bash
python main.py
```

A interface interativa permitirá selecionar tabelas, configurar a exportação e acompanhar o progresso do processo.

## Estrutura do Projeto

```
📦 projeto
 ┣ 📂 src
 ┃ ┣ 📜 main.py            # Arquivo principal que inicia a interface
 ┃ ┣ 📜 config.py          # Gerenciamento de configurações
 ┃ ┣ 📜 exporter.py        # Lógica de exportação SAP HANA → Parquet → Athena
 ┃ ┣ 📜 ui.py              # Interface com Textualize
 ┣ 📜 requirements.txt     # Dependências do projeto
 ┣ 📜 README.md            # Documentação do projeto
 ┣ 📜 config.json          # Configurações de conexão (criado na primeira execução)
```

## Contribuição

Sinta-se à vontade para abrir issues ou enviar pull requests com melhorias e sugestões!

## Licença

Este projeto é licenciado sob a MIT License.

