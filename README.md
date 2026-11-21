# Mercado Imobiliário Batch

Este repositório corresponde à etapa Batch do pipeline de dados Mercado Imobiliário Regional.

A etapa recebe artefatos brutos por armazenamento e os processa localmente com PySpark. Por padrão, o script lê `data/raw/properties.json` com schema explícito e grava `data/curated/properties.parquet` com compressão Snappy.

## Curadoria e validação

- Remove registros sem `id_anuncio` e duplicidades por esse identificador. Quando duplicatas possuem dados diferentes, o Spark mantém uma delas sem garantir qual será escolhida.
- Remove espaços nas extremidades de `cidade`, `bairro`, `tipo_imovel`, `finalidade` e `fonte`.
- Converte `preco` e `area_m2` para números, aceitando vírgula decimal e convertendo valores inválidos em nulos. Valores negativos dos campos numéricos também se tornam nulos.
- Calcula `preco_m2` somente quando preço e área são positivos; registros com área zero ou nula são mantidos.
- Exibe contagens de valores nulos ou não positivos e de registros por tipo de imóvel, cidade e finalidade.

## Execução

Com as dependências de `requirements.txt` instaladas, execute na raiz do projeto:

```shell
python src/process_properties.py
```

Os caminhos podem ser configurados por variáveis de ambiente:

- `RAW_DATA_PATH`: caminho do JSON bruto; padrão `data/raw/properties.json`.
- `CURATED_DATA_PATH`: caminho do Parquet curado; padrão `data/curated/properties.parquet`.

Sem essas variáveis, os caminhos padrão continuam válidos. Caminhos relativos são resolvidos a partir do diretório de execução; o armazenamento configurado precisa ser acessível pelo Spark.

O Spark cria o diretório de saída automaticamente e sobrescreve os dados curados existentes a cada execução.

No Windows, a gravação local pode exigir uma instalação compatível do Hadoop com `winutils.exe` em `%HADOOP_HOME%\bin`.

## Execução com Docker

Construa a imagem na raiz do projeto:

```powershell
docker build -t mercado-imobiliario-batch:latest .
```

Após a etapa Collection disponibilizar o JSON bruto em `/data/properties.json` no volume compartilhado `mercado-imobiliario-data`, execute no PowerShell:

```powershell
docker run --rm `
  -v mercado-imobiliario-data:/data `
  -e RAW_DATA_PATH=/data/properties.json `
  -e CURATED_DATA_PATH=/data/curated/properties.parquet `
  mercado-imobiliario-batch:latest
```

O Batch lê o artefato JSON produzido pela Collection e grava o Parquet curado com compressão Snappy em `/data/curated/properties.parquet`, no mesmo volume. Ele não importa código da Collection nem acessa diretamente a fonte imobiliária. As variáveis `RAW_DATA_PATH` e `CURATED_DATA_PATH` permitem configurar os caminhos sem alterar o código.

Para verificar os arquivos no volume:

```powershell
docker run --rm --mount type=volume,source=mercado-imobiliario-data,target=/data,readonly --entrypoint ls mercado-imobiliario-batch:latest -R /data
```
