# Mercado Imobiliário Batch

Este repositório corresponde à etapa Batch do pipeline de dados Mercado Imobiliário Regional.

A etapa recebe artefatos brutos por armazenamento e os processa localmente com PySpark. O script lê `data/raw/properties.json` com schema explícito e grava `data/curated/properties.parquet` com compressão Snappy.

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

O Spark cria o diretório de saída automaticamente e sobrescreve os dados curados existentes a cada execução.

No Windows, a gravação local pode exigir uma instalação compatível do Hadoop com `winutils.exe` em `%HADOOP_HOME%\bin`.
