import os

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def create_spark_session():
    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME") or r"C:\hadoop"
        os.environ["HADOOP_HOME"] = hadoop_home

        hadoop_bin = os.path.join(hadoop_home, "bin")
        path = os.environ.get("PATH", "")
        path_entries = [
            os.path.normcase(os.path.normpath(entry))
            for entry in path.split(os.pathsep)
            if entry
        ]

        if os.path.normcase(os.path.normpath(hadoop_bin)) not in path_entries:
            os.environ["PATH"] = hadoop_bin + os.pathsep + path

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("Process Properties")
        .getOrCreate()
    )

def load_properties(
    spark: SparkSession, path: str = "data/raw/properties.json"
) -> DataFrame:
    schema = StructType(
        [
            StructField("titulo", StringType(), True),
            StructField("preco", StringType(), True),
            StructField("moeda", StringType(), True),
            StructField("tipo_imovel", StringType(), True),
            StructField("finalidade", StringType(), True),
            StructField("endereco", StringType(), True),
            StructField("localizacao", StringType(), True),
            StructField("cidade", StringType(), True),
            StructField("bairro", StringType(), True),
            StructField("estado", StringType(), True),
            StructField("area_m2", StringType(), True),
            StructField("quartos", IntegerType(), True),
            StructField("banheiros", IntegerType(), True),
            StructField("vagas", IntegerType(), True),
            StructField("url", StringType(), True),
            StructField("id_anuncio", StringType(), True),
            StructField("fonte", StringType(), True),
        ]
    )
    return (
        spark.read.schema(schema)
        .option("multiLine", True)
        .json(path)
    )


def transform_properties(properties: DataFrame) -> DataFrame:
    curated = properties.filter(
        F.col("id_anuncio").isNotNull() & (F.trim(F.col("id_anuncio")) != "")
    ).dropDuplicates(["id_anuncio"])

    for column in ("cidade", "bairro", "tipo_imovel", "finalidade", "fonte"):
        curated = curated.withColumn(column, F.trim(F.col(column)))

    for column in ("preco", "area_m2"):
        curated = curated.withColumn(
            column,
            F.regexp_replace(F.trim(F.col(column)), ",", ".").try_cast(DoubleType()),
        )

    for column in ("preco", "area_m2", "quartos", "banheiros", "vagas"):
        curated = curated.withColumn(
            column,
            F.when(F.col(column) < 0, None).otherwise(F.col(column)),
        )

    return curated.withColumn(
        "preco_m2",
        F.when(
            (F.col("preco") > 0) & (F.col("area_m2") > 0),
            F.col("preco") / F.col("area_m2"),
        ),
    )


def validate_curated_data(properties: DataFrame) -> None:
    metrics = properties.agg(
        F.count("*").alias("total"),
        F.count(F.when(F.col("preco").isNull(), 1)).alias("preco_nulo"),
        F.count(F.when(F.col("area_m2").isNull(), 1)).alias("area_nula"),
        F.count(F.when(F.col("area_m2") <= 0, 1)).alias("area_nao_positiva"),
        F.count(
            F.when(F.col("preco").isNull() | (F.col("preco") <= 0), 1)
        ).alias("preco_nulo_ou_nao_positivo"),
        F.count(F.when(F.col("preco_m2").isNull(), 1)).alias("preco_m2_nulo"),
    ).collect()[0]

    print("\nValidação dos dados curados:")
    print(f"  Total de registros: {metrics['total']}")
    print(f"  Preço nulo: {metrics['preco_nulo']}")
    print(f"  Área nula: {metrics['area_nula']}")
    print(f"  Área <= 0: {metrics['area_nao_positiva']}")
    print(f"  Preço nulo ou <= 0: {metrics['preco_nulo_ou_nao_positivo']}")
    print(f"  Preço por m² nulo: {metrics['preco_m2_nulo']}")

    for column in ("tipo_imovel", "cidade", "finalidade"):
        print(f"\n  Registros por {column}:")
        for row in properties.groupBy(column).count().orderBy(column).toLocalIterator():
            print(f"    {row[column]}: {row['count']}")


def save_curated_data(
    properties: DataFrame, path: str = "data/curated/properties.parquet"
) -> None:
    properties.write.mode("overwrite").option("compression", "snappy").parquet(path)


def main():
    raw_path = os.environ.get("RAW_DATA_PATH", "data/raw/properties.json")
    curated_path = os.environ.get("CURATED_DATA_PATH", "data/curated/properties.parquet")
    spark = create_spark_session()
    try:
        properties = load_properties(spark, raw_path)
        print(f"Registros lidos: {properties.count()}")

        curated = transform_properties(properties)
        print(f"Registros após a curadoria: {curated.count()}")
        curated.printSchema()
        curated.show(10, truncate=False)
        validate_curated_data(curated)
        save_curated_data(curated, curated_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
