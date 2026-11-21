import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.process_properties import (
    create_spark_session,
    load_properties,
    save_curated_data,
    transform_properties,
    validate_curated_data,
)


class SparkEnvironmentTests(unittest.TestCase):

    def test_windows_sets_default_and_preserves_existing_configuration(self):
        cases = [
            ({}, r"C:\hadoop"),
            ({"HADOOP_HOME": ""}, r"C:\hadoop"),
            ({"HADOOP_HOME": r"C:\other"}, r"C:\other"),
        ]

        for environment, expected_home in cases:
            with self.subTest(environment=environment):
                with patch.dict(
                    os.environ,
                    {"PATH": r"C:\tools", **environment},
                    clear=True,
                ):
                    with patch("src.process_properties.os.name", "nt"):
                        with patch("src.process_properties.SparkSession"):
                            create_spark_session()
                            create_spark_session()

                    self.assertEqual(os.environ["HADOOP_HOME"], expected_home)

                    expected_bin = os.path.join(expected_home, "bin")
                    self.assertEqual(
                        os.environ["PATH"].split(os.pathsep).count(expected_bin),
                        1,
                    )

    def test_other_platforms_do_not_change_environment(self):
        with patch.dict(os.environ, {"PATH": "/usr/bin"}, clear=True):
            with patch("src.process_properties.os.name", "posix"):
                with patch("src.process_properties.SparkSession"):
                    create_spark_session()

            self.assertEqual(dict(os.environ), {"PATH": "/usr/bin"})


class ProcessPropertiesTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_loads_multiline_json_with_explicit_schema(self):
        reader_type = type(self.spark.read)
        read_json = reader_type.json

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "properties.json"

            path.write_text(
                '[\n  {"id_anuncio": "1", "preco": " 100,50 ", '
                '"area_m2": " 2,5 ", "quartos": 2}\n]',
                encoding="utf-8",
            )

            with patch.object(
                reader_type,
                "json",
                autospec=True,
                side_effect=lambda reader, _: read_json(reader, str(path)),
            ):
                properties = load_properties(self.spark)

            self.assertEqual(
                properties.schema["preco"].dataType,
                StringType(),
            )
            self.assertEqual(
                properties.schema["area_m2"].dataType,
                StringType(),
            )
            self.assertEqual(
                properties.schema["quartos"].dataType,
                IntegerType(),
            )

            row = properties.collect()[0]

            self.assertEqual(row.preco, " 100,50 ")
            self.assertEqual(row.area_m2, " 2,5 ")

            curated = transform_properties(properties).collect()[0]

            self.assertEqual(curated.preco_m2, 40.2)

    def test_curates_identifiers_text_and_numeric_values(self):
        schema = StructType(
            [
                StructField("id_anuncio", StringType()),
                StructField("cidade", StringType()),
                StructField("bairro", StringType()),
                StructField("tipo_imovel", StringType()),
                StructField("finalidade", StringType()),
                StructField("fonte", StringType()),
                StructField("preco", DoubleType()),
                StructField("area_m2", DoubleType()),
                StructField("quartos", IntegerType()),
                StructField("banheiros", IntegerType()),
                StructField("vagas", IntegerType()),
            ]
        )

        text = (
            " Cidade ",
            " Bairro ",
            " Casa ",
            " Venda ",
            " Fonte ",
        )

        valid = ("valid", *text, 100.0, 20.0, 2, 1, 0)

        rows = [
            valid,
            ("valid", *text, 100.0, 20.0, 2, 1, 1),
            (None, *text, 100.0, 20.0, 2, 1, 0),
            ("", *text, 100.0, 20.0, 2, 1, 0),
            ("   ", *text, 100.0, 20.0, 2, 1, 0),
            ("negative", *text, -1.0, -2.0, -3, -4, -5),
            ("zero", *text, 100.0, 0.0, 0, 0, 0),
            ("small", *text, 100.0, 0.01, 1, 1, 1),
            ("missing", *text, 100.0, None, None, None, None),
            ("zero-price", *text, 0.0, 20.0, 1, 1, 1),
        ]

        curated = transform_properties(
            self.spark.createDataFrame(rows, schema)
        )

        curated_rows = curated.collect()
        results = {row.id_anuncio: row for row in curated_rows}

        self.assertEqual(len(curated_rows), 6)

        self.assertEqual(
            set(results),
            {
                "valid",
                "negative",
                "zero",
                "small",
                "missing",
                "zero-price",
            },
        )

        self.assertEqual(
            tuple(
                results["valid"][name]
                for name in (
                    "cidade",
                    "bairro",
                    "tipo_imovel",
                    "finalidade",
                    "fonte",
                )
            ),
            ("Cidade", "Bairro", "Casa", "Venda", "Fonte"),
        )

        self.assertEqual(results["valid"].preco_m2, 5.0)
        self.assertEqual(results["small"].preco_m2, 10000.0)

        for name in (
            "preco",
            "area_m2",
            "quartos",
            "banheiros",
            "vagas",
        ):
            self.assertIsNone(results["negative"][name])

        for identifier in (
            "negative",
            "zero",
            "missing",
            "zero-price",
        ):
            self.assertIsNone(results[identifier].preco_m2)

        self.assertEqual(results["zero"].area_m2, 0.0)
        self.assertIsNone(results["missing"].area_m2)

    def test_converts_numeric_text_and_keeps_invalid_values_null(self):
        schema = StructType(
            [
                StructField("id_anuncio", StringType()),
                StructField("cidade", StringType()),
                StructField("bairro", StringType()),
                StructField("tipo_imovel", StringType()),
                StructField("finalidade", StringType()),
                StructField("fonte", StringType()),
                StructField("preco", StringType()),
                StructField("area_m2", StringType()),
                StructField("quartos", IntegerType()),
                StructField("banheiros", IntegerType()),
                StructField("vagas", IntegerType()),
            ]
        )

        text = (
            "cidade",
            "bairro",
            "tipo",
            "finalidade",
            "fonte",
        )

        rows = [
            ("comma", *text, " 100,50 ", " 2,5 ", 1, 1, 1),
            ("dot", *text, " 25.0 ", " 0.5 ", 1, 1, 1),
            ("invalid", *text, "inválido", "", 1, 1, 1),
            ("negative", *text, " -3,5 ", " -2 ", 1, 1, 1),
            ("missing", *text, None, " 2 ", 1, 1, 1),
        ]

        curated = transform_properties(
            self.spark.createDataFrame(rows, schema)
        )

        self.assertEqual(
            curated.schema["preco"].dataType,
            DoubleType(),
        )
        self.assertEqual(
            curated.schema["area_m2"].dataType,
            DoubleType(),
        )

        ansi = self.spark.conf.get("spark.sql.ansi.enabled")

        try:
            self.spark.conf.set("spark.sql.ansi.enabled", "true")

            results = {
                row.id_anuncio: row
                for row in curated.collect()
            }
        finally:
            self.spark.conf.set("spark.sql.ansi.enabled", ansi)

        self.assertEqual(
            (
                results["comma"].preco,
                results["comma"].area_m2,
            ),
            (100.5, 2.5),
        )

        self.assertEqual(
            results["comma"].preco_m2,
            40.2,
        )

        self.assertEqual(
            results["dot"].preco_m2,
            50.0,
        )

        for identifier in (
            "invalid",
            "negative",
            "missing",
        ):
            self.assertIsNone(results[identifier].preco)
            self.assertIsNone(results[identifier].preco_m2)

        self.assertIsNone(results["invalid"].area_m2)
        self.assertIsNone(results["negative"].area_m2)

    def test_quality_report_counts_nulls_nonpositive_values_and_all_groups(
        self,
    ):
        schema = StructType(
            [
                StructField("preco", DoubleType()),
                StructField("area_m2", DoubleType()),
                StructField("preco_m2", DoubleType()),
                StructField("tipo_imovel", StringType()),
                StructField("cidade", StringType()),
                StructField("finalidade", StringType()),
            ]
        )

        rows = [
            (
                10.0,
                5.0,
                2.0,
                "Casa",
                f"Cidade_{i:02}",
                "Venda",
            )
            for i in range(21)
        ]

        rows.extend(
            [
                (None, None, None, None, None, None),
                (0.0, 0.0, None, "Casa", "Cidade_00", "Aluguel"),
                (5.0, 0.0, None, "Casa", "Cidade_00", "Venda"),
            ]
        )

        properties = self.spark.createDataFrame(rows, schema)

        output = io.StringIO()

        with redirect_stdout(output):
            validate_curated_data(properties)

        report = output.getvalue()

        for line in (
            "Total de registros: 24",
            "Preço nulo: 1",
            "Área nula: 1",
            "Área <= 0: 2",
            "Preço nulo ou <= 0: 2",
            "Preço por m² nulo: 3",
            "Registros por tipo_imovel:",
            "Casa: 23",
            "None: 1",
            "Registros por cidade:",
            "Cidade_00: 3",
            "Cidade_20: 1",
            "Registros por finalidade:",
            "Aluguel: 1",
            "Venda: 22",
        ):
            self.assertIn(line, report)

        output = io.StringIO()

        with redirect_stdout(output):
            validate_curated_data(properties.limit(0))

        self.assertIn(
            "Total de registros: 0",
            output.getvalue(),
        )

        self.assertIn(
            "Preço nulo ou <= 0: 0",
            output.getvalue(),
        )

    def test_saves_and_reads_snappy_parquet(self):
        properties = self.spark.createDataFrame(
            [("1", 10.0)],
            ["id_anuncio", "preco_m2"],
        )

        with tempfile.TemporaryDirectory() as directory:
            path = (
                Path(directory)
                / "curated"
                / "properties.parquet"
            )

            save_curated_data(properties, str(path))

            self.assertTrue(
                list(path.glob("*.snappy.parquet"))
            )

            self.assertEqual(
                self.spark.read.parquet(str(path)).collect(),
                properties.collect(),
            )

            replacement = self.spark.createDataFrame(
                [("2", 20.0)],
                ["id_anuncio", "preco_m2"],
            )

            save_curated_data(replacement, str(path))

            self.assertTrue(
                list(path.glob("*.snappy.parquet"))
            )

            self.assertEqual(
                self.spark.read.parquet(str(path)).collect(),
                replacement.collect(),
            )


if __name__ == "__main__":
    unittest.main()