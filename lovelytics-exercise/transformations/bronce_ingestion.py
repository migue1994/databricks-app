from pyspark.sql import DataFrame
from pyspark import pipelines as dp
from pyspark.sql.functions import *

volume_path = "/Volumes/pipeline_way/bronce/input_data/archivos/archivos/"
cloud_files_schema_location = "/Volumes/pipeline_way/bronce/input_data/archivos/schema/"

def create_bronze_ingestion(name: str, sep: str) -> None:
    @dp.table(
        name=f"pipeline_way.bronce.{name}"
    )
    def get_table() -> DataFrame:
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("sep", sep)
            .option("cloudFiles.schemaLocation", f"{cloud_files_schema_location}/{name}")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{volume_path}/{name}")
        )

tables = [("empleado", ","), ("fact", ";"), ("producto", ";"), ("local", ";")]

[create_bronze_ingestion(x, y) for x, y in tables]