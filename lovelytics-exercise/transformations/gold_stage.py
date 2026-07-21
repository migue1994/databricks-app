from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dp.view(
    name="fact_ventas_transformada"
)
def fact_ventas_final():
    fact = (
        spark
        .readStream
        .option("readChangeFeed", "true")
        .table("pipeline_way.silver.fact_ventas")
    )

    return (
        fact
        .filter(col("mes") != 12)
        .withColumn(
            "monto_total",
            when(col("mes") != 6, col("precio_unitario") * col("cantidad"))
            .otherwise(col("precio_unitario") * col("cantidad") * 0.9)
        )
    )

dp.create_streaming_table(
    name="pipeline_way.gold.fact_ventas_final",
    cluster_by=["mes"]
)

dp.create_auto_cdc_flow(
    target="pipeline_way.gold.fact_ventas_final",
    source="fact_ventas_transformada",
    keys=["id_vendedor", "mes", "sku", "dia", "anio"],
    sequence_by=col("_commit_timestamp"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type="1"
)