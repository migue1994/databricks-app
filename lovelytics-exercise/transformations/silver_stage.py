from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


@dp.table(
    name="pipeline_way.silver.dim_vendedor"
)
def dim_vendedor() -> DataFrame:
    empleado = spark.readStream.table("pipeline_way.bronce.empleado")
    local = spark.readStream.table("pipeline_way.bronce.local")
    return (
        empleado.alias("a")
        .join(local.alias("b"), col("a.sucursal") == col("b.id_sucursal"), "inner")
        .select(
            col("a.id_vendedor"),
            col("a.nombre").alias("vendedor_nombre"),
            col("b.nombre").alias("sucursal_nombre"),
            col("b.tipo").alias("region")
        )
    )

@dp.table(
    name="pipeline_way.silver.dim_producto"
)
def dim_producto() -> DataFrame:
    productos = spark.readStream.table("pipeline_way.bronce.producto")
    cols = [col(x).alias(x) for x in productos.columns if x != "_rescued_data"]
    return(
        productos
        .select(*cols)
    )

@dp.table(
    name="pipeline_way.silver.fact_ventas"
)
def fact_ventas() -> DataFrame:
    fact = spark.readStream.table("pipeline_way.bronce.fact")
    dim_vendedor = spark.readStream.table("pipeline_way.silver.dim_vendedor")
    dim_producto = spark.readStream.table("pipeline_way.silver.dim_producto")
    cols_a = [col(x).alias(x) for x in fact.columns if x != "_rescued_data"]
    cols_b = [col(x).alias(x) for x in dim_vendedor.columns]
    cols_c = [col(x).alias(x) for x in dim_producto.columns]
    return(
        fact.alias("a")
        .join(dim_vendedor.alias("b"), col("a.vendedor") == col("b.id_vendedor"), "inner")
        .join(dim_producto.alias("c"), col("a.sku") == col("c.id_producto"), "inner")
        .select(
            *(
                cols_a +
                cols_b +
                cols_c +
                [dayofmonth(col("timestamp")).alias("dia")] +
                [month(col("timestamp")).alias("mes")] +
                [year(col("timestamp")).alias("anio")]
            )
        )
    )








