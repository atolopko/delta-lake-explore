import pyspark
from pyspark.sql import DataFrame
from delta import *
from datetime import datetime
from time import sleep
import shutil
from delta.tables import *
from pyspark.sql.functions import col

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

s = configure_spark_with_delta_pip(builder).getOrCreate()


def make_df(rows: int, ts: datetime, version: int) -> DataFrame:
    data = [(row, ts, version) for row in range(0, rows)]
    return s.createDataFrame(data)
    

table_name = 'delta_table'

shutil.rmtree(table_name, ignore_errors=True)

for version in range(0, 3):
    make_df(rows=1, ts=datetime.now(), version=version).\
        write.format('delta').mode('append').save(table_name)
    sleep(3)

print("Table history:")
delta_table = DeltaTable.forPath(s, table_name)
delta_table.history().show()
timestamps = dict(
    [(row.version, row.timestamp) for row in 
    delta_table.history().orderBy(col('version').desc()).
     select('version', 'timestamp').collect()])

print("Latest version:")
s.read.format('delta').load(table_name).show(truncate=False)

for version in range(0, 3):
    print(f"Version ordinal={version}:")
    s.read.format('delta').option("versionAsOf", version).\
        load(table_name).show(truncate=False)

    ts = timestamps[version]
    print(f"Version ts={ts}:")
    s.read.format('delta').option("timestampAsOf", ts).\
        load(table_name).show(truncate=False) 

