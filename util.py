import random
import string
import uuid

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def init_spark(conf=None) -> SparkSession:
    builder = pyspark.sql.SparkSession.builder.appName("MyApp")
    if conf is not None:
        builder.config(conf=conf)
    builder\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.databricks.delta.checkLatestSchemaOnRead", "false")

    return configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4",
                                                                   "com.amazonaws:aws-java-sdk-bundle:1.12.310"]).getOrCreate()


def build_row():
    return (uuid.uuid4().hex, ''.join(random.choice(string.ascii_letters) for i in range(10)))