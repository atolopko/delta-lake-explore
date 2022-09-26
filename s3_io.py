import sys
from time import sleep

from delta.tables import *
from pyspark.sql.types import StringType

from util import init_spark, build_row
from table import Table

"""
Experiment with s3 as storage layer

* 1-1.5 sec/read. Slow!
* spark.databricks.delta.checkLatestSchemaOnRead can be used avoid checking, so clearly this means an S3 round-trip
  is occurring on every read (tested by deleting table and next query did fail, as expected). 
"""

if __name__ == '__main__':
    s = init_spark()

    schema = StructType([StructField("id", StringType(), nullable=False),
                         StructField("name", StringType(), nullable=False)])

    tbl_loc = sys.argv[1] or exit(1)
    t = Table(s, tbl_loc, schema)
    rows = [build_row() for _ in range(10)]
    t.insert_rows(rows)

    t_read = Table(s, tbl_loc, schema)
    t_read.show()
