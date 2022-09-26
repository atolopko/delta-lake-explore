import sys
from time import sleep

from delta.tables import *
from pyspark.sql.types import StringType

from util import init_spark, build_row
from table import Table

"""
Experiment with s3 as storage layer
"""

if __name__ == '__main__':
    s = init_spark()

    schema = StructType([StructField("id", StringType(), nullable=False),
                         StructField("name", StringType(), nullable=False)])

    t = Table(s, "s3a://atolopko-tmp/delta-tables/test_table", schema)
    rows = [build_row() for _ in range(10)]
    t.insert_rows(rows)

    t_read = Table(s, "test_table", schema)
    t_read.show()



# TODO: Test if a second df instance reflects changes after df.write.mode('append'), to assess whether df is disk update-aware

# Does DeltaTable.toDF() reload entire Dataframe from disk or just latest changes (since it a journaled storage design)?  What's the performance like?