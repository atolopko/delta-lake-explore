import sys
from time import sleep

from delta.tables import *
from pyspark.sql.types import StringType

from util import init_spark, build_row
from table import Table

"""
Experiment with a writer job and reader jobs in separate processes.
Take-aways:
1. Reader process with long-lived Spark session will refresh from disk. Local disk storage, count query takes ~200ms with 
no data changes, and takes ~1 sec when data changes on disk, regardless of size. Size of data changes does not have
measurable impact, testing adding 1 new row vs 100K's new rows.
2. To support concurrent reads on S3, you need to use DynamoDb for Delta table log management:
https://docs.delta.io/latest/delta-storage.html#multi-cluster-setup. S3 does not provide the guarantees required of the
storage system (https://docs.delta.io/latest/delta-storage.html)
"""

if __name__ == '__main__':
    s = init_spark()

    # DeltaTable.forName(s, "/tmp/test_table").toDF().show()

    schema = StructType([StructField("id", StringType(), nullable=False),
                         StructField("name", StringType(), nullable=False)])

    t = Table(s, "test_table", schema)

    if sys.argv[1] == 'insert':
        rows = [build_row() for _ in range(int(sys.argv[2]))]
        t.insert_rows(rows)
    elif sys.argv[1] == 'show':
        while True:
            t.show()
            sleep(2)



# TODO: Test if a second df instance reflects changes after df.write.mode('append'), to assess whether df is disk update-aware

# Does DeltaTable.toDF() reload entire Dataframe from disk or just latest changes (since it a journaled storage design)?  What's the performance like?