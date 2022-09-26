import random
import string
import sys
import uuid
from time import sleep

import funcy
import pyspark
from delta import *
from delta.tables import *
from pyspark.sql.types import StringType

"""
Experiment with a writer job and reader jobs in separate processes.
Take-aways:
1. Reader process with long-lived Spark session will refresh from disk. Local disk storage, count query takes ~200ms with 
no data changes, and takes ~1 sec when data changes on disk, regardless of size. Size of data changes does not have
measurable impact, testing adding 1 new row vs 100K's new rows.
2. To support concurrent reads on S3, you need to use DynamoDb for Delta table log management:
https://docs.delta.io/latest/delta-storage.html#multi-cluster-setup. S3 does not provide the guarantees required of the
storage system (https://docs.delta.io/latest/delta-storage.html)


class Table:

    @staticmethod
    def create(spark: SparkSession, name: str, schema: StructType) -> DeltaTable:
        table_builder = DeltaTable.createIfNotExists(sparkSession=spark)
        return table_builder.location(name).tableName(name).addColumns(cols=schema).execute() # #partitionedBy('id')

    def __init__(self, spark: SparkSession, name: str, schema: StructType) -> None:
        super().__init__()
        self.spark = spark
        self.name = name
        self.schema = schema
        self._table = None

    @property
    def table(self) -> DeltaTable:
        if self._table is None:
            self._table = Table.create(self.spark, self.name, self.schema)
            print(self._table.detail)
        return self._table

    def insert_row(self, record: Tuple):
        self.insert_rows([record])

    @funcy.print_durations
    def insert_rows(self, records: List[Tuple]):
        new_df = s.createDataFrame(records, schema=self.schema)
        self.table.alias('existing'). \
            merge(new_df.alias('new'), "existing.id = new.id"). \
            whenNotMatchedInsertAll(). \
            execute()
        # whenNotMatchedInsert(values=dict(id='new.id', name='new.name')). \
        # self.table.vacuum()

    @funcy.print_durations
    def show(self):
        print(self.table.toDF().count())
        # DeltaTable.forName(s, 'crud_test').toDF().show()
        # df = s.read.format('delta').load('crud_test')


def init_spark() -> SparkSession:
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()


def build_row():
    return (uuid.uuid4().hex, ''.join(random.choice(string.ascii_letters) for i in range(10)))


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