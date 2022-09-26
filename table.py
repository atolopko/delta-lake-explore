from typing import Tuple, List

import funcy
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class Table:

    @staticmethod
    def create(spark: SparkSession, name: str, schema: StructType) -> DeltaTable:
        table_builder = DeltaTable.createIfNotExists(sparkSession=spark)
        return table_builder.location(name).addColumns(cols=schema).execute() # #partitionedBy('id') # .tableName(name).

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
        new_df = self.spark.createDataFrame(records, schema=self.schema)
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