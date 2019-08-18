from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, DateType


class OnlineRetailSchema(object):
    INITIAL_SCHEMA = StructType(
        [
            StructField('InvoiceNo', StringType(), False),
            StructField('StockCode', StringType(), False),
            StructField('Description', StringType(), True),
            StructField('Quantity', StringType(), False),
            StructField('InvoiceDate', StringType(), False),
            StructField('UnitPrice', StringType(), False),
            StructField('CustomerID', StringType(), True),
            StructField('Country', StringType(), True)
        ]
    )
