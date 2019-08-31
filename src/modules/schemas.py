from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, DateType


class OnlineRetailSchema(object):
    INITIAL_SCHEMA = StructType(
        [
            StructField('InvoiceNo', StringType(), False),
            StructField('StockCode', StringType(), False),
            StructField('Description', StringType(), True),
            StructField('Quantity', IntegerType(), False),
            StructField('InvoiceDate', DateType(), False),
            StructField('UnitPrice', DoubleType(), False),
            StructField('CustomerID', IntegerType(), True),
            StructField('Country', StringType(), True)
        ]
    )
