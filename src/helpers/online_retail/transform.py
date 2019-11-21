from typing import Optional

from datetime import datetime

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame


def transform_online_retail(
    sc: SparkSession,
    raw_rdd: RDD,
    schema: str,
    max_month: Optional[int] = None,
) -> DataFrame:
    """Method to transform online retail dataset to its correct dataformats, specific
    for online retail

    :return:
    """

    # initial transformation of the raw RDD
    raw_rdd = raw_rdd.map(lambda retail: (
        retail[0],  # InvoiceNo
        retail[1],  # StockCode
        retail[2] if retail[2] != '' else None,  # Description
        int(retail[3]),  # Quantity
        datetime.strptime(retail[4], '%d/%m/%Y %H:%M') if
        int(retail[4].split('/')[1]) < max_month
        else datetime.strptime(retail[4], '%m/%d/%Y %H:%M'),  # InvoiceDate
        float(retail[5]),  # UnitPrice
        int(retail[6]) if retail[6] != '' else None,  # CustomerID
        retail[7] if retail[7] != '' else None)  # Country
    )

    return sc.createDataFrame(
        raw_rdd,
        schema=schema
    )
