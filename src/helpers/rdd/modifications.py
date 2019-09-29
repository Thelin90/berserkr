from pyspark import RDD


def remove_header(rdd: RDD) -> RDD:
    header_rdd: RDD = rdd.first()
    return rdd.filter(lambda row: row != header_rdd)
