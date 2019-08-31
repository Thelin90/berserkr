from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SQLContext


class InitSpark(object):

    def __init__(
            self,
            app_name,
            warehouse_location,
            endpoint_url,
            aws_access_key_id,
            aws_secret_access_key,
    ):
        self.app_name = app_name
        self.warehouse_location = warehouse_location
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def spark_init(self):
        """

        :return:
        """
        sc = SparkSession \
            .builder \
            .appName(self.app_name) \
            .config("spark.sql.warehouse.dir", self.warehouse_location) \
            .getOrCreate()

        sc.sparkContext.setLogLevel("WARN")
        # Enable Arrow-based columnar data transfers
        sc.conf.set("spark.sql.execution.arrow.enabled", "true")

        hadoop_conf = sc.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", self.endpoint_url)
        hadoop_conf.set("fs.s3a.access.key", self.aws_access_key_id)
        hadoop_conf.set("fs.s3a.secret.key", self.aws_secret_access_key)

        return sc
