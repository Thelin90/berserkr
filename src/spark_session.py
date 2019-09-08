from pyspark.sql import SparkSession


class InitSpark(object):

    def __init__(
            self,
            app_name: str,
            warehouse_location: str,
            aws_endpoint_url: str,
            aws_access_key_id: str,
            aws_secret_access_key: str,
    ):
        self.app_name = app_name
        self.warehouse_location = warehouse_location
        self.aws_endpoint_url = aws_endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def spark_init(self) -> SparkSession:
        """Method to initialise the given SparkSession, generic

        :return: pyspark SparkSession
        """
        sc: SparkSession = SparkSession \
            .builder \
            .appName(self.app_name) \
            .config("spark.sql.warehouse.dir", self.warehouse_location) \
            .getOrCreate()

        # set log level
        sc.sparkContext.setLogLevel("WARN")

        # Enable Arrow-based columnar data transfers
        sc.conf.set("spark.sql.execution.arrow.enabled", "true")

        # configure s3 connection for read/write operation (native spark)
        hadoop_conf = sc.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", self.aws_endpoint_url)
        hadoop_conf.set("fs.s3a.access.key", self.aws_access_key_id)
        hadoop_conf.set("fs.s3a.secret.key", self.aws_secret_access_key)
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("spark.history.fs.logDirectory", 's3a://spark-logs-test/')

        return sc
