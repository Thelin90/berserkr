from src.apps.spark_session import InitSpark


def main():
    # Initialise SparkSession
    _init_spark_session = InitSpark("berserkr", "spark-warehouse")
    _default_spark_session = _init_spark_session.spark_init()
    _default_spark_session.sparkContext.setLogLevel("WARN")
