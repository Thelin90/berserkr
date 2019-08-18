"""

This class will clean the data, perhaps create some new features from the existing one if possible,
then save it on S3 as a parquet file.


The machine learning app of the project will then read this file, and utilize pandas udf, featuretools


"""