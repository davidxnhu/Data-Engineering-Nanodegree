import configparser
import os
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import DateType
from pyspark.sql.functions import split
from pyspark.sql.types import FloatType


config = configparser.ConfigParser()
config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create spark session.
    Returns:
        spark (SparkSession) - spark session 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

    
def main():
    """
    - Establishes spark session  
    
    - Process data and load back into S3

    """
    spark = create_spark_session()

    input_data = "./output/"
    output_data = "s3a://udacity-xn/DataEngineering/Capstone/"
    
    
    for filename in os.listdir(input_data):
        f = os.path.join(input_data, filename)
        if not filename == ".ipynb_checkpoints" and os.path.isdir(f):
            df = spark.read.parquet(f)
            if filename == "fact_immi" or filename == "dim_demographic_population" or filename == "dim_demographic_statistics":
                df.write.mode("overwrite").partitionBy('state_code').parquet(path=output_data + filename)
            else:
                df.write.mode("overwrite").parquet(path=output_data + filename)
    
if __name__ == "__main__":
    main()

