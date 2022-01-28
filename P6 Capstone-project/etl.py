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
from pyspark.sql.types import FloatType, IntegerType, LongType


config = configparser.ConfigParser()
config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create spark session.
    
    Returns:
        spark (SparkSession) - spark session
    """
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
    
    return spark


def SAS_to_date(date):
    """
    Transform SAS date
    
    Keyword arguments:
    date -- raw date in SAS format
    
    Returns:
       Transformed date in Datetype
    """
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
    
process_SAS_date_udf = udf(SAS_to_date, DateType())

def rename_columns(table, new_columns):
    """
    Rename as new column names
    
    Keyword arguments:
    table -- spark table to be renamed
    new_columns -- new column names
    
    Returns:
       Renamed table with new column names
    """  
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table


def process_labels(spark, input_data, output_data):
    """
    Extract, transform and load label files
    
    Keyword arguments:
    spark -- spark session
    input_data -- localtion of input data
    output_data -- target output repository
    
    """  
    with open("I94_SAS_Labels_Descriptions.SAS") as f:
        contents = f.readlines()
                
    country_code = {}
    for countries in contents[9:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country

    port_code = {}
    for ports in contents[302:962]:
        pair = ports.split('=')
        code, port = pair[0].strip().strip("''"), pair[1].strip('\t').strip().strip("'")
        port_code[code] = port

    state_code = {}
    for states in contents[981:1036]:
        pair = states.split('=')
        code, state = pair[0].strip().strip("''"), pair[1].strip('\t').strip().strip("'")
        state_code[code] = state

    visa_code = {}
    for visas in contents[1046:1049]:
        pair = visas.split('=')
        code, visa = pair[0].strip().strip("''"), pair[1].strip().strip("'")
        visa_code[code] = visa

    travel_code = {}
    for travels in contents[972:976]:
        pair = travels.split('=')
        code, travel = pair[0].strip().strip("''"), pair[1].strip('\n').strip(";").strip().strip("''")
        travel_code[code] = travel

    spark.createDataFrame(country_code.items(), ['code', 'country'])\
             .withColumn('code', col('code').cast(IntegerType()))\
             .write.mode("overwrite")\
             .parquet(path=output_data + 'country_code')

    spark.createDataFrame(port_code.items(), ['code', 'port'])\
             .write.mode("overwrite")\
             .parquet(path=output_data + 'port_code')

    spark.createDataFrame(state_code.items(), ['code', 'state'])\
             .write.mode("overwrite")\
             .parquet(path=output_data + 'state_code')

    spark.createDataFrame(visa_code.items(), ['code', 'visa'])\
             .withColumn('code', col('code').cast(IntegerType()))\
             .write.mode("overwrite")\
             .parquet(path=output_data + 'visa_code')

    spark.createDataFrame(travel_code.items(), ['code', 'travel'])\
             .withColumn('code', col('code').cast(IntegerType()))\
             .write.mode("overwrite")\
             .parquet(path=output_data + 'travel_code')

def process_fact(spark, input_data, output_data):
    """
    Extract, transform and load fact table
    
    Keyword arguments:
    spark -- spark session
    input_data -- localtion of input data
    output_data -- target output repository
    
    """  
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    
    fact_immi = df_spark.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', 'arrdate', 'depdate', 'i94mode', 'i94visa').distinct()\
                             .withColumn("immigration_id", monotonically_increasing_id())

    new_columns = ['cic_id', 'year', 'month', 'port_code', 'state_code', 'arrive_date', 'departure_date', 'travel_code', 'visa_code']
    fact_immi = rename_columns(fact_immi, new_columns)

    fact_immi = fact_immi.withColumn("country", lit("United States"))
    fact_immi = fact_immi.withColumn('arrive_date',  process_SAS_date_udf(col('arrive_date')))
    fact_immi = fact_immi.withColumn('departure_date',  process_SAS_date_udf(col('departure_date')))
    fact_immi = fact_immi.withColumn('cic_id',  col('cic_id').cast(IntegerType()))
    fact_immi = fact_immi.withColumn('year',  col('year').cast(IntegerType()))
    fact_immi = fact_immi.withColumn('month',  col('month').cast(IntegerType()))
    fact_immi = fact_immi.withColumn('travel_code',  col('travel_code').cast(IntegerType()))
    fact_immi = fact_immi.withColumn('visa_code',  col('visa_code').cast(IntegerType()))
     
    fact_immi.write.mode("overwrite").partitionBy('state_code')\
                        .parquet(path=output_data + 'fact_immi')

def process_dim_personal(spark, input_data, output_data):
    """
    Extract, transform and load dimension personal table
    
    Keyword arguments:
    spark -- spark session
    input_data -- localtion of input data
    output_data -- target output repository
    
    """  
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    dim_immi_personal = df_spark.select('cicid', 'i94cit', 'i94res', 'biryear', 'gender', 'insnum').distinct()\
                                    .withColumn("immi_person_id",  monotonically_increasing_id())

    new_columns = ['cic_id', 'citizen_country', 'residence_country', 'birth_year', 'gender', 'ins_num']
    dim_immi_personal = rename_columns(dim_immi_personal, new_columns)
    dim_immi_personal = dim_immi_personal.withColumn('cic_id',  col('cic_id').cast(IntegerType()))
    dim_immi_personal = dim_immi_personal.withColumn('citizen_country',  col('citizen_country').cast(IntegerType()))
    dim_immi_personal = dim_immi_personal.withColumn('residence_country',  col('residence_country').cast(IntegerType()))
    dim_immi_personal = dim_immi_personal.withColumn('birth_year',  col('birth_year').cast(IntegerType()))
    
    dim_immi_personal.write.mode("overwrite")\
                        .parquet(path=output_data + 'dim_immi_personal')

    
def process_dim_airline(spark, input_data, output_data):
    """
    Extract, transform and load dimension airline table
    
    Keyword arguments:
    spark -- spark session
    input_data -- localtion of input data
    output_data -- target output repository
    
    """  
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    
    dim_immi_airline = df_spark.select('cicid', 'airline', 'admnum', 'fltno', 'visatype','i94port').distinct()\
                                .withColumn("immi_airline_id",  monotonically_increasing_id())
    
    new_columns = ['cic_id', 'airline', 'admin_num', 'flight_number', 'visa_type', 'port_code']
    dim_immi_airline = rename_columns(dim_immi_airline, new_columns)
    dim_immi_airline = dim_immi_airline.withColumn('cic_id',  col('cic_id').cast(IntegerType()))
    dim_immi_airline = dim_immi_airline.withColumn('admin_num',  col('admin_num').cast(LongType()))
    
    dim_immi_airline.write.mode("overwrite")\
                    .parquet(path=output_data + 'dim_immi_airline')
    

def process_dim_demographic(spark, input_data, output_data):
    """
    Extract, transform and load dimension demographic table
    
    Keyword arguments:
    spark -- spark session
    input_data -- localtion of input data
    output_data -- target output repository
    
    """      
    path = os.path.join(input_data + '/us-cities-demographics.csv')
    df_spark = spark.read.format('csv').options(header=True, delimiter=';').load(path)
    
    dim_demographic_population = df_spark.select('City', 'State Code', 'Male Population', 'Female Population', 'Number of Veterans', 'Foreign-born', 'Race').distinct()\
                                    .withColumn("pop_id",  monotonically_increasing_id())
    new_columns = ['city', 'state_code', 'male_pop', 'female_pop', 'num_vetarans', 'foreign_born', 'race']
    dim_demographic_population = rename_columns(dim_demographic_population, new_columns)

    dim_demographic_population.write.mode("overwrite").partitionBy('state_code')\
                        .parquet(path=output_data + 'dim_demographic_population')

    dim_demographic_statistics = df_spark.select('City', 'State Code', 'Median Age', 'Average Household Size').distinct()\
                                    .withColumn("stat_id",  monotonically_increasing_id())

    new_columns = ['city', 'state_code', 'median_age', 'avg_household_size']
    dim_demographic_statistics = rename_columns(dim_demographic_statistics, new_columns)

    dim_demographic_statistics = dim_demographic_statistics.withColumn('city', upper(col('city')))

    dim_demographic_statistics.write.mode("overwrite").partitionBy('state_code')\
                        .parquet(path=output_data + 'dim_demographic_statistics')
    
def process_dim_airport(spark, input_data, output_data):
    """
    Extract, transform and load dimension airport table
    
    Keyword arguments:
    spark -- spark session
    input_data -- localtion of input data
    output_data -- target output repository
    
    """      
    path = os.path.join(input_data + '/airport-codes_csv.csv')
    
    df_spark = spark.read.format('csv').options(header=True).load(path)
    
    dim_airport = df_spark.select('iata_code', 'name', 'type', 'iso_country', 'iso_region', 'municipality', 'coordinates').distinct()
    new_columns = ['iata_code', 'name', 'type', 'country', 'state_code', 'municipality', 'coordinates']
    dim_airport = rename_columns(dim_airport, new_columns)

    dim_airport = dim_airport.filter(col("iata_code").isNotNull()).filter(col('iso_country')=='US')
    dim_airport = dim_airport.withColumn("longitude", split('coordinates', ", ")[0].cast(FloatType()))
    dim_airport = dim_airport.withColumn("latitude", split('coordinates', ", ")[1].cast(FloatType()))

    dim_airport = dim_airport.drop(col("coordinates"))
    dim_airport = dim_airport.withColumn("state_code", split("state_code", "-")[1])
    dim_airport = dim_airport.withColumn("country", lit("United States"))
    dim_airport.write.mode("overwrite").partitionBy('state_code')\
                        .parquet(path=output_data + 'dim_airport')

def process_dim_temperature(spark, input_data, output_data):
    """
    Extract, transform and load dimension temperature table
    
    Keyword arguments:
    spark -- spark session
    input_data -- localtion of input data
    output_data -- target output repository
    
    """      
    df_spark = spark.read.format('csv').options(header=True).load('../../data2/GlobalLandTemperaturesByCity.csv')
    dim_temperature = df_spark.filter(col('country')=='United States')
    
    dim_temperature = dim_temperature.select('dt', 'AverageTemperature', 'AverageTemperatureUncertainty', 'City', 'Country')
    new_columns = ['dt', 'avg_temp', 'avg_temp_uncertnty', 'city', 'country']
    dim_temperature = rename_columns(dim_temperature, new_columns)

    dim_temperature = dim_temperature.withColumn('dt', to_date(col('dt')))
    dim_temperature = dim_temperature.withColumn('year', year(col('dt')))
    dim_temperature = dim_temperature.withColumn('month', month(col('dt')))
    dim_temperature = dim_temperature.withColumn("record_id",  monotonically_increasing_id())

    dim_temperature = dim_temperature.withColumn('avg_temp', col('avg_temp').cast(FloatType()))
    dim_temperature = dim_temperature.withColumn('avg_temp_uncertnty', col('avg_temp_uncertnty').cast(FloatType()))
    dim_temperature.write.mode("overwrite")\
                    .parquet(path=output_data + 'dim_temperature')
    
    
def main():
    """
    The main function for etl process

    """   
    spark = create_spark_session()
    
    input_data = "./"   # to be replaced with source S3 bucket if needed
    output_data = "./output/" # to be replaced with target S3 bucket in etl.py
    
    process_labels(spark, input_data, output_data)
    process_fact(spark, input_data, output_data)
    process_dim_personal(spark, input_data, output_data)
    process_dim_airline(spark, input_data, output_data)
    process_dim_demographic(spark, input_data, output_data)
    process_dim_airport(spark, input_data, output_data)
    process_dim_temperature(spark, input_data, output_data)
    
    
if __name__ == "__main__":
    main()

