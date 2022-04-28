import logging

from pyspark import SparkConf
from pyspark.sql import SparkSession


from .etl_pipeline_exception import ETLPipelineException


"""
    create or get spark session.
    spark_config : dict_items  =  spark config passed in the job_json. The dict_items is converted to a list. 
    Ex:
        ('spark.master', 'local[*]'), 
        ('spark.app.name', 'pyspark-shell')
    Refer this link [1] for all available spark config properties     
    [1]: "https://spark.apache.org/docs/latest/configuration.html#available-properties"
    
    Register UDF's: You have the flexibility to register any number udf's. This could also be extended to control the 
    udfs' present for a particular pipeline job. ( This functionality can be implemented by adding a new json object in 
    job_json and parsing it accordingly)
    
"""

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("spark")


def start_spark(spark_config):
    try:
        conf = SparkConf()
        conf.setAll(list(spark_config))
        log.info("starting spark session")
        spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

        return spark

    except Exception as e:
        log.error("Error while starting spark " + str(e))
        raise ETLPipelineException("Error while starting spark " + str(e))


