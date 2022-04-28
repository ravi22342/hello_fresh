import logging

from pyspark.sql.types import *
from .etl_pipeline_exception import ETLPipelineException

"""
@:param schema: String = schema should be in the below format
    col_1!<data_type>!#!col2!<data_type>
    Currently these data types are supported 
        String
        Integer
        Date
        Boolean
        Decimal
        Float
        Double
        
"""

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("parse_schema")

def parse_schema(schema):
    schema_struct = StructType()
    for column in schema.split("!#!"):
        column_name = column.split("!")[0]
        column_data_type = column.split("!")[1]
        if column_data_type == "String":
            schema_struct.add(StructField(column_name, StringType(), True))
        elif column_data_type == "Integer":
            schema_struct.add(StructField(column_name, IntegerType(), True))
        elif column_data_type == "Date":
            schema_struct.add(StructField(column_name, DateType(), True))
        elif column_data_type == "Boolean":
            schema_struct.add(StructField(column_name, BooleanType(), True))
        elif column_data_type == "Decimal":
            schema_struct.add(StructField(column_name, DecimalType(), True))
        elif column_data_type == "Float":
            schema_struct.add(StructField(column_name, FloatType(), True))
        elif column_data_type == "Double":
            schema_struct.add(StructField(column_name, DoubleType(), True))
        else:
            log.error(f"Data Type not supported : '{column_data_type}")
            raise ETLPipelineException(f"Data Type not supported : '{column_data_type}'")

    return schema_struct
