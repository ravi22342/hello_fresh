import logging
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from dependencies.etl_pipeline_exception import ETLPipelineException
from .job_json_model import read_file, write, create_edit_column, filter_component, aggregate_component
from dependencies.schema_parser import parse_schema
from dependencies.udfs import convert_iso_duration_to_minutes

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("ProcessComponents")

"""
    Process components 
    :param spark = Spark Session Object
        
    Currently the Etl application supports
        1. Read file - can read all the file format supported by spark
        2. Write file - can write in all the file formats supported by spark
        3. create_edit_column - This is helpful in transforming or creating a column
        4. aggregate_component - This is help in executing aggregate functions on dataset. 
        5. filter_component - This is helpful in filtering dataset
    
    New Spark transformation and actions can be implemented by creating new functions in this class.
     
    At each transformation spark temp table is created with component_name. The next component can read from that temp
    table and do further processing. "input_component_name" field holds the value of the previous component. 
    
"""


class ProcessComponents:
    def __init__(self, spark):
        self.spark = spark
        log.info("registering udfs..")
        self.register_udfs()

    def register_udfs(self):
        self.spark.udf.register("convertISODurationToMinutes", convert_iso_duration_to_minutes, StringType())

    def process(self, component):
        if isinstance(component, read_file):
            self.process_read(component)
        elif isinstance(component, create_edit_column):
            self.create_edit_column(component)
        elif isinstance(component, write):
            self.process_write(component)
        elif isinstance(component, filter_component):
            self.process_filter_component(component)
        elif isinstance(component, aggregate_component):
            self.process_aggregate_component(component)
        else:
            log.error(f"{component} Unsupported component")
            raise ETLPipelineException(f"{component} Unsupported component")

    def process_read(self, component: read_file):
        try:
            if component.component_schema != "":
                schema_struct = parse_schema(component.component_schema)
                log.info("schema parsed")
                self.spark.read \
                    .options(**component.read_options) \
                    .format(component.file_format) \
                    .schema(schema_struct) \
                    .load(component.input_path) \
                    .createOrReplaceTempView(component.component_name)
            else:
                self.spark.read \
                    .options(**component.read_options) \
                    .format(component.file_format) \
                    .load(component.input_path) \
                    .createOrReplaceTempView(component.component_name)

        except Exception as e:
            log.error("Exception in read component ")
            self.spark.stop()
            raise

    def process_write(self, component: write):
        try:
            df = self.spark.sql(f"select {component.column_name} from {component.input_component_name}")
            df.write.mode(component.mode) \
                .format(component.format_type) \
                .options(**component.write_options) \
                .save(component.output_file_path)
        except Exception as e:
            log.error("Exception in write component "+str(e))
            raise
        finally:
            self.spark.stop()

    def create_edit_column(self, component):
        try:
            df = self.spark.sql(f"select {component.column_name} from {component.input_component_name}")
            column_name = component.transformations.split("!#!")[0]
            transformation = component.transformations.split("!#!")[1]
            df.withColumn(column_name, F.expr(transformation)).createOrReplaceTempView(component.component_name)
        except Exception as e:
            log.error("Exception in 'create/edit column' component ")
            self.spark.stop()
            raise

    def process_filter_component(self, component):
        try:
            df = self.spark.sql(f"select {component.column_name} from {component.input_component_name}")
            df.filter(F.expr(component.filter_condition)).createOrReplaceTempView(component.component_name)
        except Exception as e:
            log.error("Exception in 'Filter' component ")
            self.spark.stop()
            raise

    def process_aggregate_component(self, component):
        try:
            df = self.spark.sql(f"select {component.column_name} from {component.input_component_name}")
            df.groupby(component.group_by_column).agg(F.expr(f"{component.aggregate_function}({component.agg_column}) "
                                                             f"as {component.alias_name}")) \
                .createOrReplaceTempView(component.component_name)
        except Exception as e:
            log.error("Exception in 'aggregate' component ")
            self.spark.stop()
            raise

