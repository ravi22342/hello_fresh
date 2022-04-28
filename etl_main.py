import argparse
import json
import logging

from dependencies.etl_pipeline_exception import ETLPipelineException
from Pipeline.job_json_model import read_file, write, create_edit_column, filter_component, aggregate_component
from Pipeline.job_processor import ProcessComponents

from dependencies.spark import start_spark

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("etl-pipeline")

# for executing this program on pycharm windows uncomment below lines
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

"""
This is the class where all the parsing and processing functions are implemented. 
The 'job_json' parsed to extract job_flow, spark_config and create objects of each components and then process/execute 
the components on spark.
 
"""


class HelloFreshPipeline:
    def __init__(self, job_json):
        self.spark_config = []
        self.components_list = []
        self.job_flow = []
        self.job_json = job_json

    def parse_job_json(self):
        try:
            with open(self.job_json) as json_file:
                job_file = json.load(json_file)

            self.job_flow = job_file["job_flow"]
            if self.job_flow == "":
                raise Exception("job flow cannot be empty")
            self.spark_config = job_file["spark_config"].items()
            if self.spark_config == "":
                raise Exception("spark_config cannot be empty")

            components = job_file["components"]
            for component_name in self.job_flow:
                component_type = components[component_name]["component_type"]
                if component_type == "read_file":
                    self.components_list.append(read_file(**components[component_name]))
                elif component_type == "create_edit_column":
                    self.components_list.append(create_edit_column(**components[component_name]))
                elif component_type == "filter_component":
                    self.components_list.append(filter_component(**components[component_name]))
                elif component_type == "aggregate_component":
                    self.components_list.append(aggregate_component(**components[component_name]))
                elif component_type == "write":
                    self.components_list.append(write(**components[component_name]))
                else:
                    log.error(f"Unsupported Component Type : '{component_type}'")
                    raise ETLPipelineException(f"Unsupported Component Type : '{component_type}'")
        except KeyError as e:
            log.error("Missing mandatory fields: " + str(e))
            raise ETLPipelineException("Missing mandatory fields: " + str(e))
        except Exception as e:
            log.error("Error while parsing job JSON")
            raise

    def process_components(self):
        spark = start_spark(self.spark_config)
        job_processor = ProcessComponents(spark)
        for component in self.components_list:
            job_processor.process(component)


def main(job_json):
    etl_pipeline = HelloFreshPipeline(f"/var/etl/job_json/{job_json}")

    # parse job-json
    log.info("parsing job_json")
    etl_pipeline.parse_job_json()

    # process components
    log.info("processing components")
    etl_pipeline.process_components()

    log.info("pipeline successfully completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--job_json')
    args = parser.parse_args()
    main(args.job_json)
