
## Overview

we have a big recipes archive that was created over the last 8 years. It is constantly being updated either by adding new recipes or by making changes to existing ones. We have a service that can dump archive in JSON format to selected s3 location. We are interested in tracking changes to see available recipes, their cooking time and difficulty level.



## Approach 

I have designed an application which could be used to create and run any pipeline by preparing a 'job_json' and passing it as argument to the application. The application is extremely flexiable, new features can easily be added. The code is very generic not specific to current excerise, user can create any pipeline he wishes and run it.

To track the changes of available I would proably use AWS lamda and then push it to kafka topic. Implement a new component which consumes from kafka topic in application. Create 'job_json' accordingly and pass it to application. 
 
The whole Application has been dockerized. etl_volume is mounted to the container, The job json should be placed under this folder. This can also used to store input and output files. Please find the docker command to run it. If you are running it on linux you might have to change to *(pwd)*. For windows its *{PWD}*

To build the docker container. Execute the following docker build command 

`docker build -t "etlpipeline" .`


For Task 1:

    docker run -v ${PWD}/etl_volume:/var/etl/ etlpipeline --job_json task1.json

*output file :  pre-processed_data,  can be found under 'etl_volume'*

For Task 2:

    docker run -v ${PWD}/etl_volume:/var/etl/ etlpipeline --job_json task1.json

*output file :  output,  can be found under 'etl_volume'*
	
### Job Json

This is a json file which consist of all the details required to run the pipeline. The structure of json file is very simple. 

>    {   "job_flow": []   "spark_config": {},   "components": {} }

#### job_flow : 
  
 JSONArray in which components name are entered in the sequence of execution. 

#### spark_config 
JSONObject of  Spark Config. 

#### components
JSONObject consisting of configuration of all the components

Example job_json for the below pipeline

read_csv --->  filter ---> write csv
 

    {
      "job_flow": ["<read_csv_component_name>","<filter_component_name>","<write_csv_compnent_name>"],
      "spark_config": {
        "spark.master": "local[*]",
        "spark.app.name": "dummy pipeline"
      },
      "components": {
        "<read_csv_component_name>": {
          "component_type": "read_file",
          "component_name": "read_component",
          "input_path": "/var/files/input",
          "options": {
            "header": false,
            "inferSchema": false
          },
          "file_format": "json",
          "component_schema": "col1!String!#!col2!#!Integer"
        },
        "<filter_component_name>": {
          "filter_condition": "col1== 'M'",
          "component_name": "<filter_component_name>",
          "input_component_name": "<read_csv_component_name>",
          "component_type": "filter_component"
        },
        "<write_csv_component_name>": {
          "column_name": "<colums to select>",
          "component_type": "write",
          "component_name": "write_csv_component_name",
          "output_file_path": "<path to save>",
          "input_component_name": "<filter_component_name>",
          "format_type": "csv",
          "write_options": {
            "header": "true"
          }
        }
      }
    }
     
### ETL Application

The application currently supports following component types

1. **read_file** :  This components is used for reading any file that is supported by spark.  The "*read_options*" can be used to configure the read components. All spark read options parameter are supported. 
2. **write** : This component is used for saving the output to a file. The *"write_options"* can be used set configurations. All Spark write configurations are supported 
3.  **create_edit_column**: This components is used to transform a column or create a new column. You can pass any spark-sql expression it will be executed. 
4. **filter** :  This component is used for performing filter operation.  The condition is evaluvated as *spark.expr().* 
5. **aggregate_component** :  This components can perform aggreagate operations. All spark aggregation are supported. 
 
 Other components like join, repartion/coalsec, persist, broadcase etc.  can easily be integrated to the application with no much effort. 
 
## Coding challenge

### Task 1

> Using Apache Spark and Python, read, pre-process and persist rows to
> ensure optimal structure and performance for further processing.   The
> source events are located on the `input` folder.

With the use of above application. I have created a job_json  (*task1.json*) which reads the input json file and stores them as parquet.  For preprocessing I have converted the ISO durations to minutes using an UDF (convertISODurationToMinutes). I had earlier used regex expression to extract values and convert it to minutes but currently I am using a python lib call isodates (I have commented the code that uses regex earlier for your reference).  For this pipeline I did not feel preprocessing of other columns will be required as we will be mainly dealing with "cooktime" and "preptime".  

Note : By adding "create_edit_columns" components. The pre-processing of other columns can easily be handled. 

### Task 2

> Using Apache Spark and Python read processed dataset from Task 1 and:
> 
> 1.  Extract only recipes that have  `beef`  as one of the ingredients.
> 2.  Calculate average cooking time duration per difficulty level.
> 3.  Persist dataset as CSV to the  `output`  folder.  
>     The dataset should have 2 columns:  `difficulty,avg_total_cooking_time`.


For this task I designed the job_json (*task2.json*) that reads the parquet that was created in task 1, created two column called "total_time" and "difficulty" with some case statements by using *"create_edit_column",* then using *"aggregate_component"* I calculated the avg time for each difficulty level and wrote it to a csv

    difficulty    avg(total_time)  
    medium         45.0  
    hard           194.3913043478261  
    easy           19.625

This can be further be improved for huge datasets by using repartioning on *difficulty* column.  ( In the current application there no component to repartion but it can be added easily)

### Scalability
The application can configured to run on spark cluster by passing the spark cluster url to *"spark.master"*. The application can also run as spark submit.  This application scalable with large amount of data without any code changes. 

### Scheduling 

By changing the job_json arguments to the application, it can easily be scheduled on airflow or any other scheduling software. 

### CI/CD 
I have experience on GIT lab CI/CD, so I would configure a gitlab runner in one the machines. The CI/CD pipeline can be configured in gitlab-ci.yml file. The pipeline witll first build container then run tests by passing different job_json  and check the results. 
 