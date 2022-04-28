from typing import Optional
from pydantic import BaseModel

"""
The json objects are validated and deserialized into python class object using pydantic.
This can extended to add new components like join's, reading and writing to databases etc. Corresponding class models 
can be created to deserialize object into json

For more information please refer the below link. 
https://pydantic-docs.helpmanual.io/

"""


class read_file(BaseModel):
    input_path: str
    component_name: str
    component_schema: Optional[str] = ""
    read_options: Optional[dict] = {"header": "false"}
    file_format: str


class write(BaseModel):
    column_name: Optional[str] = "*"
    mode: Optional[str] = "Overwrite"
    input_component_name: str
    output_file_path: str
    component_name: str
    format_type: Optional[str] = "csv"
    write_options: Optional[dict] = {"header": "false"}


class filter_component(BaseModel):
    column_name: Optional[str] = "*"
    input_component_name: str
    component_name: str
    filter_condition: str


class create_edit_column(BaseModel):
    column_name: Optional[str] = "*"
    input_component_name: str
    component_name: str
    transformations: str


class aggregate_component(BaseModel):
    column_name: Optional[str] = "*"
    input_component_name: str
    component_name: str
    group_by_column: str
    agg_column: str
    aggregate_function: str
    alias_name:str
