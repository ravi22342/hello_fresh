import isodate

from .etl_pipeline_exception import ETLPipelineException

"""
    In this file you could create or implement functions which can be registers as UDF's
"""


def convert_iso_duration_to_minutes(line):
    # using regex
    # extract_hours_group_list = re.findall(r'[0-9]*(?=H)', line)
    # extracted_hours_in_minutes = int(extract_hours_group_list[0]) * 60 if extract_hours_group_list != [] else 0
    # extracted_minutes_group_list = re.findall(r'[0-9]*(?=M)', line)
    # extracted_minutes = int(extracted_minutes_group_list[0]) if extracted_minutes_group_list != [] else 0
    # return extracted_minutes + extracted_hours_in_minutes

    try:
        duration_in_minutes = 'NA'
        if line != '':
            duration_in_minutes = str(isodate.parse_duration(line).total_seconds() / 60)
    except Exception as e:
        raise ETLPipelineException(str(e))
    return duration_in_minutes
