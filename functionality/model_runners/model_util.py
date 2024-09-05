from datetime import datetime

def format_time(time_string, input_format="%Y-%m-%dT%H:%M:%SZ", output_format="%Y-%m-%d %H:%M:%S"):
    if isinstance(time_string, str):
        datetime_obj = datetime.strptime(time_string, input_format)
    else:
        datetime_obj = time_string

    output_time_string = datetime_obj.strftime(output_format)
    return output_time_string