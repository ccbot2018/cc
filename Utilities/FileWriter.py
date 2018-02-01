import os, time
import pandas as pd
import numpy as np
import json

STAMP_DATE_FORMAT = "%Y%m%d_%H%M%S"

def write_file(folder_path, file_name, data_table, add_stamp_date):
    prefix = ""
    if add_stamp_date:
        t = time.localtime()
        prefix = time.strftime(STAMP_DATE_FORMAT, t)
    file_path = os.path.join(folder_path, prefix + "_" + file_name + ".csv")
    with open(file_path, 'w') as f:
        data_table.to_csv(f, header=True)


def write_binary_file(folder_path, file_name, record_array, add_stamp_date):
    prefix = ""
    if add_stamp_date:
        t = time.localtime()
        prefix = time.strftime(STAMP_DATE_FORMAT, t)
    file_path = os.path.join(folder_path, prefix + "_" + file_name + ".bin")
    record_array.tofile(file_path)
    file_path_dtype = os.path.join(folder_path, prefix + "_" + file_name + "_dtype.txt")
    dtype_desc = record_array.dtype.descr
    data_dict_string = {str(t[0]): t[1] for t in dtype_desc}
    data_json = json.dumps(data_dict_string, sort_keys=True, indent=4, separators=(',', ': '))
    f = open(file_path_dtype, 'w')
    f.write(data_json)

