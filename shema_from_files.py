__author__ = 'Travis'

import pandas as pd
import numpy as np
import glob as glob

file_directory = glob.glob('C:/NHIS2014/test_database_2014/*.csv')

for file in file_directory:
    print file
    file_name = file.split('C:/NHIS2014/test_database_2014\\')[1]
    df = pd.read_csv(file)
    schema_list = []
    for x in df.columns.tolist():
        data_type = str(df[x].dtype).split('64')[0]
        data_type_cap = data_type.upper()
        if data_type_cap == 'OBJECT':
            data_type_cap = 'VARCHAR(10)'
        schema_list.append({'column': x, 'dtype': data_type_cap})
    df = pd.DataFrame(schema_list)
    df.to_csv("C:/NHIS2014/test_database_2014/2014_Schema/" + file_name)