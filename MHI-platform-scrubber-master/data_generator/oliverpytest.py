from winreg import HKEY_LOCAL_MACHINE
from faker import Faker
from random import randint
import pandas as pd
import random
import numpy as np
from random import choices

def pop_column(values, weighting, column_name, number_of_rows):
    list_of_row_entries = []
    for i in range(number_of_rows):
        list_of_row_entries.append(choices(values, weighting)[0])
    df1 = pd.DataFrame(list_of_row_entries, columns=[column_name])
    print(df1)

version = [1.0, 2.0, 3.0, 4.0]
version_weight = [0.25, 0.25, 0.25, 0.25]
pop_column(version, version_weight, 'hello', 10)

