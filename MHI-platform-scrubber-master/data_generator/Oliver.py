from faker import Faker
from random import randint
import pandas as pd
import random
import numpy as np
from random import choices

# population = [1, 2, 3, 4, 5, 6]
# weights = [0.1, 0.05, 0.05, 0.2, 0.4, 0.2]
# print(choices(population, weights))

number_of_rows = 10

#post code varchar
postcode = []
for x in range(number_of_rows):
    fake = Faker()
    pc = fake.postcode()
    postcode.append(pc)
# print(postcode)
df = pd.DataFrame(postcode, columns=["zip code"])

# write_access varchar
write_access= ['Read-write', 'Read Only', 'Read-write', 'Reject Delete']
write_access_weight = [0.25, 0.25, 0.25, 0.25]
wa = []
for x in range(number_of_rows):
    wa.append((choices(write_access, write_access_weight)[0]))
df = df.assign(write_access=wa)

# volunteer_pronouns
pronouns= ['he', 'she', 'they', 'ze']
pronoun_weight = [0.25, 0.25, 0.25, 0.25]
pronoun_list = []
for x in range(number_of_rows):
    pronoun_list.append((choices(pronouns, pronoun_weight)[0]))
df = df.assign(volunteer_pronouns=pronoun_list)


#versions
versions= [1.0, 2.0, 3.0, 4.0]
versions_weight = [0.25, 0.25, 0.25, 0.25]
version_list = []
for x in range(number_of_rows):
    version_list.append((choices(versions, versions_weight)[0]))
df = df.assign(versions=version_list)

def pop_column(values, weighting, column_name, rowsnumber):
    list_of_row_entries = []
    for i in range(rowsnumber):
        list_of_row_entries.append(choices(values, weighting)[0])
    df1 = pd.DataFrame(list_of_row_entries, columns=[column_name])
    return df1

def make_list_of_ones(x):
    list1 = []
    for i in range(x):
        one = 1.0
        list1.append(one)
    return list1

#version
version = [1.0, 2.0, 3.0, 4.0]
version_weight = [0.25, 0.25, 0.25, 0.25]
df = df.join(pop_column(version, version_weight, 'version', number_of_rows))

#Full Access, Limited Access and Basic Access
df = df.join(pop_column(['Full Access', 'Limited Access', 'Basic Access'], [1.0, 1.0, 1.0], 'access level', number_of_rows))

df = df.join(pop_column(['a', 'b', 'c', 'd'], make_list_of_ones(4), 'letters', number_of_rows))

print(df)

#id

