from faker import Faker
from random import randint
import pandas as pd
import random
import numpy as np
from random import choices
fake = Faker()


rows = input('How many rows do you want?:')
number_of_rows = int(rows)

# id ##############
list1 = []
for x in range(number_of_rows):
    list1.append(x)
df = pd.DataFrame(list1, columns=["id"])

# actor_id ##############
list1 = []
for x in range(number_of_rows):
    fake = Faker()
    pc = fake.bothify('#########')
    list1.append(pc)
df = df.assign(actor_id=list1)

# conversation_id ##############
list1 = []
for x in range(number_of_rows):
    pc= x
    list1.append(pc)
df = df.assign(conversation_id=list1)

# survey_id ##############
list1 = []
for x in range(number_of_rows):
    pc= x
    list1.append(pc)
df = df.assign(survey_id=list1)

# response_id ##############
response_id = []
for x in range(number_of_rows):
    r = random.randint(1, 10000000000)
    response_id.append(r)
df = df.assign(response_id = response_id)

# returning responder ##############
rr = 0
df = df.assign(returning_responder=rr)

# status ##############
list1 = []
for x in range (number_of_rows):
    fake = Faker()
    status = fake.text(255)
    list1.append(status)

df = df.assign(status=list1)

# time submitted ##############
list1 = []
for x in range (number_of_rows):
    fake = Faker()
    l_s_o = fake.time_object()
    list1.append(l_s_o)
df = df.assign(time_submitted=list1)

# time imported ##############
list1 = []
for x in range (number_of_rows):
    fake = Faker()
    t_i = fake.time_object()
    list1.append(t_i)
df = df.assign(time_imported=list1)


print(df)

df.to_csv('texter_survey.csv')