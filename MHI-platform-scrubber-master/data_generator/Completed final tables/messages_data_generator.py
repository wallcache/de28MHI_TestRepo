from contextlib import nullcontext
from winsound import MessageBeep
from faker import Faker
from random import randint
import pandas as pd
import random
import numpy as np
from random import choices
from random import randrange
from datetime import timedelta
from datetime import datetime
import string

rows = input('How many rows do you want?:')
number_of_rows = int(rows)

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

# id
list1 = []
for x in range(number_of_rows):
    pc = x
    list1.append(pc)
df = pd.DataFrame(list1, columns=["id"])

# conversation_id
list1 = []
for x in range(number_of_rows):
    pc= x
    list1.append(pc)
df = df.assign(conversation_id=list1)

# actor_id
list1 = []
for x in range(number_of_rows):
    fake = Faker()
    pc = fake.bothify('#########')
    list1.append(pc)
df = df.assign(actor_id=list1)

# type
df = df.join(pop_column(['1'], make_list_of_ones(1), 'type', number_of_rows))

###############
faker = Faker()
name = faker.first_name()
intensity_dictionary = {1:'fairly', 2: 'quite', 3:'a little', 4:'slightly', 5:'somewhat', 6:'pretty', 7:'sort of', 8:'kind of', 9:'reasonably', 10:'considerably', 11:'very', 12:'reasonably', 13:'awfully', 14:'deeply', 15:'truly', 16:'unusually', 17:'seriously', 18:'extremely', 19:'particularly', 20:'extremely'}
emotion_dictionary = {1:'sad', 2: 'upset', 3:'worried', 4:'anxious', 5:'heartbroken', 6:'somber', 7:'unhappy', 8:'dejected', 9:'dismal', 10:'stressed', 11:'distressed', 12:'glum', 13:'miserable', 14:'depressed', 15:'bad', 16:'enraged', 17:'agitated', 18:'angry', 19:'enraged', 20:'frustrated'}
situation_dictionary = {1:'at home', 2:'at work', 3:'at the moment', 4:'with life', 5:'with family', 6:'with relationship', 7:'with friends', 8:'with my parents', 9:'in the matrix', 10:'with my development as a person' }
relations_dictionary = {1:'mother', 2:'father', 3:'sister', 4:'brother', 5:'cousin', 6:'son', 7:'uncle', 8:'aunt', 9:'grandmother', 10:'grandfather' }
greeting_dictionary = {1:'Hello Shout', 2: 'Hey Shout', 3:'Hi Shout', 4:'', 5:'Good morning Shout', 6:'Good evening Shout', 7:'Good afternoon Shout', 8:'Hi there', 9: 'Hello there', 10: 'Hey there'}
thought_dictionary = {1:'thinking', 2:'wondering', 3:'inquiring', 4:'pondering'}
help_dictionary = {1:'help', 2:'advice', 3:'input', 4:'guidance', 5:'support', 6:'words of advice', 7:'advice or support', 8:'help or guidance', 9:'input or advice', 10:'help or input'}
time_dictionary = {1:'this past week', 2:'this last week', 3:'these last few weeks', 4:'this past month', 5:'this last month', 6:'these last few months', 7:'these last few days', 8:'these past few days', 9:'this week', 10:'this month'}

list_of_messages = []
list1 = []
for x in range(number_of_rows):
    mess_a = random.randint(1, 20)
    mess_b = random.randint(1, 10)
    mess_c = random.randint(1, 4)
    message_1 = f'{greeting_dictionary[mess_b]}, I am feeling {intensity_dictionary[mess_a]} {emotion_dictionary[mess_a]} about my current situation {situation_dictionary[mess_b]}, I am {thought_dictionary[mess_c]} if I could get some {help_dictionary[mess_b]} with this,\n thank you, {fake.first_name()}.'
    message_2 = f'{greeting_dictionary[mess_b]}, this is {fake.first_name()} and I am feeling {intensity_dictionary[mess_a]} {emotion_dictionary[mess_a]} about my current situation {situation_dictionary[mess_b]}, I am {thought_dictionary[mess_c]} if I could get some {help_dictionary[mess_b]} with this,\n thank you.'
    message_3 = f'{greeting_dictionary[mess_b]}, I am feeling {intensity_dictionary[mess_a]} {emotion_dictionary[mess_a]} about my current situation {situation_dictionary[mess_b]}, I am {thought_dictionary[mess_c]} if I could get some {help_dictionary[mess_b]} with this,\n thank you.'
    message_4 = f'{greeting_dictionary[mess_b]}, {time_dictionary[mess_b]} has left me feeling {intensity_dictionary[mess_a]} {emotion_dictionary[mess_a]} in relation to my situation {situation_dictionary[mess_b]}, I am in need of {help_dictionary[mess_b]}, \n thank you, {fake.first_name()}.'
    message_5 = f'{greeting_dictionary[mess_b]}, this is {fake.first_name()} and {time_dictionary[mess_b]} has left me feeling {intensity_dictionary[mess_a]} {emotion_dictionary[mess_a]} in relation to my current predicament {situation_dictionary[mess_b]}, I am in need of {help_dictionary[mess_b]},\n thank you.'
    message_6 = f'{greeting_dictionary[mess_b]}, {time_dictionary[mess_b]} has left me feeling {intensity_dictionary[mess_a]} {emotion_dictionary[mess_a]} in relation to my current situ {situation_dictionary[mess_b]}, I am in need of {help_dictionary[mess_b]},\n thank you.'
    message_7 = f'I am feeling {intensity_dictionary[mess_a]} {emotion_dictionary[mess_a]} right now. Is there any chance I could get some {help_dictionary[mess_b]}, \n thank you, {fake.first_name()}.'
    message_8 = f'{fake.first_name()} here. I am currently feeling {intensity_dictionary[mess_a]} {emotion_dictionary[mess_a]}. Is there any chance I could get some {help_dictionary[mess_b]}, \n thank you.'
    message_9 = f'I need some {help_dictionary[mess_b]} please'
    message_10 = f'Hi Shout, my name is {faker.first_name()}, my {relations_dictionary[mess_b]} {fake.first_name()} is causing me to feel {emotion_dictionary[mess_a]}'
    message_11 = f'Hi Shout, I am {faker.first_name()} I am feeling a bit {emotion_dictionary[mess_a]}'
    message_12 = f'Sometimes I think i feel {emotion_dictionary[mess_a]}, about my friends asking me questions...yeah, i would rather be playing tennis'
    message_13 = f'It is all good'
    message_14 = f'I am deeply neurtotic,'
    message_tuple = (message_1, message_2, message_3, message_4, message_5, message_6, message_7, message_8, message_9,  message_10, message_11,  message_12, message_13)
    message = random.choice(tuple(message_tuple))
    list1.append(message)
df = df.assign(message=list1)
###############

# timestamp
def random_date(start, end):

    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

list1 = []
for x in range(number_of_rows):
    d1 = datetime.strptime('1/1/2018 12:00 AM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('12/31/2022 11:59 PM', '%m/%d/%Y %I:%M %p')
    timestamp = random_date(d1, d2)
    list1.append(timestamp)
df = df.assign(timestamp = list1)

# salt
list1 = []
for x in range(number_of_rows):
    characters = string.ascii_letters.upper() + string.digits
    salt = ''.join(random.choice(characters) for i in range(10))
    list1.append(salt)
df = df.assign(salt = list1)

# remote_message_id
list1 = []
for x in range(number_of_rows):
    remote_message_id = ''.join(random.choice(characters) for i in range(50))
    list1.append(remote_message_id)
df = df.assign(remote_message_id = list1)

# status
list1 = []
for x in range(number_of_rows):
    mess_c = {1, 2, 4}
    status = random.choice(tuple(mess_c))
    list1.append(status)
df = df.assign(status = list1)

# retries
list1 = []
for x in range(number_of_rows):
    retries = ''
    list1.append(retries)
df = df.assign(retries = list1)

# delivery error
list1 = []
for x in range(number_of_rows):
    mess_c = {'Error: Unable to connect, please try again', ''}
    delivery_error = random.choice(tuple(mess_c))
    list1.append(delivery_error)
df = df.assign(delivery_error = list1)

# media uri
list1 = []
for x in range(number_of_rows):
    row = ''
    list1.append(row)
df = df.assign(media_uri=list1)

# media mimetype
list1 = []
for x in range(number_of_rows):
    row = ''
    list1.append(row)
df = df.assign(media_mimetype=list1)

print(df)
# df.to_csv('message_table.csv')
