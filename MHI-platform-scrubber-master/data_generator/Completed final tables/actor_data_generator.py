

from faker import Faker
import pandas as pd
from random import randint
from faker.providers import BaseProvider
import random
import csv
import socket
import struct
import time
import datetime
import geocoder
import string
import pandas as pd
import random
import numpy as np
import re
from random import choices
from random import randrange
from datetime import timedelta
from datetime import datetime
fake = Faker("en_GB")
# pip install geocoder 
# pip install socket 
# pip install faker

#set N as a the number of rows you would like to make 

N = 10
number_of_rows = N
# please assign the value of N which shows the number of rows to be generated 

############################  id   #########################
#int(11) default = Not Null

id_list = []
for x in range(number_of_rows):
    pc = fake.bothify('###########') #11 values
    id_list.append(pc)
df = pd.DataFrame(id_list, columns=["id"])

############################### profile_id ############################
#int(11) default = Null

profile_id = []
for x in range(number_of_rows):
    pid = fake.bothify('###########')
    profile_id.append(pid)
df = pd.DataFrame(profile_id, columns=["profile_id"])


#################### address ######  city  ##################
#longtext default = Not Null
#splitting it to get relevant data 
#there is also code below for a postcode , but its not a column in the actor table 
address_list = []
postcode_list = []
city_list = []

for x in range(N):
    address = fake.address()
    address_list.append(address)

    address_str = address.split('\n')
    address_1 = address_str[0]
    city = address_str[1]
    postcode = address_str[-1] #the postcode is always the last line , faker produces 3or4 lines of an address 
    city_list.append(city)
    postcode_list.append(postcode)

df = df.assign(address = address_list)
df = df.assign(city = city_list)


#########################______address_hash________#############################
#hash is just a 128-bit value, so we just generate a random 128 bit value 
#varchar(255) Default = Not Null

address_hash_list = []

for x in range(N):
    address_hash = random.getrandbits(128)
    address_hash_list.append(address_hash)
df = df.assign(address_hash = address_hash_list)


############################### type  ############################
type_list = []

#varchar(32) Default = Not Null

for x in range(N):
    type = fake.bothify('#########################################')
    type_list.append(type)
df = df.assign(type = type_list)


############################ cached_conversation_count #########################
#counts the amount of conversation this person has had in the past 
#int(11) Default = 0
cached_conversation = []

for x in range(N):
    cached_conversation_count = 0
    cached_conversation_count = random.randint(1, 1000)
    cached_conversation.append(cached_conversation_count)
df = df.assign(cached_conversation_count = cached_conversation)

############################ salt #########################
# longtext 
salt_list = []

for x in range(number_of_rows):
    characters = string.ascii_letters.upper() + string.digits
    salt = ''.join(random.choice(characters) for i in range(10))
    salt_list.append(salt)
df = df.assign(salt = salt_list)


############################### can_text  ############################
# a 0 or 1 value that says if the person can be texted or not 
# default = 1
can_text_list = []

for x in range(N):
    can_text = 1
    can_text = random.getrandbits(1)
    can_text_list.append(can_text)
df = df.assign(can_text = can_text_list)

###############################  carrier  ############################
# varchar(255), Default = Null

carrier_names = ('T-Mobile','O2','3','EE','Verizon','AT&T','Vodaphone','Orange')
carrier_list = []

for x in range(N):
    carrier = random.choice(carrier_names)
    carrier_list.append(carrier)
df = df.assign(carrier = carrier_list)

###############################  city  ############################
#faker address produces a address , the second line is the city
#please see ######### address ############## code 


###############################  state  ############################
#generate a random state as all data is fake and cannot relate to a real state 
state_list = []
#varchar = 255 , default = Null
state_names = ('Middlesex','Buckinghamshire','Northamptonshire','Somerset','Surrey','Barnet','West Sussex','Bexley')

for x in range(N):
    state = random.choice(state_names)
    state_list.append(state)
df = df.assign(state = state_list)

############################### is_ceded ############################
# default = Null
is_ceded_list = []

for x in range(N):
    is_ceded = random.getrandbits(1)
    is_ceded_list.append(is_ceded)
df = df.assign(is_ceded = is_ceded_list)


############################### connection_id  ############################
#varchar(255) , default = Null
connection_id_list = []
for x in range(N):

    connection_id = fake.bothify('#########')
    connection_id_list.append(connection_id)

df = df.assign(connection_id = connection_id_list)

###############################  ip_address  ############################

ip_address_list = []
for x in range(N):

    ip_address = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
    ip_address_list.append(ip_address)

df = df.assign(ip_address = ip_address_list)

############################### is_in_country  ############################
# outputting a 0 or 1 which tells us if the ip is in the country or not 
# default = null 
is_in_country_list = []
for x in range(N):

    is_in_country = random.getrandbits(1)
    is_in_country_list.append(is_in_country)

df = df.assign(is_in_country = is_in_country_list)

############################### location_overidden ############################
#if location is None , its overridden to GB
#ip_country_code = ip_location()
#its a bit long winded but i found its the only way for this to work
#location overidden is spelt as locationOverridden in the SQL script
#default = 0 

location_overridden_list = []

for x in range(N):
    locationOverriddden = 0
    ip_location = geocoder.ip(ip_address)

    location = '_'.join(map(str,ip_location))
    location_split = location.split(', ')
    country_code_bracket =  location_split[-1]
    country_code = country_code_bracket[ : -1]
    locationOverriddden = 0

    if country_code == None: 
        country_code == GB
        locationOverriddden = 1
    else: 
        pass

    location_overridden_list.append(locationOverriddden)

df = df.assign(locationOverriddden = location_overridden_list)


#####################################################################################

# create a dataframe for the actor table 
#df_fake = pd.DataFrame({
#    "address": address,
#    "address_hash": address_hash,
#    "cached_conversation_count": cached_conversation_count,
#    "can_text": can_text,
#    "carrier": carrier,
#    "city": city,
#    "connection_id": connection_id,
#    "id" : id,
#    "ip_address": ip_address,
#    "is_in_country": is_in_country,
#    "is_ceded": is_ceded,
#    "location_overiddden": locationOverriddden,
#    "salt": salt,
#    "profile_id": profile_id,
#    "state": state,
#    "type": type

#   })


print(df)
#un comment below to export table into a csv 
#df.to_csv('actor_table_data.csv')