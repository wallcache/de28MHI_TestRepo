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
# from random_phone import RandomUkPhone

# pip install randomphone

fake = Faker('en_GB') 
number_of_rows = 100
N = 10 
#some code has N, and some has number_of_rows 

# action_order
action_order = []
for x in range(number_of_rows):
    fake = Faker()
    a_o = fake.text() # need to update this for canonical
    action_order.append(a_o)

df = pd.DataFrame(action_order, columns=['action_order'])
# df = df.assign(action_order=action_order)

# actor_id
actor_id = []
for x in range(number_of_rows):
#    a = random.randint(1, 10000000000)    ##ishak has changed this into a comment 
    actor_id.append(a)

df = df.assign(actor_id = actor_id)

# avatar_uploaded
avatar_uploaded = []
for x in range(number_of_rows):
    fake = Faker()
    a_u = fake.text() 
    avatar_uploaded.append(a_u)

df = df.assign(avatar_uploaded=avatar_uploaded)

#beta_features_raw
beta_features_raw = []
for x in range(number_of_rows):
    fake = Faker()
    b_f_r = fake.text() 
    beta_features_raw.append(b_f_r)

df = df.assign(beta_features_raw=beta_features_raw)

# can receive email
can_receive_email = []
for x in range(number_of_rows):
    fake = Faker()
    c_r_e = [(f'{fake.random_int(0, 1000000)}')]
    can_receive_email.append(c_r_e)

df = df.assign(can_receive_email=can_receive_email)

# capacity
capacity = []
for x in range(number_of_rows):
    cap = random.randint(1, 10000000000)
    capacity.append(cap)

df = df.assign(capacity=capacity)

# coach_id
coach_id = []
for x in range(number_of_rows):
    co = random.randint(1, 10000000000)
    coach_id.append(co)

df = df.assign(coach_id = coach_id)

# confirmation_token
confirmation_token = []
for x in range(number_of_rows):
    fake = Faker()
    c_t = fake.text()
    confirmation_token.append(c_t)

df = df.assign(confirmation_token=confirmation_token)

## counselor level 
counselor_level = []
for x in range(number_of_rows):
    cou_lvl = random.randint(1, 10000000000)
    counselor_level.append(cou_lvl)

df = df.assign(counselorLevel=counselor_level)

## counselor years
counselor_years = []
for x in range(number_of_rows):
    cou_yrs = random.randint(1, 10000000000)
    counselor_years.append(cou_yrs)

df = df.assign(counselorYears=counselor_years)

#created_on
created_on = []
for x in range(number_of_rows):
    fake = Faker()
    c_o = fake.date_time()
    created_on.append(c_o)

df = df.assign(created_on=created_on)

# createdby_id
createdby_id = []
for x in range(number_of_rows):
    c = random.randint(1, 10000000000)
    createdby_id.append(c)

df = df.assign(createdby_id=createdby_id)

#credentials_expire_at
credentials_expire_at = []
for x in range(number_of_rows):
    fake = Faker()
    c_e_a = fake.date_time()
    credentials_expire_at.append(c_e_a)

df = df.assign(credentials_expire_at=credentials_expire_at)

# credentials_expired #####tinyint
credentials_expired = []
for x in range(number_of_rows):
    fake = Faker()
    c_e = fake.date_time()
    credentials_expired.append(c_e)

df = df.assign(credentials_expired=credentials_expired)

# email
email = []
for x in range(number_of_rows):
    fake = Faker()
    first_name = fake.first_name
    last_name = fake.last_name()
    e = f"{fake.first_name()}.{fake.last_name()}@{fake.domain_name()}" # range 1 used to produce 1 email
    email.append(e)

df = df.assign(email=email)

## email canonical varchar255
##
email_canonical = []

for x in email:
    s = x
    start = s.find('@') + 1
    ss = s[start::]
    email_canonical.append(ss)
df = df.assign(email_canonical = email_canonical)

# email_primary
email_primary = []
for x in range(number_of_rows):
    fake = Faker()
    first_name = fake.first_name
    last_name = fake.last_name()
    e_p = [f"{fake.first_name()}.{fake.last_name()}@{fake.domain_name()}" for i in range(1)] # range 1 used to produce 1 email
    email_primary.append(e_p)

df = df.assign(email_primary=email_primary)

## enabled tinyint
##
enabled_list = []

for x in range(N):
    enabled = random.getrandbits(1)
    enabled_list.append(enabled)
df = df.assign(enabled = enabled_list)

## expired tinyint
##
expired_list = []

for x in range(N):
    expired = random.getrandbits(1)
    expired_list.append(expired)
df = df.assign(expire = expired_list)

# expires_at
expires_at = []
for x in range(number_of_rows):
    fake = Faker()
    e_a = fake.date_time()
    expires_at.append(e_a)

df = df.assign(expires_at=expires_at)

# firstName
firstName = []
for x in range(number_of_rows):
    fake = Faker()
    fN = fake.first_name()
    firstName.append(fN)

df = df.assign(firstName=firstName)

# id
list1 = []
for x in range(number_of_rows):
    pc = x
    list1.append(pc)
df = df.assign(id=list1)

## initial supervisees assigned tinyint = 0
##
initial_supervisees_assigned_list = []

for x in range(N):
    initial_supervisees_assigned = 0
    initial_supervisees_assigned = random.getrandbits(1)
    initial_supervisees_assigned_list.append(initial_supervisees_assigned)
df = df.assign(initial_supervisees_assigned = initial_supervisees_assigned_list)

## is in peer support tinyint = 0
is_in_peer_support = 0
df = df.assign(is_in_peer_support=is_in_peer_support)

## is practicum student tinyint = 0
is_in_practicum_student = 0
df = df.assign(is_in_practicum_student=is_in_practicum_student)

## isCounselorDisplayedAsStaff tinyint = 0
is_in_practicum_student = 0
df = df.assign(isCounselorDisplayedAsStaff=is_in_practicum_student)

# land_line
land_line = []
for x in range(number_of_rows):
    l_l = random.randint(1, 100000000000000)
    land_line.append(l_l)

df = df.assign(land_line=land_line)

# last_login
# timestamp
def random_date(start, end):

    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

#last_login
list1 = []
for x in range(number_of_rows):
    d1 = datetime.strptime('1/1/2018 12:00 AM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('12/31/2022 11:59 PM', '%m/%d/%Y %I:%M %p')
    timestamp = random_date(d1, d2)
    list1.append(timestamp)
df = df.assign(last_login = list1)

#last_seen_online_at
list1 = []
for x in range(number_of_rows):
    d1 = datetime.strptime('1/1/2018 12:00 AM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('12/31/2022 11:59 PM', '%m/%d/%Y %I:%M %p')
    timestamp = random_date(d1, d2)
    list1.append(timestamp)
df = df.assign(last_seen_online_at = list1)

##
## lastName varchar255
lastName = []
for x in range(number_of_rows):
    fake = Faker()
    lN = fake.last_name()
    lastName.append(lN)

df = df.assign(lastName=lastName)

## last released notes read id int(11)
## ideally this should be when a certain txt file is opened , but for now we will randomly generate a number
last_released_read_list = []

for x in range(N):
    last_released_read = random.getrandbits(1)
    last_released_read_list.append(last_released_read)
df = df.assign(last_released_read = last_released_read_list)

#lastReleaseNotesRead_id
list1 = []
for x in range(number_of_rows):
    fake = Faker()
    pc = fake.bothify('#########')
    list1.append(pc)
df = df.assign(lastReleaseNotesRead_id=list1)

## locked tinyint
##
locked_list = []

for x in range(N):
    locked = random.getrandbits(1)
    locked_list.append(locked)
df = df.assign(locked = locked_list)

#lastReleaseNotesRead_id
list1 = []
for x in range(number_of_rows):
    fake = Faker()
    pc = fake.bothify('###########')
    list1.append(pc)
df = df.assign(mobile_phone=list1)

## newbie tinyint = 1
##
newbie_list = []

for x in range(N):
    newbie = 1
    newbie = random.getrandbits(1)
    newbie_list.append(newbie)
df = df.assign(newbie = newbie_list)

## online tinyint = 1
##
newbie_list = []

for x in range(N):
    newbie = 1
    newbie = random.getrandbits(1)
    newbie_list.append(newbie)
df = df.assign(online = newbie_list)

#password
password = []
for x in range(number_of_rows):
    fake = Faker()
    p = fake.text()
    password.append(p)

df = df.assign(password=password)

#password_changed_on
list1 = []
for x in range(number_of_rows):
    d1 = datetime.strptime('1/1/2018 12:00 AM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('12/31/2022 11:59 PM', '%m/%d/%Y %I:%M %p')
    timestamp = random_date(d1, d2)
    list1.append(timestamp)
df = df.assign(password_changed_on = list1)

#password_requested_at
list1 = []
for x in range(number_of_rows):
    d1 = datetime.strptime('1/1/2018 12:00 AM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('12/31/2022 11:59 PM', '%m/%d/%Y %I:%M %p')
    timestamp = random_date(d1, d2)
    list1.append(timestamp)
df = df.assign(password_requested_at = list1)

#roles
roles = []
for x in range(number_of_rows):
    fake = Faker()
    r = fake.text()
    roles.append(r)

df = df.assign(roles=roles)

# salt
list1 = []
for x in range(number_of_rows):
    characters = string.ascii_letters.upper() + string.digits
    salt = ''.join(random.choice(characters) for i in range(10))
    list1.append(salt)
    
df = df.assign(salt = list1)

#spike_form_url
spike_form_url = []
for x in range(number_of_rows):
    fake = Faker()
    s_f_u = fake.text()
    spike_form_url.append(s_f_u)

df = df.assign(spike_form_url=spike_form_url)

#status_message
status_message = []
for x in range(number_of_rows):
    fake = Faker()
    s_m = fake.text()
    status_message.append(s_m)

df = df.assign(status_message=status_message)

# supervisor_id
supervisor_id = []
for x in range(number_of_rows):
    s = random.randint(1, 10000000000)
    supervisor_id.append(s)

df = df.assign(supervisor_id=supervisor_id)

## supervisor mode int11 default = 0 
supervisor_mode_list = []

for x in range(N):
    supervisor_mode = 0
    supervisor_mode = random.getrandbits(1)
    supervisor_mode_list.append(supervisor_mode)
df = df.assign(supervisor_mode = supervisor_mode_list)

## trainer tinyint = 0
trainer = 0
df = df.assign(trainer=trainer)

# two factor auth

two_factor_auth_type = []
for x in range(number_of_rows):
    fake = Faker()
    t_f_a_t = fake.text()
    two_factor_auth_type.append(t_f_a_t)

df = df.assign(two_factor_auth_type=two_factor_auth_type)
# two factor code
two_factor_code = []
for x in range(number_of_rows):
    two_fc = random.randint(1, 10000000000)
    two_factor_code.append(two_fc)

df = df.assign(two_factor_code=two_factor_code)

#two_facotr_code_expires
list1 = []
for x in range(number_of_rows):
    d1 = datetime.strptime('1/1/2018 12:00 AM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('12/31/2022 11:59 PM', '%m/%d/%Y %I:%M %p')
    timestamp = random_date(d1, d2)
    list1.append(timestamp)
df = df.assign(two_factor_code_expires = list1)

# user_profile_id
user_profile_id = []
for x in range(number_of_rows):
    u = random.randint(1, 10000000000)
    user_profile_id.append(u)

df = df.assign(user_profile_id=user_profile_id)

# username
username = []
for x in range(number_of_rows):
    fake = Faker()
    un = fake.user_name()
    username.append(un)

df = df.assign(username=username)

# username_canonical
username_canonical = []
for x in range(number_of_rows):
    fake = Faker()
    unc = fake.user_name() # need to update this for canonical
    username_canonical.append(unc)

df = df.assign(username_canonical=username_canonical)






print(df)

