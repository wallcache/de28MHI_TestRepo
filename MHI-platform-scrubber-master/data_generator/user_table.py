from faker import Faker
from random import randint
import pandas as pd
import random
import numpy as np
from random import choices
# from random_phone import RandomUkPhone

# pip install randomphone

fake = Faker('en_GB') 
number_of_rows = 10
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
    a = random.randint(1, 10000000000)
    actor_id.append(a)

df = df.assign(actor_id = actor_id)

# avatar_uploaded
avatar_uploaded = []
for x in range(number_of_rows):
    fake = Faker()
    a_u = fake.text() 
    avatar_uploaded.append(a_u)

df = df.assign(avatar_uploaded=avatar_uploaded)

# createdby_id
createdby_id = []
for x in range(number_of_rows):
    c = random.randint(1, 10000000000)
    createdby_id.append(c)

df = df.assign(createdby_id=createdby_id)

# supervisor_id
supervisor_id = []
for x in range(number_of_rows):
    s = random.randint(1, 10000000000)
    supervisor_id.append(s)

df = df.assign(supervisor_id=supervisor_id)

# coach_id
coach_id = []
for x in range(number_of_rows):
    co = random.randint(1, 10000000000)
    coach_id.append(co)

df = df.assign(coach_id = coach_id)

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

# email
email = []
for x in range(number_of_rows):
    fake = Faker()
    first_name = fake.first_name
    last_name = fake.last_name()
    e = [f"{fake.first_name()}.{fake.last_name()}@{fake.domain_name()}" for i in range(1)] # range 1 used to produce 1 email
    email.append(e)

df = df.assign(email=email)

##
## email canonical varchar255
##
email_canonical = []

for x in email:
    s = x
    start = s.find('@') + 1
    ss = s[start::]
    email_canonical.append(ss)
df = df.assign(email_canonical = email_canonical)
##
## enabled tinyint
##
enabled_list = []

for x in range(N):
    enabled = random.getrandbits(1)
    enabled_list.append(enabled)
df = df.assign(enabled = enabled_list)

# can receive email
can_receive_email = []
for x in range(number_of_rows):
    fake = Faker()
    c_r_e = [(f'{fake.random_int(0, 1000000)}')]
    can_receive_email.append(c_r_e)

df = df.assign(can_receive_email=can_receive_email)

##
## salt varchar255
##

##
## password varchar255
##

##
## last login datetime
##

##
## locked tinyint
##
locked_list = []

for x in range(N):
    locked = random.getrandbits(1)
    locked_list.append(locked)
df = df.assign(locked = locked_list)

##
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

# confirmation_token
confirmation_token = []
for x in range(number_of_rows):
    fake = Faker()
    c_t = fake.text()
    confirmation_token.append(c_t)

df = df.assign(confirmation_token=confirmation_token)

##
## password requested at datetime
##

##
## roles varchar300
##

# credentials_expired #####tinyint
credentials_expired = []
for x in range(number_of_rows):
    fake = Faker()
    c_e = fake.date_time()
    credentials_expired.append(c_e)

df = df.assign(credentials_expired=credentials_expired)

# credentials_expire_at
credentials_expire_at = []
for x in range(number_of_rows):
    fake = Faker()
    c_e_a = fake.date_time()
    credentials_expire_at.append(c_e_a)

df = df.assign(credentials_expire_at=credentials_expire_at)

# firstName
firstName = []
for x in range(number_of_rows):
    fake = Faker()
    fN = fake.first_name()
    firstName.append(fN)

df = df.assign(firstName=firstName)

##
## lastName varchar255
lastName = []
for x in range(number_of_rows):
    fake = Faker()
    lN = fake.last_name()
    lastName.append(lN)

df = df.assign(lastName=lastname)
##

# email_primary
email_primary = []
for x in range(number_of_rows):
    fake = Faker()
    first_name = fake.first_name
    last_name = fake.last_name()
    e_p = [f"{fake.first_name()}.{fake.last_name()}@{fake.domain_name()}" for i in range(1)] # range 1 used to produce 1 email
    email_primary.append(e_p)

df = df.assign(email_primary=email_primary)

##
## online tinyint = 0 
##
online_list = []

for x in range(N):
    online = 0
    online = random.getrandbits(1)
    online_list.append(online)
df = df.assign(online = online_list)

##
## password_changed_on datetime
##

# created_on
created_on = []
for x in range(number_of_rows):
    fake = Faker()
    c_o = fake.date_time()
    created_on.append(c_o)

df = df.assign(created_on=created_on)

# land_line
land_line = []
for x in range(number_of_rows):
    l_l = random.randint(1, 100000000000000)
    land_line.append(l_l)

df = df.assign(land_line=land_line)

##
## mobile_phone varchar15
##

##
## status_message varchar50
##

##
## trainer tinyint = 0
trainer = 0
df = df.assign(trainer=trainer)

##
## supervisor mode int11 default = 0 
supervisor_mode_list = []

for x in range(N):
    supervisor_mode = 0
    supervisor_mode = random.getrandbits(1)
    supervisor_mode_list.append(supervisor_mode)
df = df.assign(supervisor_mode = supervisor_mode_list)

## counselor level 
counselor_level = []
for x in range(number_of_rows):
    cou_lvl = random.randint(1, 10000000000)
    counselor_level.append(cou_lvl)

df = df.assign(counselor_level=counselor_level)

## counselor years
counselor_years = []
for x in range(number_of_rows):
    cou_yrs = random.randint(1, 10000000000)
    counselor_years.append(cou_yrs)

df = df.assign(counselor_level=counselor_level)

##
## newbie tinyint = 1
##
newbie_list = []

for x in range(N):
    newbie = 1
    newbie = random.getrandbits(1)
    newbie_list.append(newbie)
df = df.assign(newbie = newbie_list)

## capacity
capacity = []
for x in range(number_of_rows):
    cap = random.randint(1, 10000000000)
    capacity.append(cap)

df = df.assign(capacity=capacity)

##
## initial supervisees assigned tinyint = 0
##
initial_supervisees_assigned_list = []

for x in range(N):
    initial_supervisees_assigned = 0
    initial_supervisees_assigned = random.getrandbits(1)
    initial_supervisees_assigned_list.append(initial_supervisees_assigned)
df = df.assign(ninitial_supervisees_assigned = initial_supervisees_assigned_list)



#  beta_features_raw
beta_features_raw = []
for x in range(number_of_rows):
    fake = Faker()
    b_f_r = fake.text() 
    beta_features_raw.append(b_f_r)

df = df.assign(beta_features_raw=beta_features_raw)

# two factor code
two_factor_code = []
for x in range(number_of_rows):
    two_fc = random.randint(1, 10000000000)
    two_factor_code.append(two_fc)

df = df.assign(two_factor_code=two_factor_code)

##
## two factor code expires datetime
##

##
## two factor auth type varchar255
##

##
## last released notes read id int(11)
## ideally this should be when a certain txt file is opened , but for now we will randomly generate a number
last_released_read_list = []

for x in range(N):
    last_released_read = random.getrandbits(1)
    last_released_read_list.append(last_released_read)
df = df.assign(last_released_read = last_released_read_list)

##
## spike form url varchar255
##

## is counselor displayed as staff tinyint = 0
is_counselor_displayed_as_staff = 0
df = df.assign(is_counselor_displayed_as_staff=is_counselor_displayed_as_staff)

## is practicum student tinyint = 0
is_in_practicum_student = 0
df = df.assign(is_in_practicum_student=is_in_practicum_student)


## is in peer support tinyint = 0
is_in_peer_support = 0
df = df.assign(is_in_peer_support=is_in_peer_support)


## last seen online at datetime
last_seen_online = []
for x in range (number_of_rows):
    fake = Faker()
    l_s_o = fake.time_object()
    last_seen_online.append(l_s_o)

df = df.assign(last_seen_online=last_seen_online)