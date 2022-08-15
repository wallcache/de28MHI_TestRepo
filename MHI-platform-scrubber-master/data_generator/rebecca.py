# email_canonical

email_canonical = []
for x in range(number_of_rows):
    fake = Fake()
    first_name = fake.first_name
    last_name = fake.last_name()
    e_c = [f"{fake.first_name()}.{fake.last_name()}@{fake.domain_name()}" for i in range(1)] # range 1 used to produce 1 email
    email_canonical.append(e_c)

df = df.assign(email_canonical=email_canonical)



## password requested at
password_requested_at = []
for x in range(number_of_rows):
    fake = Faker()
    p_r_a = fake.date_time()
    password_requested_at.append(p_r_a)

df = df.assign(password_requested_at=password_requested_at)



## roles

roles = []
for x in range(number_of_rows):
    fake = Faker()
    r = fake.text()
    roles.append(r)

df = df.assign(roles=roles)


## last name varchar

last_name = []
for x in range(number_of_rows):
    fake = Faker()
    l_n = fake.text() 
    last_name.append(l_n)

df = df.assign(last_name=last_name)


# password_changed_on

password_changed_on = []
for x in range(number_of_rows):
    fake = Faker()
    p_c_o = fake.date_time()
    password_changed_on.append(p_c_o)

df = df.assign(password_changed_on=password_changed_on)


# mobile phone

def random_varchar():
    for x in range(1):
        fake = Faker()
        m_p = fake.bothify('??#########')
    return m_p


# status message 

status_message = []
for x in range(number_of_rows):
    fake = Faker()
    s_m = fake.text()
    status_message.append(s_m)

df = df.assign(status_message=status_message)


# two factor code expires datetime

two_factor_code = []
for x in range(number_of_rows):
    fake = Faker()
    t_f_c = fake.date_time()
    two_factor_code.append(t_f_c)

df = df.assign(two_factor_code=two_factor_code)


# two factor auth

two_factor_auth_type = []
for x in range(number_of_rows):
    fake = Faker()
    t_f_a_t = fake.text()
    two_factor_auth_type.append(t_f_a_t)

df = df.assign(two_factor_auth_type=two_factor_auth_type)


# spike form url

spike_form_url = []
for x in range(number_of_rows):
    fake = Faker()
    s_f_u = fake.text()
    spike_form_url.append(s_f_u)

df = df.assign(spike_form_url=spike_form_url)