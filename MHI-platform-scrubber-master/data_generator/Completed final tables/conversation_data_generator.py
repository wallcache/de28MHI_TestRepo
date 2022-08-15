def conversation_data_generator():
    # The required modules to run this code are seen below :
    from random import randint, randrange
    import pandas as pd
    from faker import Faker

    # The list of variable_names are the column names in the Conversation table constructed within MHI's SQL database. The list of data_types are the different data types of the variable names.
    # These two lists are both respectively ordered and hence easier to manipulate -> variable_names[x] has a data_type of data_types[x].
    variable_names = ['id', 'owner_id', 'queue_level_taken_id', 'startTime', 'endTime', 'state', 'priority', 'priority_predicted', 'post_conversation_survey_start_time',
    'post_conversation_survey_end_time', 'conversation_rating', 'conversation_feedback_description', 'addedToQueue', 'takenFromQueue', 'handled', 'engaged',
    'num_wait_time_notifications', 'spike_alert_sent', 'hash', 'transferObserver_id', 'firstMessage_id', 'language', 'active_auto_message_bot_conversation_id',
    'presenting_concern_message_id', 'is_stopped', 'source', 'is_risk_assessed', 'is_ceded', 'externalId', 'supervisor_notes']
    data_types = ['int', 'int', 'int', 'datetime', 'datetime', 'varchar', 'int', 'int', 'datetime', 'datetime', 'smallint', 'varchar', 'datetime', 'datetime',
    'tinyint', 'tinyint', 'smallint', 'tinyint', 'varchar', 'int', 'int', 'varchar', 'int', 'int', 'tinyint', 'varchar', 'tinyint', 'tinyint', 'int', 'varchar']

    # The function random_with_N_digits was created to help select a random integer up to a certain number of n digits.
    def random_with_N_digits(n):
        randint(10000000000, 99999999999)     # randint is inclusive at both ends
        randrange(10000000000, 100000000000)  # randrange is exclusive at the stop
        range_start = 10**(n-1)
        range_end = (10**n)-1
        return randint(range_start, range_end)

    #The second function below was used to generate random varchar data. The function returns a mix of letters and numbers.
    # Do note, the letters always appear in the first two figures and the remainder are numbers.
    def random_varchar():
        for x in range(1):
            fake = Faker()
            pc = fake.bothify('??#########')
        return pc

    # It is necessary to create a faker object so we can utilise the modules methods. 
    # number_of_rows is a variable that can determines how many rows of test data will be produced.
    fake = Faker()
    rows = input('How many rows do you want?:')
    number_of_rows = int(rows)

    # Sets up the list of test data and then creates the data depending on the data type
    alldata = []
    for element in data_types:
        test_data = []
        for x in range(number_of_rows):
            if element == 'int':
                test_data.append(random_with_N_digits(randrange(1,12)))
            if element == 'tinyint':
                test_data.append(random_with_N_digits(randrange(1,2)))
            if element == 'smallint':
                test_data.append(random_with_N_digits(randrange(1,7)))
            if element == 'datetime':
                test_data.append(fake.date_time())
            if element == 'varchar':
                test_data.append(random_varchar())
        alldata.append(test_data)

    # Insert the test data into a dataframe. Specify the column names and transpose to facilitate the opted table orientation.
    df = pd.DataFrame(alldata)
    tdf = df.T
    tdf.columns=variable_names

    # Enforce the default values which are specified in the SQL schema
    list1 = []
    for x in range(number_of_rows):
        list1.append(x)
    tdf = tdf.assign(id = list1)
    tdf = tdf.assign(conversation_rating = 0)
    tdf = tdf.assign(handled = 0)
    tdf = tdf.assign(engaged = 0)
    tdf = tdf.assign(num_wait_time_notifications = 0)
    tdf = tdf.assign(language = 'en')
    tdf = tdf.assign(is_risk_assessed = 0)

    print(tdf)
    tdf.to_csv('conversationdata.csv')

conversation_data_generator()