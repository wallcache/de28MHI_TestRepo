# Importing the function that generates Conversation data
from conversation_data_generator import conversation_data_generator

# Using the function to produce the dataframe (tdf) and confirm results are correct
conversation_data_generator()

#Defining the unit test function
def conversation_data_test():
    for column in tdf:
        for element in tdf[column]:
            # int 
            if column in ['id', 'owner_id', 'queue_level_taken_id', 'priority', 'priority_predicted', 'transferObserver_id', 
            'firstMessage_id', 'active_auto_message_bot_conversation_id', 'presenting_concern_message_id', 'externalId']:
                assert type(element)==int and len(str(element)) <= 11
            # smallint
            elif column in ['conversation_rating', 'num_wait_time_notifications']:
                assert type(element)==int and len(str(element)) <= 6
            # tinyint
            elif column in ['handled', 'engaged', 'spike_alert_sent', 'is_stopped', 'is_risk_assessed', 'is_ceded']:
                assert type(element)==int and len(str(element)) <= 1
            # datetime, fake.date_time() is not a typical data_type
            elif column in ['startTime', 'endTime', 'post_conversation_survey_start_time', 'post_conversation_survey_end_time', 'addedToQueue', 'takenFromQueue']:
                assert len(str(element)) <= 19
            # varchar
            elif column in ['state', 'conversation_feedback_description', 'hash', 'language', 'source', 'supervisor_notes']:
                assert type(element)==str and len(str(element)) <= 11