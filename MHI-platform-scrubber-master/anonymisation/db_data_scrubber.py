import pymysql
import pandas as pd
# import pyarrow
from pandas.api.types import is_numeric_dtype
import numpy as np
import re
import sys
import scrubadub
import math
import time
import hashlib
import boto3
import yaml
from tqdm import tqdm

import logging
import logging.config


class DBScrubber():

    def __init__(self):
        """ Constructor of the class DBScrubber """

        # # this is for overriding the scrubadup default prefix, suffix values
        # scrubadub.filth.base.Filth.prefix = u'<'
        # scrubadub.filth.base.Filth.suffix = u'>'
        # self.scrubber = scrubadub.Scrubber()

        try:
            self.set_format_values()
            self.read_run_config_parameters()
            self.read_s3_bucket_data()
            self.open_db_connection()
            self.read_metadata()
            self.get_table_list()
        except Exception as ex:
            print(self.errstr + 'ERROR at initializing the environment...')
            print(ex)

    def set_format_values(self):
        # these values are just for highlighting the print statements
        errcol = '\033[91m'
        warncol = '\033[92m'
        endcol = '\033[0m'
        self.errstr = f"{errcol}--> " + endcol
        self.warnstr = f"{warncol}--> " + endcol

        # this is for overriding the scrubadup default prefix, suffix values
        scrubadub.filth.base.Filth.prefix = u'<'
        scrubadub.filth.base.Filth.suffix = u'>'
        self.scrubber = scrubadub.Scrubber()

    def run_process(self):
        """ Running the all process based on the run_config.yml file parameter values"""

        timebegin = time.time()

        # exporting data to pickles
        self.export_tables_to_pickles()

        # anonymising the tables
        self.anonymize_tables()

        timelapsed = time.time() - timebegin
        print('\n*****************************************************************')
        print('[ DATA PROCESS COMPLETED: Total Elapsed time: ' + str(round(timelapsed, 2)) + ' sec ]')
        self.logger.info('[ DATA PROCESS COMPLETED: Total Elapsed time: ' + str(round(timelapsed, 2)) + ' sec ]')


    def read_run_config_parameters(self):
        """ Reading the run time parameters value from run_config.yml """

        if len(sys.argv) <= 1:
            print('Missing argument. Please give the path for run_config.yml')
            self.logger.error('Missing argument. Please give the path for run_config.yml')
            sys.exit()

        config_file_path = sys.argv[1]

        params = None

        try:
            with open(config_file_path) as file:
                params = yaml.load(file, Loader=yaml.FullLoader)

            if params is None:
                print('Could not read run parameters')
                self.logger.error('Could not read run parameters')
                sys.exit()

            # TODO : We don't have to assign the values one by one, it can be automated later.
            self.run_as_test = params['run_as_test']
            self.run_at_local = params['run_at_local']

            self.select_tables_only = params['select_tables_only']
            self.hashids = params['hashids']

            self.logfile_path = params['logfile_path']
            self.home_dir = params['home_dir']
            self.db_pword_s3_key = params['db_pword_s3_key']
            self.hash_salt_s3_key = params['hash_salt_s3_key']

            if self.run_as_test==True:
                self.output_dir = params['output_dir_test']
                self.s3_bucket_credentials = params['s3_bucket_credentials_test']
                self.raw_data_output_dir = params['raw_data_output_dir_test']
            else:
                self.output_dir = params['output_dir']
                self.s3_bucket_credentials = params['s3_bucket_credentials']
                self.raw_data_output_dir = params['raw_data_output_dir']

            if self.run_at_local==True:
                self.db_user = params['db_user_test']
                self.db_name = params['db_name_test']
                self.db_port = params['db_port_test']
                self.db_host_name = params['db_host_name_test']
            else:
                self.db_user = params['db_user']
                self.db_name = params['db_name']
                self.db_port = params['db_port']
                self.db_host_name = params['db_host_name']

            self.chunksize = params['chunksize']
            self.infotype_percentage = params['infotype_percentage']
            self.scrub_mask_all = params['scrub_mask_all']
            self.scrub_mask_url = params['scrub_mask_url']
            self.scrub_mask_name = params['scrub_mask_name']
            self.scrub_mask_email = params['scrub_mask_email']
            self.scrub_mask_phone = params['scrub_mask_phone']
            self.scrub_mask_postcode = params['scrub_mask_postcode']
            self.scrub_mask_street = params['scrub_mask_street']
            self.scrub_mask_nino = params['scrub_mask_nino']

            self.regex_postcode = r"{}".format(params['regex_postcode'])
            # self.regex_postcode_deduplicator = params['regex_postcode_deduplicator']
#            self.regex_phone1 = params['regex_phone1']
#            self.regex_phone2 = params['regex_phone2']
            self.regex_phone3 = params['regex_phone3']
            self.regex_email = params['regex_email']
            self.regex_nino = params['regex_nino']

            self.infotypes = params['infotypes']
            self.infotype_prefix = params['infotype_prefix']
            self.infotype_suffix = params['infotype_suffix']

            # this was the code before trying changing the prefix and suffix of the scubadub library
            # self.infotypes = ['{{'+str(i)+'}}' for i in self.infotypes]
            self.infotypes = [self.infotype_prefix+str(i) + self.infotype_suffix for i in self.infotypes]

            self.infotypes.append(self.scrub_mask_url)
            self.infotypes.append(self.scrub_mask_phone)
            self.infotypes.append(self.scrub_mask_name)
            self.infotypes.append(self.scrub_mask_email)

            self.select_tables = params['select_tables']

            self.read_from_pickle = params['read_from_pickle']
            self.full_export = params['full_export']

            self.run_export_step = params['run_export_step']
            self.run_anonymisation_step = params['run_anonymisation_step']

            self.use_external_name_dictionary = params['use_external_name_dictionary']
            self.use_db_usertable_dictionary = params['use_db_usertable_dictionary']

            # self.replace_with_identifiers = params['replace_with_identifiers']
            self.exclude_tables = params['exclude_tables']

            self.enable_exclude_tables = params['enable_exclude_tables']
            self.enable_split_tables = params['enable_split_tables']

            if self.run_as_test==True:
                self.split_tables = []
            else:
                self.split_tables = params['split_tables']

            self.partition_record_count = params['partition_record_count']

            self.enable_exclude_phones = params['enable_exclude_phones']
            self.enable_exclude_urls = params['enable_exclude_urls']
            self.enable_exclude_emails = params['enable_exclude_emails']
            self.enable_exclude_words = params['enable_exclude_words']

            self.exclude_word_relative_path = params['exclude_word_relative_path']
            self.exclude_phone_relative_path = params['exclude_phone_relative_path']
            self.exclude_email_relative_path = params['exclude_email_relative_path']
            self.exclude_url_relative_path = params['exclude_url_relative_path']

            self.exclude_words = []
            self.exclude_phones = []
            self.exclude_urls = []
            self.exclude_emails = []

            if self.enable_exclude_words==True:

                with open(self.home_dir+self.exclude_word_relative_path) as f:
                    content = f.readlines()

                self.exclude_words = [x.strip().replace('\n', '') for x in content]
                self.exclude_words = [e.upper() for e in self.exclude_words]

            if self.enable_exclude_phones==True:
                with open(self.home_dir+self.exclude_phone_relative_path) as f:
                    content = f.readlines()

                self.exclude_phones = [x.strip().replace('\n', '') for x in content]
                self.exclude_phones = [e.upper() for e in self.exclude_phones]

            if self.enable_exclude_urls==True:
                with open(self.home_dir+self.exclude_url_relative_path) as f:
                    content = f.readlines()

                self.exclude_urls = [x.strip().replace('\n', '') for x in content]
                self.exclude_urls = [e.upper() for e in self.exclude_urls]

            if self.enable_exclude_emails==True:
                with open(self.home_dir+self.exclude_email_relative_path) as f:
                    content = f.readlines()

                self.exclude_emails = [x.strip().replace('\n', '') for x in content]
                self.exclude_emails = [e.upper() for e in self.exclude_emails]

            self.enable_name_scrubbing = params['enable_name_scrubbing']
            self.enable_url_scrubbing = params['enable_url_scrubbing']
            self.enable_phone_scrubbing = params['enable_phone_scrubbing']
            self.enable_email_scrubbing = params['enable_email_scrubbing']

            self.enable_name_scrubbing2 = params['enable_name_scrubbing2']
            self.enable_url_scrubbing2 = params['enable_url_scrubbing2']
            self.enable_phone_scrubbing2 = params['enable_phone_scrubbing2']
            self.enable_email_scrubbing2 = params['enable_email_scrubbing2']
            self.enable_postcode_scrubbing2 = params['enable_postcode_scrubbing2']
            self.enable_street_scrubbing2 = params['enable_street_scrubbing2']
            self.enable_nino_scrubbing2 = params['enable_nino_scrubbing2']

            if self.enable_name_scrubbing==False:
                self.scrubber.remove_detector(name='name')

            if self.enable_email_scrubbing==False:
                self.scrubber.remove_detector(name='email')

            if self.enable_url_scrubbing==False:
                self.scrubber.remove_detector(name='url')

            if self.enable_phone_scrubbing==False:
                self.scrubber.remove_detector(name='phone')

            self.display_scrubbed_text = params['display_scrubbed_text']
            self.street_columns = params['street_columns']
            self.test_record_limit = params['test_record_limit']

            self.init_loggers()
            self.logger.info('Read run_config.yml content successfully')

        except Exception as ex:
            self.logger.error('ERROR at reading the run_config.yml file')
            print(self.errstr + 'ERROR at reading the run_config.yml file')
            print(ex)
            sys.exit()


    def read_s3_bucket_data(self):
        """ Reading the database password and hash salt values from the S3 bucket files """

        try:
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(name=self.s3_bucket_credentials)

            for obj in bucket.objects.filter(Prefix='data_import_zone/'):
                if obj.key == self.db_pword_s3_key:
                    self.db_pass = obj.get()["Body"].read()
                    self.db_pass = self.db_pass[:-1].decode('utf-8')

                if obj.key == self.hash_salt_s3_key:
                    self.hash_salt = obj.get()["Body"].read()
                    self.hash_salt = self.hash_salt[:-1]

            self.logger.info('Read database credentials and hash salt from S3 bucket completed')

        except Exception as ex:
            self.logger.error('ERROR at reading the files from s3 bucket. Program will be terminated.')
            print(self.errstr + 'ERROR at reading the files from s3 bucket. Program will be terminated.')
            print(ex)
            sys.exit()


    def read_metadata(self):
        """ Reading metadata from resource files such as names, street names. """

        try:
            self.firstnames_dic = {}

            if self.use_external_name_dictionary:
                with open(self.home_dir + 'resources/nltk_male_female_names.txt') as fp:
                    for line in fp:
                        self.firstnames_dic[line.replace('\n', '')] = 1

            if self.use_db_usertable_dictionary:
                sql_query = 'select firstName, lastName, username from user;'
                dfusers = pd.read_sql_query(sql=sql_query, con=self.conn)

                for ix, row in dfusers.iterrows():
                    fullname = str(row[0]) + ' ' + str(row[1])
                    fullname = fullname.strip()

                    # skip if fullname contains any number
                    if bool(re.search(r'\d', fullname))==True:
                        continue

                    fullname = fullname.replace('-', ' ')
                    fullname = fullname.replace('(', ' ')
                    fullname = fullname.replace(')', ' ')
                    fullname = fullname.upper().strip()
                    wordlist = fullname.split(' ')
                    wordlist = [w for w in wordlist if len(w) > 1]

                    for word in wordlist:
                        self.firstnames_dic[word] = 1

            # self.postcode_finder = re.compile(self.regex_postcode)
            # self.postcode_deduplicator = re.compile(self.regex_postcode_deduplicator)

            self.is_numeric = False
            self.dfdata = pd.DataFrame()
            self.tablecontext = {}
            self.hasher = hashlib.sha256()

            self.street_names = {}
            if self.enable_street_scrubbing2==True:
                with open(self.home_dir + 'resources/street_names.txt') as fp:
                    for line in fp:
                        self.street_names[line.replace('\n', '')] = 1

            self.logger.info('Reading metadata files completed')

        except Exception as ex:
            self.logger.error('ERROR at reading the metadata files. The program is terminated.')
            print(self.errstr + 'ERROR at reading the metadata. The program is terminated.')
            print(ex)
            sys.exit()


    def open_db_connection(self):

        """ Opening database connection for the credentials read from the config files """

        try:
            self.conn = pymysql.connect(host=self.db_host_name, user=self.db_user, passwd=self.db_pass, db=self.db_name, port=self.db_port)
            self.logger.info('Database connection opened')
        except Exception as ex:
            self.logger.error('ERROR at opening the connection to the database. The program is terminated.')
            print(self.errstr + 'ERROR at opening the connection to the database. The program is terminated.')
            print(ex)
            sys.exit()


    def get_table_list(self):

        """ Creating a list for the tables to be processed based on the configuration options """

        try:
            if self.select_tables_only ==True:
                self.db_table_list = self.select_tables.copy()
            else:
                sql_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + self.db_name + "';"
                dftablelist = pd.read_sql_query(sql_query, self.conn, index_col=None)
                self.db_table_list = dftablelist['table_name'].unique().tolist()

            self.db_table_list = [t.upper() for t in self.db_table_list]
            self.logger.info('Table list reading completed')

            print('Table list to process: ' + str(self.db_table_list))

        except Exception as ex:
            self.logger.error('ERROR at getting the table list. The program is terminated')
            print(self.errstr + 'ERROR at getting the table list. The program is terminated')
            print(ex)
            sys.exit()


    def export_tables_to_pickles(self):

        """ Exporting the tables data read from database into pickle files """

        # if the run parameter set False, then skip this step do nothing
        if self.run_export_step==False:
            return

        self.logger.info('PICKLE EXPORT STARTED')

        print()
        print('---->> PICKLE EXPORT STARTED ...')
        print()

        timebegin = time.time()

        # if full_export then export everything
        if self.full_export == True:

            for tablename in self.db_table_list:

                self.current_table = tablename

                if self.enable_exclude_tables ==True:
                    if tablename in self.exclude_tables:
                        print(self.warnstr + 'EXCLUDED table : ' + tablename + '\n')
                        self.logger.warning('EXCLUDED table ' + tablename)
                        continue

                self.get_table_context()

                if len(self.tablecontext.keys())==0:
                    print()
                    print(self.warnstr + 'SKIPPED export, table context not exists: ' + tablename + '\n')
                    self.logger.warning('SKIPPED export, table context not exists: ' + tablename)
                    continue

                # if the table is marked as not to export
                if self.tablecontext['EXPORT_EXEMPT'] == 'Y':
                    print(self.warnstr + "EXEMPTED table : " + tablename)
                    self.logger.warning("EXEMPTED table " + tablename)
                    continue

                try:
#                    self.export_data_to_pickle(table_name=tablename)
                    self.export_data_to_pickle()
                except Exception as ex:
                    print()
                    print(self.errstr + 'ERROR at pickle export ' + tablename + ' table')
                    self.logger.error('ERROR at pickle export ' + tablename + ' table')
                    print()
                    print(ex)
                    pass

            timelapsed = time.time() - timebegin
            print()
            print('[ PICKLE EXPORT STEP COMPLETED: Elapsed time: ' + str(round(timelapsed, 2)) + ' sec ]')
            print()
            print()
            self.logger.info('[ PICKLE EXPORT STEP COMPLETED: Elapsed time: ' + str(round(timelapsed, 2)) + ' sec ]')

        else:

            # first id tables and then foreignkey tables in second step
            for tablename in self.db_table_list:
                self.current_table = tablename

                self.get_table_context()
                print(self.warnstr + 'Table ' + tablename )

                if len(self.tablecontext.keys()) == 0:
                    print(self.warnstr + 'SKIPPED export, table context not exists: ' + tablename)
                    self.logger.warning(self.warnstr + 'SKIPPED export, table context not exists: ' + tablename)
                    continue

                if self.tablecontext['TYPE'] == 'FOREIGN_KEY':
                    continue

                # if the table is marked as not to export
                if self.tablecontext['EXPORT_EXEMPT'] == 'Y':
                    print(self.warnstr + 'EXEMPTED to pickle export ' + tablename + ' table')
                    self.logger.warning('EXEMPTED to pickle export ' + tablename + ' table')
                    continue

                try:
#                    self.export_data_to_pickle(table_name=tablename)
                    self.export_data_to_pickle()
                except Exception as ex:
                    print(self.errstr + 'ERROR at pickle export ' + tablename + ' table')
                    self.logger.error('ERROR at pickle export ' + tablename + ' table')
                    print(ex)
                    pass


            # exporting tables with foreign keys in second pass, this part can be combined in one loop later
            for tablename in self.db_table_list:

                print('Table ' + tablename)

                # reading the table context
                self.get_table_context()

                if len(self.tablecontext.keys()) == 0:
                    print(self.warnstr + 'SKIPPED export, table context not exists: ' + tablename)
                    self.logger.warning('SKIPPED export, table context not exists: ' + tablename)
                    continue

                if self.tablecontext['TYPE'] != 'FOREIGN_KEY':
                    continue

                # if the table is marked as not to export
                if self.tablecontext['EXPORT_EXEMPT'] == 'Y':
                    print(self.warnstr + 'EXEMPTED to pickle export ' + tablename + ' table')
                    self.logger.warning('EXEMPTED to pickle export ' + tablename + ' table')
                    continue

                try:
                    self.export_data_to_pickle()
                    print('Pickle export completed: ' + tablename)
                    self.logger.info('Pickle export completed: ' + tablename)
                except Exception as ex:
                    print(self.errstr + 'ERROR at pickle export ' + tablename + ' table')
                    self.logger.error('ERROR at pickle export ' + tablename + ' table')
                    print(ex)
                    pass


    def anonymize_tables(self):

        """ Anonymising the tables data exported into the pickle files """

        timebegin = time.time()

        # if the run parameter set False, then skip this step do nothing
        if self.run_anonymisation_step==False:
            return

        if self.conn is None:
            print(self.errstr + 'ERROR: Database connection is not open. The program is terminated.')
            sys.exit()

        print()
        print()
        print('----->> DATA ANONYMISATION STEP STARTED')
        self.logger.info('DATA ANONYMISATION STEP STARTED')

        for tablename in self.db_table_list:
            self.current_table = tablename
            print('Table ' + tablename)
            self.logger.info('Started processing ' + tablename + ' table')

            if self.enable_exclude_tables==True:
                if tablename in self.exclude_tables:
                    print('Table excluded from scrubbing : ' + tablename)
                    self.logger.warning('Table excluded from scrubbing ' + tablename)
                    continue

            try:
#                self.anonymize_a_table(tablename=tablename)
                self.anonymize_table()
            except Exception as ex:
                print(self.errstr + 'ERROR at anonymising ' + tablename + ' table')
                self.logger.error('ERROR at anonymising ' + tablename + ' table')
                print(ex)
                pass

        timelapsed = time.time() - timebegin
        print('[ DATA ANONYMISATION STEP COMPLETED: Elapsed time: ' + str(round(timelapsed, 2)) + ' sec ]')
        print()
        print()
        self.logger.info('[ DATA ANONYMISATION STEP COMPLETED: Elapsed time: ' + str(round(timelapsed, 2)) + ' sec ]')

        self.conn.close()

    def anonymize_table(self):

        """ Anonymising the current table """

        self.get_table_context()

        try:

            if self.current_table in self.split_tables:
                sql_query_max_id = 'select max(id) as MAX_ID from ' + self.current_table.lower() + ';'
                dfmax = pd.read_sql_query(sql_query_max_id, self.conn, index_col=None)
                max_id = dfmax.iloc[0]['MAX_ID']
                partition_count = math.ceil(max_id / self.partition_record_count)

                for pn in range(1, partition_count +1):

                    # it is assumed will always read from pickle file
                    if self.read_from_pickle == True:
#                        datafound = self.read_table_data_from_pickle(table_name=tablename, partitionno=pn)
                        datafound = self.read_table_data_from_pickle(partitionno=pn)

                    # TODO: the else can be closed as we always do read from pickle. And read_table_data_from_db is no longer being maintened. Must be checked if needed.
                    # else:
                    #     datafound = self.read_table_data_from_db(table_name=tablename)

                    if datafound ==True:

                        # scrubbing data
                        self.scrub_dataframe()

                        # exporting the data to csv
                        self.export_dataframe_tocsv(partitionno=pn)
                        # exporting the data to parquet
                        #self.export_dataframe_toparquet(partitionno=pn)

                        print(self.warnstr + 'Completed anonymisation of table ' + self.current_table)
                        #print('**************************************************************')
                        print()

                        self.logger.info('Completed anonymisation of table ' + self.current_table)


            else:
                # table name is not in split tables

                if self.read_from_pickle ==True:
                    datafound = self.read_table_data_from_pickle(partitionno=None)

                # TODO: Currently it is always assumed to read from pickle files. Must be edited if reading directly from db is needed
                # else:
                #     datafound = self.read_table_data_from_db(table_name=tablename)

                if datafound==True:

                    # scrubbing data
                    self.scrub_dataframe()

                    # exporting the data to csv
                    self.export_dataframe_tocsv(partitionno=None)
                    # exporting the data to parquet
                    #self.export_dataframe_toparquet(partitionno=None)

                    print('Completed anonymisation of table ' + self.current_table)
                    print('**************************************************************')
                    print()
                    self.logger.info('Completed anonymisation of table ' + self.current_table)

        except Exception as ex:
            print(self.errstr + 'ERROR at anonymisation table ' + self.current_table)
            self.logger.error('ERROR at anonymisation table ' + self.current_table)
            print(ex)
            return


#    def read_table_data_from_db(self, table_name):
    def read_table_data_from_db(self):

        """ Reading data from database directly """
        # this method is not being used anymore
        sql_query = "SELECT * FROM " + self.db_name + '.' + self.current_table + " ;"

        try:
            self.dfdata = pd.read_sql_query(sql_query, self.conn, index_col=None)
            self.dfdata = self.dfdata.fillna('')
            print('Completed reading data from database for table ' + self.current_table + '(' + str(len(self.dfdata)) + ' rows read)')
            self.logger.info('Completed reading data from database for table ' + self.current_table + '(' + str(len(self.dfdata)) + ' rows read)')
            return True
        except Exception as ex:
            print(self.errstr + 'ERROR at reading data from database for table ' + self.current_table)
            self.logger.error('ERROR at reading data from database for table ' + self.current_table)
            print(ex)
            return False


    def export_data_to_pickle(self):

        """ Exporting current table's data into the pickle file """

        if self.current_table in self.split_tables and self.enable_split_tables==True:

            sql_query_max_id = 'select max(id) as MAX_ID from ' + self.current_table.lower() + ';'
            dfmax = pd.read_sql_query(sql_query_max_id, self.conn, index_col=None)
            max_id = dfmax.iloc[0]['MAX_ID']
            print('max id value ' + str(max_id))
            partition_count = math.ceil(max_id/self.partition_record_count)
            print('partition count ' + str(partition_count))
            min_max_ranges = []

            for p in range(0, partition_count):
                min_val = self.partition_record_count * p
                max_val = self.partition_record_count * (p + 1) -1
                min_max_ranges.append((min_val,max_val))

            # now adding the last partition
            min_max_ranges.append((max_val,max_id))

            partition_index = 1
            for rn in min_max_ranges:

                sql_query = self.prepare_query_for_table(use_partition=True, id_min_val=rn[0], id_max_val=rn[1])
                try:
                    self.dfdata = pd.read_sql_query(sql_query, self.conn, index_col=None)
                    self.dfdata = self.dfdata.fillna('')
                    self.dfdata.columns = [c.upper() for c in self.dfdata.columns]
                    self.dfdata.to_pickle(self.raw_data_output_dir + self.current_table + '_PART_' + str(partition_index) + '.pkl')

                    # write primary key value to seek value file
                    if self.tablecontext['ID_COLUMN'] != '' and self.full_export == False:
                        max_column_value = self.dfdata[self.tablecontext['ID_COLUMN']].astype('int64').max()
                        self.update_seek_value(max_column_value)
                        print('Last ID value updated for ' + self.tablecontext['ID_COLUMN'] + ' : ' + str(
                            max_column_value))

                        self.logger.info('Last ID value updated for ' + self.tablecontext['ID_COLUMN'] + ' ' + str(
                            max_column_value))

                    partition_index += 1
                    print(self.current_table + ' Part-' + str(partition_index) + ' : Pickle export for completed (' + str(len(self.dfdata)) + ' rows read)')
                    self.logger.info(self.current_table + ' Part-' + str(partition_index) + ' Pickle export for completed (' + str(len(self.dfdata)) + ' rows read)')

                except Exception as ex:
                    print(self.errstr + 'ERROR at exporting data to pickle file for table ' + self.current_table)
                    self.logger.error('ERROR at exporting data to pickle file for table ' + self.current_table)
                    print(ex)

        else:

            sql_query = self.prepare_query_for_table(use_partition=False)

            try:
                self.dfdata = pd.read_sql_query(sql_query, self.conn, index_col=None)
                self.dfdata = self.dfdata.fillna('')
                self.dfdata.columns = [c.upper() for c in self.dfdata.columns]
                self.dfdata.to_pickle(self.raw_data_output_dir + self.current_table + '.pkl')

                # write primary key value to seek value file
                if self.tablecontext['ID_COLUMN'] != '' and self.full_export==False:
                    max_column_value = self.dfdata[self.tablecontext['ID_COLUMN']].astype('int64').max()
                    self.update_seek_value(max_column_value)
                    print('Last ID value updated for ' + self.tablecontext['ID_COLUMN'] + ' : ' + str(max_column_value))
                    self.logger.info('Last ID value updated for ' + self.tablecontext['ID_COLUMN'] + ' : ' + str(max_column_value))

                print(self.current_table + ' : Pickle export for completed (' + str(len(self.dfdata)) + ' rows read)')
                self.logger.info(self.current_table + ' Pickle export for completed (' + str(len(self.dfdata)) + ' rows read)')

            except Exception as ex:
                print(self.errstr + 'ERROR at exporting data to pickle file for table ' + self.current_table)
                self.logger.error('ERROR at exporting data to pickle file for table ' + self.current_table)
                print(ex)

    def get_foreign_key_id_values(self):

        """ Getting foreign key last id values for the current table """
        where_str = ''
        try:

            sql_query = """SELECT upper(t2.REFERENCED_TABLE_NAME) as REFERENCED_TABLE_NAME, upper(t2.REFERENCED_COLUMN_NAME) as REFERENCED_COLUMN_NAME
             FROM INFORMATION_SCHEMA.COLUMNS t1, INFORMATION_SCHEMA.KEY_COLUMN_USAGE t2 
             WHERE t1.TABLE_SCHEMA = 'mhidb' 
             and t1.TABLE_NAME = t2.table_name 
             and t1.column_name = t2.column_name 
             and t2.referenced_table_name is not null 
             and t1.data_type = 'int' and upper(t1.table_name) = '""" + self.current_table + """';"""

            dfforeign = pd.read_sql_query(sql_query, self.conn, index_col=None)
            if len(dfforeign) > 0:
                for ix, row in dfforeign.iterrows():
                    if where_str == '':
                        where_str = where_str + ' ' + row[1] + ' > ' + self.get_id_column_last_value()
                    else:
                        where_str = where_str + ' AND ' + row[1] + ' > ' + self.get_id_column_last_value()

                where_str = ' WHERE ' + where_str

            self.logger.info('Foreign key ID values read for the table ' + self.current_table)
            return where_str

        except Exception as ex:
            print('Error at reading foreign key ID values for the table ' + self.current_table)
            print(ex)
            self.logger.error('Error at reading foreign key ID values for the table ' + self.current_table)



    def prepare_query_for_table(self, use_partition=False, id_min_val=None, id_max_val=None):

        """ Preparing a select query for the current table """

        if self.current_table.upper() == 'TRANSFERREQUEST':
            tablename = 'TransferRequest'
        else:
            tablename = self.current_table.lower()

        if self.full_export ==True or self.tablecontext['ALWAYS_FULL_EXPORT'] == 'Y':
            if self.run_as_test == True:
                sql_query = "SELECT * FROM " + self.db_name + '.' + tablename + " limit " + str(self.test_record_limit) +";"
            else:
                if use_partition==True:
                    sql_query = "SELECT * FROM " + self.db_name + '.' + tablename + " where ID between " + str(id_min_val) + " and " + str(id_max_val) + ";"
                else:
                    sql_query = "SELECT * FROM " + self.db_name + '.' + tablename + ";"

        else:
            # last id key, foreign key, date keys etc.
            if self.tablecontext['TYPE'] == 'ID':
                if self.run_as_test ==True:
                    sql_query = "SELECT * FROM " + tablename + ' WHERE ' + self.tablecontext['ID_COLUMN'] + ' > ' + str(self.tablecontext['ID_COLUMN_LAST_VALUE']) + " limit 500;"
                else:
                    sql_query = "SELECT * FROM " + tablename + ' WHERE ' + self.tablecontext[
                        'ID_COLUMN'] + ' > ' + str(self.tablecontext['ID_COLUMN_LAST_VALUE']) + ";"
            elif self.tablecontext['TYPE'] == 'FOREIGN_KEY':
                sql_query = "SELECT * FROM " + self.db_name + '.' + tablename + self.get_foreign_key_id_values() + ";"

        self.logger.info('Query prepared for the table ' + self.current_table)
        return sql_query


#    def get_id_column_last_value(self, idcolumn):
    def get_id_column_last_value(self):

        """ Getting the last value of the ID for the current table """

        table_column = self.current_table + '.' + self.tablecontext['ID_COLUMN']

        try:
            idvalues_str = open(self.home_dir + 'config/scrub_seek_values.txt', 'r').read()
            idvalues = eval(idvalues_str)

            if table_column in idvalues.keys():
                return idvalues[table_column]
            else:
                return 0 # zero for the first run to set the initial value

            self.logger.info('ID Column last value read for the table ' + self.current_table)
        except Exception as ex:
            self.logger.error('Error at reading ID Column last value for the table ' + self.current_table)
            return 0


    def get_hash_column_list(self):

        """ Getting the column list to be hashed for the current table """

        try:
            dfhash = pd.read_csv(self.home_dir + 'config/scrub_column_config.txt', sep=',', index_col=None)
            dfhash = dfhash[(dfhash.TABLE_NAME==self.current_table) & (dfhash.TO_BE_HASHED == 'Y')]
            hash_column_list = dfhash['COLUMN_NAME'].to_list()
            self.logger.info('Hash column list read for the table ' + self.current_table)
            return hash_column_list
        except Exception as ex:
            self.logger.error('ERROR reading hash column list for table ' + self.current_table)
            print(self.errstr + 'ERROR reading hash column list for table ' + self.current_table)
            print(ex)
            if self.hashids==True:
                print('The program is terminated.')
                sys.exit()

    def get_hash_column_always_list(self):

        """ Getting the column always list to be hashed for the current table """

        try:
            dfhash = pd.read_csv(self.home_dir + 'config/scrub_column_config.txt', sep=',', index_col=None)
            dfhash = dfhash[(dfhash.TABLE_NAME==self.current_table) & (dfhash.TO_BE_HASHED == 'X')]
            hash_column_list = dfhash['COLUMN_NAME'].to_list()
            self.logger.info('Hash column list read for the table ' + self.current_table)
            return hash_column_list
        except Exception as ex:
            self.logger.error('ERROR reading hash column list for table ' + self.current_table)
            print(self.errstr + 'ERROR reading hash column list for table ' + self.current_table)
            print(ex)
            if self.hashids==True:
                print('The program is terminated.')
                sys.exit()

    def get_fullscrub_column_list(self):

        """ Getting the column list to be fully scrubbed for the current table """

        try:
            df = pd.read_csv(self.home_dir + 'config/scrub_column_config.txt', sep=',', index_col=None)
            df = df[(df.TABLE_NAME==self.current_table) & (df.FULL_SCRUB == 'Y')]
            scrub_column_list = df['COLUMN_NAME'].to_list()
            self.logger.info('Scrub column list read for the table ' + self.current_table)
            return scrub_column_list
        except Exception as ex:
            self.logger.error('ERROR reading scrub column list for table ' + self.current_table)
            print(self.errstr + 'ERROR reading scrub column list for table ' + self.current_table)
            print('The program is terminated')
            print(ex)
            sys.exit()


#    def get_scrub_exempt_column_list(self, tablename):
    def get_scrub_exempt_column_list(self):

        """ Getting the exempt column list(no hash, no scrub on them) for the current table """

        try:
            df = pd.read_csv(self.home_dir + 'config/scrub_column_config.txt', sep=',', index_col=None)
            df = df[(df.TABLE_NAME==self.current_table) & (df.SCRUB_EXEMPT == 'Y')]
            exempt_column_list = df['COLUMN_NAME'].to_list()
            self.logger.info('Exempt column list read for the table ' + self.current_table)
            return exempt_column_list
        except Exception as ex:
            self.logger.error('ERROR reading the scrub exempt column ' + self.current_table)
            print(self.errstr + 'ERROR reading the scrub exempt column ' + self.current_table)
            print(ex)
            return []


    def get_numeric_data_column_list(self):

        """ Getting the numeric data types for each column """

        try:
            df = pd.read_csv(self.home_dir + 'config/scrub_column_config.txt', sep=',', index_col=None)
            df1 = df[(df.TABLE_NAME==self.current_table) & (df.DB_DATA_TYPE=='int')]
            df2 = df[(df.TABLE_NAME==self.current_table) & (df.DB_DATA_TYPE=='tinyint')]
            df3 = df[(df.TABLE_NAME==self.current_table) & (df.DB_DATA_TYPE=='smallint')]
            numeric_column_list = df1['COLUMN_NAME'].to_list() + df2['COLUMN_NAME'].to_list() + df3['COLUMN_NAME'].to_list()
            self.logger.info('Numeric column list read for the table ' + self.current_table)
            print("Numeric column list: " + str(numeric_column_list))
            return numeric_column_list
        except Exception as ex:
            self.logger.error('ERROR reading the data type column ' + self.current_table)
            print(self.errstr + 'ERROR reading the data type column ' + self.current_table)
            print(ex)
            return []

#    def get_table_context(self, tablename):
    def get_table_context(self):

        """ Getting the context details of the current table """

        self.tablecontext = {}

        try:

            # reading the table context
            dftableconfig = pd.read_csv(self.home_dir + 'config/scrub_table_config.txt', sep=';', index_col=None)
            dftableconfig = dftableconfig.fillna('')
            dftableconfig = dftableconfig[dftableconfig.TABLE_NAME==self.current_table.upper()]

            if len(dftableconfig) ==0:
                print('Error: Can not find context for ' + self.current_table)
                return

            self.tablecontext['TABLE_NAME'] = dftableconfig.iat[0, 0]
            self.tablecontext['ID_COLUMN'] = dftableconfig.iat[0, 1]
            self.tablecontext['EXPORT_EXEMPT'] = dftableconfig.iat[0, 2]
            self.tablecontext['GETS_UPDATE'] = dftableconfig.iat[0, 3]
            self.tablecontext['ALWAYS_FULL_EXPORT'] = dftableconfig.iat[0, 4]
            self.tablecontext['HAS_FOREIGN_KEY'] = dftableconfig.iat[0, 5]

            # reading the last value of the primary key: ID field
            if self.tablecontext['ID_COLUMN'] != '':
                id_column_last_value = self.get_id_column_last_value()
                self.tablecontext['ID_COLUMN_LAST_VALUE'] = id_column_last_value
                self.tablecontext['TYPE'] = 'ID'
            elif self.tablecontext['ALWAYS_FULL_EXPORT'] == 'Y':
                self.tablecontext['TYPE'] = 'FULL_EXPORT'
            else:
                self.tablecontext['TYPE'] = 'FOREIGN_KEY' # default type value is foreign key

            # reading the hash columns
            self.tablecontext['HASH_COLUMNS'] = self.get_hash_column_list()
            self.tablecontext['HASH_COLUMNS_ALWAYS'] = self.get_hash_column_always_list()

            # reading the full scrub columns
            self.tablecontext['FULL_SCRUB_COLUMNS'] = self.get_fullscrub_column_list()

            # reading the scrub exempt columns
            self.tablecontext['SCRUB_EXEMPT_COLUMNS'] = self.get_scrub_exempt_column_list()

            # columns of type int
            self.tablecontext['NUMERIC_DATA_COLUMNS'] = self.get_numeric_data_column_list()

            self.logger.info('Table context read for table ' + self.current_table)

        except Exception as ex:
            self.logger.error('ERROR at reading the table context table: ' + self.current_table)
            print(self.errstr + 'ERROR at reading the table context table: ' + self.current_table)
            print(ex)



    def read_table_data_from_pickle(self, partitionno=None):

        """ Reading the data from pickle file into a Pandas Dataframe """

        try:
            if partitionno is not None:
                self.dfdata = pd.read_pickle(self.raw_data_output_dir + self.current_table + '_PART_' + str(partitionno) + '.pkl')
                print('\nData read from pickle file for table ' + self.current_table + ' Part-' + str(partitionno) + ' (' + str(len(self.dfdata)) + ' rows read)' )
                self.logger.info('Data read from pickle file for table ' + self.current_table + ' Part-' + str(partitionno) + ' (' + str(len(self.dfdata)) + ' rows read)')
                return True
            else:
                self.dfdata = pd.read_pickle(self.raw_data_output_dir + self.current_table + '.pkl')
                print('\nData read from pickle file for table ' + self.current_table + '(' + str(len(self.dfdata)) + ' rows read)')
                self.logger.info('Data read from pickle file for table ' + self.current_table + '(' + str(len(self.dfdata)) + ' rows read)')
                return True

        except Exception as ex:
            self.logger.error('ERROR at reading data from pickle file for table ' + self.current_table)
            print(self.errstr + 'ERROR at reading data from pickle file for table ' + self.current_table)
            print(ex)
            return False


    def generate_hash(self, text):

        """ Generating SHA256 hash value for the text by using the salt value """
        try:
            #hashvalue = hashlib.sha256(str.encode(text + self.hash_salt)).hexdigest()
            hashvalue = hashlib.sha256(str.encode(text) + self.hash_salt).hexdigest()
            return str(hashvalue)
        except Exception as ex:
            print(self.errstr + 'ERROR at hashing value ' + ex)
            return ''


    def scrub_dataframe(self):

        """ Scrubbing the current data frame self.dfdata for the self.current_table """

        try:

            column_names = self.dfdata.columns.tolist()
            column_names = [c.upper() for c in column_names]
            self.dfdata.columns = column_names

            row_count = len(self.dfdata)
            dfchunks = np.array_split(self.dfdata, math.floor(row_count/self.chunksize)+ 1)

            chunkindex = 1
            chunk_list = []
            scrub_columns = []
            self.numeric_columns = []

            disable_tqdm = False
            if len(dfchunks) < 2:
                disable_tqdm=True

            for dfchunk in tqdm(dfchunks, desc='Processing chunks: ', disable=disable_tqdm):

                timebegin = time.time()
                print('\n\n--->> Processing chunk : ' + str(chunkindex) + '/' + str(len(dfchunks)))
                self.logger.info('Processing chunk ' + str(chunkindex) + '/' + str(len(dfchunks)))

                # this value will update the scrub_seek_values file
                max_column_value = 0

                # self.current_table = table_name

                for cname in column_names:

                    self.current_column = cname

                    if self.tablecontext['TYPE'] == 'ID':
                        if cname == self.tablecontext['ID_COLUMN']:
                            tmp_max = dfchunk[self.tablecontext['ID_COLUMN']].astype('int64').max()
                            if tmp_max > max_column_value:
                                max_column_value = tmp_max

                    # if it is in exempt column list do nothing
                    if cname in self.tablecontext['SCRUB_EXEMPT_COLUMNS']:
                        if chunkindex==1:
                            print(self.warnstr + 'EXEMPTED column : ' + cname)
                            self.logger.warning('EXEMPTED column ' + cname)
                        continue

                    if cname in self.tablecontext['FULL_SCRUB_COLUMNS']:
                        dfchunk[cname] = self.scrub_mask_all
                        if chunkindex==1:
                            print('Full scrubbed : ' + cname)
                            self.logger.info('Full scrubbed ' + cname)
                        continue

                    # is being hashed if it is in the hash column list
                    if cname in self.tablecontext['HASH_COLUMNS']:
                        if self.hashids==True:
                            dfchunk[cname] = dfchunk[cname].apply(lambda x: self.generate_hash(str(x))).astype('str')
                            if chunkindex == 1:
                                print('Hash done for column : ' + cname)
                                self.logger.info('Hash done for column ' + cname)
                            continue
                        else:
                            if chunkindex == 1:
                                print(self.warnstr + 'SKIPPED hash column : ' + cname)
                                self.logger.warning('SKIPPED hash column ' + cname)
                            continue
                    if cname in self.tablecontext['HASH_COLUMNS_ALWAYS']:
                           dfchunk[cname] = dfchunk[cname].apply(lambda x: self.generate_hash(str(x))).astype('str')
                           if chunkindex == 1:
                               print('Hash done for column : ' + cname)
                               self.logger.info('Hash done for column ' + cname)
                           continue

                    if self.current_table == 'TEXTER_SURVEY_RESPONSE_VALUE':
                        dfchunk_part1 = dfchunk[dfchunk['QUESTION_ID']==66]
                        dfchunk_part2 = dfchunk[dfchunk['QUESTION_ID']!=66]

                        timebegin2 = time.time()
#                        if self.replace_with_identifiers==True:
#                            dfchunk_part1[cname] = dfchunk_part1[cname].apply(lambda x: self.scrubber.clean(str(x), replace_with='identifier'))
#                        else:
                        dfchunk_part1[cname] = dfchunk_part1[cname].apply(lambda x: self.scrubber.clean(str(x)))

                        timelapsed2 = time.time() - timebegin2
                        print('Scrubadub done : ' + cname + ' in ' + str(round(timelapsed2,2)) + ' sec')
                        self.logger.info('Scrubadub done ' + cname + ' in ' + str(round(timelapsed2,2)) + ' sec')

                        timebegin2 = time.time()
                        dfchunk_part1[cname] = dfchunk_part1[cname].apply(lambda x: self.scrub_second_pass(str(x)))
                        timelapsed2 = time.time() - timebegin2
                        print('Scrub second pass done : ' + cname + ' in ' + str(round(timelapsed2,2)) + ' sec')
                        self.logger.info('Scrub second pass done ' + cname + ' in ' + str(round(timelapsed2,2)) + ' sec')

                        dfchunk = pd.concat([dfchunk_part1, dfchunk_part2], axis=0)

                    else: # for all the other tables

                        timebegin2 = time.time()
#                        if self.replace_with_identifiers==True:
#                            dfchunk[cname] = dfchunk[cname].apply(lambda x: self.scrubber.clean(str(x), replace_with='identifier'))
#                        else:
                        #print("dfchunk[cname]: " + dfchunk[cname] + " name: " + cname)
                        #print("====")
                        #print(self.tablecontext['NUMERIC_DATA_COLUMNS'])
                        #print("====")
                        #dfchunk[cname] = dfchunk[cname].apply(lambda x: self.scrubber.clean(str(x)))

                        timelapsed2 = time.time() - timebegin2
                        print('Scrubadub done : ' + cname + ' in ' + str(round(timelapsed2,2)) + ' sec')
                        self.logger.info('Scrubadub done ' + cname + ' in ' + str(round(timelapsed2,2)) + ' sec')

                        timebegin2 = time.time()
                        dfchunk[cname] = dfchunk[cname].apply(lambda x: self.scrub_second_pass(str(x)))
                        timelapsed2 = time.time() - timebegin2
                        print('Scrub second pass done : ' + cname + ' in ' + str(round(timelapsed2,2)) + ' sec')
                        self.logger.info('Scrub second pass done ' + cname + ' in ' + str(round(timelapsed2,2)) + ' sec')

                    if chunkindex == 1:
                        scrub_columns.append(cname)
                        if is_numeric_dtype(dfchunk[cname]):
                            self.numeric_columns.append(cname)
                            

                # Once the data filtering is done, append the chunk to list
                chunk_list.append(dfchunk)
                chunkindex += 1


            # concat the list into dataframe
            self.dfdata = pd.concat(chunk_list)
            self.dfdata.infer_objects()

            if row_count > 0:

                for colname in scrub_columns:
                    scrubbed_value_count = len(self.dfdata[self.dfdata[colname].isin(self.infotypes)])
                    if scrubbed_value_count / row_count * 100 > self.infotype_percentage and scrubbed_value_count < row_count:
                        self.dfdata[colname] = self.dfdata[colname].apply(lambda x: self.scrub_mask_all if x not in self.infotypes and len(str(x)) > 0 else x)
                    print('Percentage based scrubbing done: ' + colname)
                    self.logger.info('Percentage based scrubbing done: ' + colname)

            timelapsed = time.time() - timebegin
            print()
            print('Elapsed time: ' + str(round(timelapsed,2)) + ' sec')
            print('Completed scrubbing the dataframe for table ' + self.current_table)
            self.logger.info('Completed scrubbing the dataframe for table ' + self.current_table + ' in ' + str(round(timelapsed,2)) + ' sec')

        except Exception as ex:
            self.logger.error('ERROR at scrubbing the the dataframe for table: ' + self.current_table + ', column: ' + self.current_column)
            print(self.errstr + 'ERROR at scrubbing the the dataframe for table: ' + self.current_table + ', column: ' + self.current_column)
            print(ex)


#    def update_seek_value(self, table_name, id_column_name, column_new_value):
    def update_seek_value(self, column_new_value):

        """ Updating the last column value for the ID column of the current table """

        try:
            idvalues_str = open(self.home_dir + 'config/scrub_seek_values.txt', 'r').read()
            idvalues = eval(idvalues_str)
            idvalues[self.current_table + '.' + self.tablecontext['ID_COLUMN']] = column_new_value

            with open(self.home_dir + 'config/scrub_seek_values.txt', 'w') as f:
                f.write(str(idvalues))

            self.logger.info('Seek value updated for ' + self.current_table)

        except Exception as ex:
            self.logger.error('Error at updating seek value for ' + self.current_table)
            return None


    def scrub_second_pass(self, text):

        """ Second pass scrubbing the column value """

        try:

            retval = str(text)

            if retval.startswith('aes256'):
                return retval

            if retval is None:
                return ''

            if len(retval) == 0:
                return

            if retval in self.infotypes:
                return retval

            if len(retval) == 0:
                return retval

            # especially for the survey_response_value table if there is any double quote before or after the word causes mis-matching.
            if self.current_table == 'SURVEY_VALUE':
                retval = retval.replace('\"', ' \" ')

            # replacing phone numbers
#            retval = re.sub(self.regex_phone1, self.scrub_mask_phone, retval)
#            retval = re.sub(self.regex_phone3, self.scrub_mask_phone, retval)

            # now splitting the words for further
            words = str(retval).split()
            word_count = len(words)

            if word_count > 0:
                for i in range(word_count):

                    # if it is in exclude words then do nothing to the word
                    if self.enable_exclude_words ==True:
                        if words[i].upper() in self.exclude_words:
                            continue

                    if self.enable_exclude_emails == True:
                        if words[i].upper() in self.exclude_emails:
                            continue

                    if self.enable_exclude_urls == True:
                        if words[i].upper() in self.exclude_urls:
                            continue

                    if self.enable_exclude_phones == True:
                        if words[i] in self.exclude_phones:
                            continue

                    # closed because of regex_phone1 and regex_phone2 is combined to regex_phone3
                    # # replacing phone numbers
                    # if self.enable_phone_scrubbing2 == True:
                    #     if re.match(self.regex_phone2, words[i]) is not None:
                    #         if self.display_scrubbed_text:
                    #             words[i] = self.scrub_mask_phone + '(' + words[i] + ')'
                    #         else:
                    #             words[i] = self.scrub_mask_phone


                    # replacing emails
                    if self.enable_email_scrubbing2 == True:
                        if self.current_column not in self.numeric_columns:
                            if re.match(self.regex_email, words[i]) is not None:
                                if self.display_scrubbed_text:
                                    words[i] = self.scrub_mask_email + '(' + words[i] + ')'
                                else:
                                    words[i] = self.scrub_mask_email


                    # replacing urls
                    if self.enable_url_scrubbing2 == True:
                        if self.current_column not in self.numeric_columns:
                            if 'HTTP' in words[i].upper() or 'WWW.' in words[i].upper():
                                if self.display_scrubbed_text:
                                    words[i] = self.scrub_mask_url + '(' + words[i] + ')'
                                else:
                                    words[i] = self.scrub_mask_url


                    # replacing first names
                    if self.enable_name_scrubbing2 == True:
                        if self.current_column not in self.numeric_columns:
                            # if words[i].upper() in self.firstnames_dic:
                            #     if self.display_scrubbed_text:
                            #         words[i] = self.scrub_mask_name + '(' + raw_word + ')'
                            #     else:
                            #         words[i] = self.scrub_mask_name

                            if words[i].find('(') != -1 or words[i].find(')') != -1 or words[i].find('[') != -1 or words[i].find(']') != -1 or words[i].find('!') != -1:
                                raw_word = words[i].replace('(', '').replace(')', '').replace('[', '').replace(']','').replace('!','')
                            else:
                                raw_word = words[i]

                            # handling hyphen case
                            replaced = False
                            raw_word_parts = raw_word.split('-')
                            part_count = len(raw_word_parts)
                            for r in range(part_count):
                                if raw_word_parts[r].upper() not in self.exclude_words:
                                    if raw_word_parts[r].upper() in self.firstnames_dic:
                                        raw_word_parts[r] = self.scrub_mask_name
                                        replaced = True

                            raw_word = '-'.join(raw_word_parts)
                            if self.display_scrubbed_text and replaced==True:
                                words[i] = raw_word + '(' + words[i] + ')'
                            # else:
                            #     words[i] = words[i]

                            # handling aposthrophe case
                            replaced = False
                            word_parts = words[i].split("'")
                            part_count = len(word_parts)
                            for r in range(part_count):
                                if word_parts[r].upper() not in self.exclude_words:
                                    if word_parts[r].upper() in self.firstnames_dic:
                                        word_parts[r] = self.scrub_mask_name
                                        replaced = True

                            new_word = "'".join(word_parts)
                            if self.display_scrubbed_text and replaced==True:
                                words[i] = new_word + '(' + words[i] + ')'
                            # else:
                            #     words[i] = words[i]


                retval = ' '.join(words)

                # Phone number scrubbing with regex_phone3 (initially it was regex_phone3)
                if self.enable_phone_scrubbing2==True:
                    phones = []
#                    for match in re.finditer(self.regex_phone1, retval):
                    for match in re.finditer(self.regex_phone3, retval):
                        matchpos = match.span()
                        phones.append(retval[matchpos[0]:matchpos[1]])

                    for p in phones:
                        if self.display_scrubbed_text:
                            retval = retval.replace(p, self.scrub_mask_phone + '(' + p + ')')
                        else:
                            retval = retval.replace(p, self.scrub_mask_phone)


                # Postcode scrubbing
                if self.enable_postcode_scrubbing2 == True:

                    if not retval.startswith('http'):

                        postcodes = []
                        for match in re.finditer(self.regex_postcode, retval):
                            matchpos = match.span()

                            # TODO: Add additinal checks against EMOJI strings
                            postcodes.append(retval[matchpos[0]:matchpos[1]])

                        for p in postcodes:

                            if self.display_scrubbed_text:
                                retval = retval.replace(p, self.scrub_mask_postcode + '(' + p + ')')
                            else:
                                retval = retval.replace(p, self.scrub_mask_postcode)



                # national insurance number scrubbing
                if self.enable_nino_scrubbing2 == True:

                    ninos = []
                    for match in re.finditer(self.regex_nino, retval):
                        matchpos = match.span()
                        ninos.append(retval[matchpos[0]:matchpos[1]])

                    for p in ninos:
                        if self.display_scrubbed_text:
                            retval = retval.replace(p, self.scrub_mask_nino + '(' + p + ')')
                        else:
                            retval = retval.replace(p, self.scrub_mask_nino)



                # doing street scrubbing only for selected columns
                if self.enable_street_scrubbing2==True and self.current_table+'.'+self.current_column in self.street_columns:

                    streetfound = False
                    words = retval.upper().split(' ')
                    indicators = ['ROAD', 'LANE', 'AVENUE', 'STREET']
                    indexes = [ix for ix in range(len(words)) if words[ix] in indicators]

                    if len(indexes) > 0:
                        for i in indexes:
                            if i == 1:
                                if ' '.join([words[i-1].upper(), words[i].upper()]) in self.street_names:
                                    streetfound = True
                                    if self.display_scrubbed_text==True:
                                        words[i-1] = self.scrub_mask_street + '(' + words[i-1] + ')'
                                    else:
                                        words[i-1] = self.scrub_mask_street
                            elif i > 1:
                                # check for 2 words before first
                                if ' '.join([words[i-2].upper(), words[i-1].upper(), words[i].upper()]) in self.street_names:
                                    streetfound = True
                                    if self.display_scrubbed_text==True:
                                        words[i-2] = self.scrub_mask_street
                                        words[i-1] = self.scrub_mask_street + '(' + words[i-1] + ')'
                                    else:
                                        words[i-2] = self.scrub_mask_street
                                        words[i-1] = self.scrub_mask_street

                                elif ' '.join([words[i-1].upper(), words[i].upper()]) in self.street_names:
                                    streetfound = True
                                    if self.display_scrubbed_text==True:
                                        words[i-1] = self.scrub_mask_street + '(' + words[i-1] + ')'
                                    else:
                                        words[i-1] = self.scrub_mask_street

                    if streetfound:
                        retval = ' '.join(words)

            return retval
        except Exception as ex:
            self.logger.error('ERROR at second pass scrub for value: ' + str(text))
            print(self.errstr + 'ERROR at second pass scrub for value : ' + str(text))
            print(ex)


    def export_dataframe_tocsv(self, partitionno=None):

        """ Exporting dataframe data of the current table into the csv file """


                
        try:
            # this is to prevent integers being written as floats. The dataframe insists the column data type is object due to some missing elements.
            for col in self.tablecontext['NUMERIC_DATA_COLUMNS']:
                col_string = self.dfdata[col].astype('str')
                col_series = pd.Series(col_string)
                #print("Is numeric data column: [" + col + "] first valid index val: [" + col_series[col_series.first_valid_index()] + "]")
                if '.0' in col_series[col_series.first_valid_index()] or col_series[col_series.first_valid_index()] == '':
                   print("Is numeric data column: [" + col + "] removing last two digits")
                   self.dfdata[col] = col_string.str[:-2]

            if partitionno is not None:
                self.dfdata.to_csv(self.output_dir + self.current_table + '/' + self.current_table + '_PART_' + str(partitionno) + '.tsv', index=False, sep='\t')
                #self.dfdata.to_csv(self.output_dir +  self.current_table + '_PART_' + str(partitionno) + '.tsv', index=False, sep='\t')
                print('Dataframe to csv exported for table ' + self.current_table + ' Part-' + str(partitionno))
                self.logger.info('Dataframe to csv exported for table ' + self.current_table + ' Part-' + str(partitionno))

            else:
                self.dfdata.to_csv(self.output_dir + self.current_table + '/' + self.current_table + '.tsv', index=False, sep='\t')
                #self.dfdata.to_csv(self.output_dir + self.current_table + '.tsv', index=False, sep='\t')
                self.logger.info('Dataframe to csv exported for table ' + self.current_table)

            # flushing the memory for dataframe
                self.dfdata = pd.DataFrame()

        except Exception as ex:
            self.logger.error('ERROR at exporting the dataframe to csv for table ' + self.current_table)
            print(self.errstr + 'ERROR at exporting the dataframe to csv for table ' + self.current_table)
            print(ex)

    def export_dataframe_toparquet(self, partitionno=None):

        """ Exporting dataframe data of the current table into the parquet file """

        try:
            output_directory = self.output_dir + 'parquet/' + self.current_table + '/'
            if partitionno is not None:
                self.dfdata.to_parquet(output_directory + self.current_table + '_PART_' + str(partitionno),compression=None)
                #pd.write_to_dataset(self.dfdata, root_path='\"'+self.output_dir + self.current_table + '_PART_' + str(partitionno) + '.parquet'+ '\"')
                print('Dataframe to parquet exported for table ' + self.current_table + ' Part-' + str(partitionno))
                self.logger.info('Dataframe to parquet exported for table ' + self.current_table + ' Part-' + str(partitionno))

            else:
                self.dfdata.to_parquet(output_directory + self.current_table,compression=None)
                #pq.write_to_dataset(self.dfdata, root_path='\"'+self.output_dir + self.current_table + '.parquet'+'\"')
                print('Dataframe to parquet exported for table ' + self.current_table)
                self.logger.info('Dataframe to parquet exported for table ' + self.current_table)

            # flushing the memory for dataframe
            self.dfdata = pd.DataFrame()

        except Exception as ex:
            self.logger.error('ERROR at exporting the dataframe to parquet for table ' + self.current_table)
            print(self.errstr + 'ERROR at exporting the dataframe to parquet for table ' + self.current_table)
            print(ex)            


    def init_loggers(self):

        # """ Initializing loggers """
        try:
#            log_format = '{time : %(asctime)s , type: %(levelname)s - %(message)s}'
            log_format = "{time : '%(asctime)s' , type : '%(levelname)s' , message : '%(message)s'},"
            time_format = '%Y-%m-%d %H:%M:%S'

#            logging.basicConfig(filename='/var/log/mhi/datapipeline.log', filemode='a', format=log_format, datefmt=time_format, level=logging.INFO)

            logging.basicConfig(filename=self.logfile_path, filemode='a', format=log_format, datefmt=time_format, level=logging.INFO)
            self.logger = logging.getLogger('Pipeline')

            self.logger.info('Platform data export and anonymisation started.')
        except Exception as ex:
             print(ex)




###############################################################

if __name__ == "__main__":

    # creating DBScrubber instance
    dbs = DBScrubber()

    # running the export and anonymising process
    dbs.run_process()


