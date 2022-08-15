# import libraries block

import os
import collections
import re
import pandas as pd
import time
import editdistance

from nltk.corpus import stopwords
import nltk

# import ssl
# try:
#     _create_unverified_https_context = ssl._create_unverified_context
# except AttributeError:
#     pass
# else:
#     ssl._create_default_https_context = _create_unverified_https_context
#
# nltk.download('punkt')
# nltk.download('stopwords')


# class name declaration
class TextWordCounter():


    def __init__(self):
        # constructor method of the class, initialize the global data structures and variables.
        self.english_words = []
        self.remove_wordsd = []

        with open(os.path.join('resources', 'english_words.txt') ) as fp:
            self.english_words = fp.readlines()
            self.english_words = [w.replace('\n','').upper() for w in self.english_words if len(w) > 1]


    def preprocess_text(self, textstr):

        # a method(actually a function) that preprocess on the given string for data cleansing purposes.

        retval = str(textstr)
        retval = retval.upper()
        retval = retval.replace(',', ' ')
        retval = retval.replace('.', ' ')
        retval = retval.replace(';', ' ')
        retval = retval.replace(':', ' ')
        retval = retval.replace('?', ' ')
        retval = retval.replace('!', ' ')
        retval = retval.replace('-', ' ')
        retval = retval.replace('=', ' ')
        retval = retval.replace('(', ' ')
        retval = retval.replace(')', ' ')
        retval = retval.replace('\\', ' ')
        retval = retval.replace('/', ' ')
        retval = retval.replace('"', '')
        retval = retval.replace('\n', ' ')
        retval = re.sub('\s+', ' ', retval).strip()
        return retval



        # main method of the class that calculates the word frequencies less than max_frequency

        stop_words = set(stopwords.words('english'))
        stop_words = [w.upper() for w in stop_words ]

        for filename in os.listdir(indirectory):

            indirectory = 'platformdata'
            infilename = filename

            outdirectory = 'word_frequencies_out'
            outfilename = 'LOW_FREQUENT_WORDS.txt'

          # read from files.list
            infilepath = os.path.join(indirectory, infilename)
            outfilepath = os.path.join(outdirectory, outfilename)

            freq = collections.Counter()

            print(infilepath)
            df = pd.read_csv(infilepath, delimiter='\t', low_memory=False, error_bad_lines=False)
            print('len of the data ' + str(len(df)))

            linecount = 1
            for index, row in df.iterrows():

                line = str(row[4])

                if line.startswith('aes256'):
                    continue

                line = self.preprocess_text(line)
                freq.update(line.split())

                linecount += 1
                if linecount %1000 == 0:
                    print('processing row number : ' + str(linecount))


            freq_tmp = {}
            print('length of the freq :' + str(len(freq)))

            for key, value in freq.items():
                if value <= max_frequency:
                    if key not in self.english_words:

                        ing_items = [w for w in self.english_words if w + 'ING' == key or key + 'ING' == w]

                        if exclude_typos == True:
                            typo_items = [w for w in self.english_words if editdistance.distance(w, key) == 1]

                            if len(typo_items) == 0 and len(ing_items) == 0:
                                freq_tmp[key] = value
                        else:
                            if len(ing_items) == 0:
                                freq_tmp[key] = value

            freq = freq_tmp
            print('length of the freq after cleansing:' + str(len(freq)))

            with open(outfilepath, 'w') as fpout:
                for key, value in freq.items():
                    fpout.write(key + '\n')


    def replace_words(self, txt):
        # This method replaces all occurrences of the words that have frequency less than 4 by the string [scrubbed].

        items = str(txt).replace('\n', ' ').split()
        f = lambda x: '{{SCRUBBED}}' if x.upper() in self.remove_wordsd else x
        items = [f(x) for x in items]
        retval = ' '.join(items)
        return retval


    def remove_low_frequencies(self, indirectory, infile):
        # This method read the file infile in the given directory parameter indirectory and replaces all occurrences of the low frequency words.

        with open(os.path.join('word_frequencies_out', 'LOW_FREQUENT_WORDS.txt')) as fp:
            for line in fp:
                self.remove_wordsd[line.replace('\n', '')] = 1

        df = pd.read_csv(os.path.join(indirectory, infile), delimiter='\t', low_memory=False)
        timebegin = time.time()
        rowcount = df.shape[0]

        with open(os.path.join('word_frequencies_out', 'SCRUBBED_MESSAGES_' + infile), 'w') as fp:
            for ix, row  in df.iterrows():

                # eliminating some of the automatically generated messages by the system
                if str(row[4]).startswith('Welcome to Crisis Text Line UK.') == False:
                    row[4] = self.replace_words(row[4])
                fp.write('\t'.join(row.values.astype(str).tolist()) + '\n')

                if ix % 10000 == 0:
                    print('Processing record ' + str(ix))
                    timelapsed = time.time() - timebegin
                    print('Elapsed time : ' + str(timelapsed))
                    print('Remaining time : ' + str(timelapsed * (rowcount - ix) / (ix+1) ))


    def prepare_5000(self):

        # this methods creates a sample of 5000 records from the text messages file

        df = pd.read_csv(os.path.join('platformdata', 'messages'), delimiter='\t', low_memory=False, error_bad_lines=False)
        df2 = df.sample(n=5000)
        df2.to_csv(os.path.join('word_frequencies_out', 'SAMPLE_5000_MESSAGES.tsv'), index=False)


    # added again
    def calculate_word_frequency(self, indirectory, max_frequency=3, exclude_typos=True):
        # main method of the class that calculates the word frequencies less than max_frequency

        stop_words = set(stopwords.words('english'))
        stop_words = [w.upper() for w in stop_words ]

        for filename in os.listdir(indirectory):

            indirectory = 'platform data'
            infilename = filename

            outdirectory = 'word_frequencies_out'
            outfilename = 'LOW_FREQUENT_WORDS.txt'

            infilepath = os.path.join(indirectory, infilename)
            outfilepath = os.path.join(outdirectory, outfilename)

            freq = collections.Counter()

            df = pd.read_csv(infilepath, delimiter='\t', low_memory=False)
            print('len of the data ' + str(len(df)))

            linecount = 1
            for index, row in df.iterrows():

                line = str(row[4])

                if line.startswith('aes256'):
                    continue

                line = self.preprocess_text(line)
                freq.update(line.split())

                linecount += 1
                if linecount %1000 == 0:
                    print('processing row number : ' + str(linecount))


            freq_tmp = {}
            print('length of the freq :' + str(len(freq)))

            for key, value in freq.items():
                if value <= max_frequency:
                    if key not in self.english_words:

                        ing_items = [w for w in self.english_words if w + 'ING' == key or key + 'ING' == w]

                        if exclude_typos == True:
                            typo_items = [w for w in self.english_words if editdistance.distance(w, key) == 1]

                            if len(typo_items) == 0 and len(ing_items) == 0:
                                freq_tmp[key] = value
                        else:
                            if len(ing_items) == 0:
                                freq_tmp[key] = value

            freq = freq_tmp
            print('length of the freq after cleansing:' + str(len(freq)))

            with open(outfilepath, 'w') as fpout:
                for key, value in freq.items():
                    fpout.write(key + '\n')




if __name__ == "__main__":

    # this is how to instantiate the class and call the steps in sequence.
    # The file contains this class can directly be executed from the Python terminal.

    twc = TextWordCounter()
    print('instantiated object')
    twc.calculate_word_frequency(indirectory='platformdata')
    print('calculated word freq')
    twc.prepare_5000()
    print('prepare 5000')
    twc.remove_low_frequencies(indirectory='platformdata', infile='messages')
    print('removed low freq') 


