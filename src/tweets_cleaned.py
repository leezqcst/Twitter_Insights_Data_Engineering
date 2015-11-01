# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 11:08:29 2015

@author: Ricardo Guerrero
"""


import json
import os
import pandas as pd
import re
import sys

#Parameters
BATCH_SIZE = 1


unicode_tweets = 0

#Functions
def removeUnicode(cad):
    '''

    '''
    global unicode_tweets

    ascii_cad = cad.encode('ascii','ignore')

    if ascii_cad != cad:
        unicode_tweets += 1

    return ascii_cad

def removeEscape(cad):
    '''

    :param cad:
    :return:
    '''
    pattern = "[^a-zA-Z0-9 #@:,.*/'!?]"
    return re.sub(pattern, '', cad)

def clean():

    global BATCH_SIZE

    path_2_input = sys.argv[1]
    path_2_output = sys.argv[2]

    #Checking the paths given by the user
    if not os.path.isfile(path_2_input):
        print("The given input file does not exist\n")
        sys.exit()

    try:
        f = open(path_2_output, 'w')
        f.close()
    except IOError:
        print("The given output file does not exist or can't be created\n")
        sys.exit()

    cleaned_tweets = []
    unprocessed_tweets = []
    end_reached = False


    with open(path_2_input, 'r') as f_input, open(path_2_output, 'w') as f_output:

        #Processing input
        #################
        while not end_reached:

            del unprocessed_tweets[:]
            del cleaned_tweets[:]

            #Read a batch

            ind = 0

            while (not end_reached) and (ind < BATCH_SIZE):

                aux = f_input.readline()

                if not aux:
                    end_reached = True
                else:
                    unprocessed_tweets.append(aux)
                    ind += 1

            if len(unprocessed_tweets):
                #Clean a batch

                df = pd.DataFrame(json.loads(line) for line in unprocessed_tweets)

                #Remove unwanted columns
                df = df[['text', 'created_at']]

                #Remove unicode characters
                df['text'] = df['text'].apply(removeUnicode)

                #Remove escape characters
                df['text'] = df['text'].apply(removeEscape)

                cleaned_tweets_ds = df['text'] + ' (timestamp: ' + df['created_at'] + ')'
                cleaned_tweets = [x for x in cleaned_tweets_ds]


                #Processing output
                ##################

                #Write cleaned batch
                for tweet in cleaned_tweets:
                    f_output.write(tweet + '\n')

        #Add summary
        f_output.write("\n%d tweets contained unicode." % unicode_tweets)

    print('Full batch processed')


if __name__ == '__main__':
    clean()