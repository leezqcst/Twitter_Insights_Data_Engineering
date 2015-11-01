# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 11:08:29 2015

@author: Ricardo Guerrero
"""

import os
import pandas as pd
import sys

#Parameters
BATCH_SIZE = 3
PATTERN = ' (timestamp: '
WINDOW_SIZE = 60

def getHashtags(text):
    '''
    Get all the hashtags in a string

    :param cad:
    :return: a list of hashtags (# is removed)
    '''


    #Note: Checked in twitter that at least one space before the hashtag and one after is required to perform a valid hashtag
    return [word[1:].lower() for word in text.split() if word.startswith('#')]


def computeDegree():
    '''

    :param cad:
    :return:
    '''

    #Checking the paths given by the user
    try:
        path_2_input = sys.argv[1]
        path_2_output = sys.argv[2]
    except IndexError:
        print("The number of parameters given was insufficient. You need to provide at least 2: the input file and the output file")

    if not os.path.isfile(path_2_input):
        print("The given input file does not exist\n")
        sys.exit()

    try:
        f = open(path_2_output, 'w')
        f.close()
    except IOError:
        print("The given output file does not exist or can't be created\n")
        sys.exit()

    #TODO aquí habrá que iniciar el hilo que nos avisa de los 60 s
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

                #TODO tenemos que eliminar la ultima linea, la del
                # X tweets contained unicode.

                if not aux:
                    end_reached = True
                else:
                    unprocessed_tweets.append(aux)
                    ind += 1

            if len(unprocessed_tweets):
                #Clean a batch

                #Pre-process tweets so we have them cleaned and organized in a Pandas' dataframe
                splitted = [line.split(PATTERN) for line in unprocessed_tweets]
                text = [elem[0] for elem in splitted]
                timestamp = [elem[1][:-2] for elem in splitted]

                #Creating dataframe
                data = {'text': text, 'timestamp': timestamp }
                df = pd.DataFrame(data = data)

                #Converting timestamp column to an appropriate format to work with
                df['timestamp'] = pd.to_datetime(df['timestamp'])

                #TODO hacerlo aqui, porque sino sería procesar a lo tonto, a lo mejor hay hashtags que vamos a tirar
                df['hashtags'] = df['text'].apply(getHashtags)

                #We can only use those tweets that have at least 2 hashtags
                length = lambda x: len(x)
                valid_tweets_df = df[ df['hashtags'].apply(length) >= 2 ]

                #Continue only if we have something to process
                if len(valid_tweets_df):

                    #Get tweets within the time window
                    newest_timestamp = max(valid_tweets_df['timestamp'])
                    oldest_valid_timestamp = newest_timestamp-pd.DateOffset(seconds=WINDOW_SIZE)
                    windowed_tweets_df = valid_tweets_df[ valid_tweets_df['timestamp'] >= oldest_valid_timestamp]




                    ''' util para convertir a lista cuando haya acabado el procesado
                    cleaned_tweets_ds = df['text'] + ' (timestamp: ' + df['created_at'] + ')'
                    cleaned_tweets = [x for x in cleaned_tweets_ds]
                    '''

                #Processing output
                ##################

                #Write cleaned batch
                for tweet in cleaned_tweets:
                    f_output.write(tweet + '\n')




    '''
    interesting_tweets_df = interesting_tweets_df[['hashtags', 'created_at']]
    '''
    #Get newest timestamp
    #Get those tweets in a 60 seconds window

    '''
    cad = 'Spark Summit East this week! #Spark #Apache (timestamp: Thu Oct 29 17:51:01 +0000 2015)'


    a = ['#Spark', '#Hadoop']
    b = ['#Spark', '#Hadoop', '#spark','#Spark', '#Hadoop', '#spark']

    print getHashtags(a)
    print getHashtags(b)

    hashtags = getHashtags(cad)

    '''

if __name__ == '__main__':
    computeDegree()