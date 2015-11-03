# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 11:08:29 2015

@author: Ricardo Guerrero
"""

import itertools
import os
import pandas as pd
import sys
import tempfile
import time
import tweets_cleaned as tw_cl

#Parameters

BATCH_SIZE = 2
PATTERN = ' (timestamp: '
WINDOW_SIZE = 600000
TIME_TRESHOLD = 55


def getHashtags(text):
    '''
    Get all the hashtags in a string

    :param cad:
    :return: a list of hashtags (# is removed)
    '''


    #Note: Checked in twitter that at least one space before the hashtag and one after is required to perform a valid hashtag
    hashtags = [word[1:].lower() for word in text.split() if word.startswith('#')]

    return list(set(hashtags)) #Remove repeated hashtags


def getEdges(listSrc):
    '''

    :param cad:
    :return:
    '''

    aux = []

    for subset in itertools.combinations(listSrc, 2):
        aux.append(subset)

    listDst = list(set(aux))
    listDst.sort()

    return listDst


def computeDegree():
    '''

    :param cad:
    :return:
    '''

    #As we cannot spend more than 60 seconds processing tweets that will arrive every 60 seconds,
    #we need to take care of the time consumed.
    starting_time = time.time()


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
        f = open(path_2_output, 'a')
        f.close()
    except IOError:
        print("The given output file does not exist or can't be created\n")
        sys.exit()

    #Cleaning input tweets
    '''
    temporal_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    temporal_file.close()
    tw_cl.clean(path_2_input, temporal_file.name)

    path_2_input = temporal_file.name
    '''
    cleaned_tweets = []
    unprocessed_tweets = []
    end_reached = False
    average_degree = 0
    full_nodes = []
    full_edges = []
    old_batch = pd.DataFrame()

    with open(path_2_input, 'r') as f_input, open(path_2_output, 'a') as f_output:

        #Processing input
        #################

        while not end_reached:

            del unprocessed_tweets[:]
            del cleaned_tweets[:]

            #Read a batch

            ind = 0

            while (not end_reached) and (ind < BATCH_SIZE):

                aux = f_input.readline()

                if not aux or aux == '\n':
                    end_reached = True
                else:
                    unprocessed_tweets.append(aux)
                    ind += 1

            if len(unprocessed_tweets):
                #Clean a batch

                #Pre-process tweets so we have them cleaned and organized in a Pandas' DataFrame
                splitted = [line.split(PATTERN) for line in unprocessed_tweets]
                text = [elem[0] for elem in splitted]
                timestamp = [elem[1][:-2] for elem in splitted]

                #Creating dataframe
                empty_l2 = [[] for a in range(0,len(text))]
                data = {'text': text, 'timestamp': timestamp ,
                        'hashtags': empty_l2, 'edges': empty_l2}
                df = pd.DataFrame(data = data)

                #Converting timestamp column to an appropriate format to work with
                df['timestamp'] = pd.to_datetime(df['timestamp'])

                #Concatenate relevant rows of the previous batch
                df = pd.concat([old_batch, df], ignore_index=True)
                begin = len(old_batch)

                for index, newest_tweet in df.iterrows():

                    if index >= begin:
                        #Get tweets within the time window
                        newest_timestamp = newest_tweet['timestamp']
                        oldest_valid_timestamp = newest_timestamp-pd.DateOffset(seconds=WINDOW_SIZE)
                        windowed_tweets_df = df[ (df['timestamp'] >= oldest_valid_timestamp)  &  (df.index <= index) ]

                        #Extract hashtags
                        windowed_tweets_df['hashtags'][begin:] = windowed_tweets_df['text'][begin:].apply(getHashtags)

                        #We can only use those tweets that have at least 2 hashtags
                        length = lambda x: len(x)
                        valid_tweets_df = windowed_tweets_df[ windowed_tweets_df['hashtags'].apply(length) >= 2 ]

                        #Continue only if we have something to process
                        if len(valid_tweets_df):

                            #Get nodes
                            flat_hashtags = [item for sublist in valid_tweets_df['hashtags'] for item in sublist]
                            nodes = list(set(flat_hashtags)) #Remove repeated elements
                            nodes.sort()

                            #Now we are converting a list of hashtags (nodes) in a list of tuples (edges)
                            valid_tweets_df['edges'][begin:] = valid_tweets_df['hashtags'][begin:].apply(getEdges)

                            #Because the same edges can appear in different tweets, we need to remove repeated edges
                            flat_edges = [item for sublist in valid_tweets_df['edges'] for item in sublist]
                            unique_edges = list(set(flat_edges)) #Remove repeated elements
                            unique_edges.sort()

                            #Initialize dictionary of nodes
                            nodes_degree = {}

                            for elem in nodes:
                                nodes_degree[elem] = 0

                            #Compute node's degree
                            for node in nodes:
                                for s_tuple in unique_edges:
                                    if node in s_tuple:
                                        nodes_degree[node] +=1

                            average_degree = sum(nodes_degree.values())/float(len(nodes))

                        else:
                            average_degree = 0


                        #Processing output
                        ##################

                        f_output.write("%.2f\n" % average_degree)

                #When processing the first tweets of the new batch, we need to take into account some relevant tweets of the previous batch
                old_batch = valid_tweets_df.copy()

            #Are we entering the red zone?

            current_time = time.time()
            if current_time - starting_time > TIME_TRESHOLD:

                #Ignore the rest of the tweets and left some margin to process already read tweets
                end_reached = True








if __name__ == '__main__':
    computeDegree()