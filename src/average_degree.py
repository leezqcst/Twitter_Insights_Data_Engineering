# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 11:08:29 2015

@author: Ricardo Guerrero
"""

import itertools
import os
import pandas as pd
import sys

#Parameters
BATCH_SIZE = 5
PATTERN = ' (timestamp: '
WINDOW_SIZE = 10000 #TODO ajustar a 100


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
        f = open(path_2_output, 'r')
        f.close()
    except IOError:
        print("The given output file does not exist or can't be created\n")
        sys.exit()

    #TODO aquí habrá que iniciar el hilo que nos avisa de los 60 s
    cleaned_tweets = []
    unprocessed_tweets = []
    end_reached = False



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
                data = {'text': text, 'timestamp': timestamp , 'hashtags': [[] for a in range(0,len(text))] }
                df = pd.DataFrame(data = data)

                #Converting timestamp column to an appropriate format to work with
                df['timestamp'] = pd.to_datetime(df['timestamp'])

                #Get tweets within the time window
                newest_timestamp = max(df['timestamp'])
                oldest_valid_timestamp = newest_timestamp-pd.DateOffset(seconds=WINDOW_SIZE)
                windowed_tweets_df = df[ df['timestamp'] >= oldest_valid_timestamp]

                #Extract hashtags
                windowed_tweets_df['hashtags'] = windowed_tweets_df['text'].apply(getHashtags)

                #We can only use those tweets that have at least 2 hashtags
                length = lambda x: len(x)
                valid_tweets_df = windowed_tweets_df[ windowed_tweets_df['hashtags'].apply(length) >= 2 ]

                #Continue only if we have something to process
                if len(valid_tweets_df):

                    #Get nodes
                    flat_hashtags = [item for sublist in valid_tweets_df['hashtags'] for item in sublist]
                    nodes = list(set(flat_hashtags))
                    nodes.sort()

                    #Now we are converting a list of hashtags (nodes) in a list of tuples (edges)
                    valid_tweets_df.loc[:, 'edges'] = valid_tweets_df['hashtags'].apply(getEdges)

                    #Because the same edges can appear in different tweets, we need to remove repeated edges
                    flat_edges = [item for sublist in valid_tweets_df['edges'] for item in sublist]
                    unique_edges = list(set(flat_edges))
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

        #Processing output
        ##################

        f_output.write("%.2f" % average_degree)







if __name__ == '__main__':
    computeDegree()