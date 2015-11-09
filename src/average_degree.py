# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 11:08:29 2015

@author: Ricardo Guerrero Gómez-Olmedo
"""

import itertools
import os
import pandas as pd
import sys
import tweets_cleaned as tw_cl


def getHashtags(text):
    """
    getHashtags is meant to process input strings that contains a tweet and return a list of valid hashtags.
    If there is more than one instance of the same tweet, it will be counted only one.
    This function is case insensitive. It means that if we have the following tweet:
        #Hadoop #Spark, #spark, and #SPARK
    it will be returned
        [hadoop, spark]

    Note: a valid hashtag (as checked in Twitter) must have at least one space before and one space after to be
    considered a valid hashtag

    Parameters
    ----------
    text : string
        text is a string that contains a tweet (typically with hashtags, mentions, etc)

    Returns
    -------
    anonymous variable : list
        This variable is a list of unique hashtags (# is removed) found in `text`
    """

    hashtags = [word[1:].lower() for word in text.split() if word.startswith('#')]

    # Remove any harmful element that was left behind
    hashtags2 = []

    for elem in hashtags:
        aux = elem[elem.rfind("#")+1 :]

        if aux:
            hashtags2.append(aux)

    return list(set(hashtags2)) # Remove repeated hashtags


def getEdges(listSrc):
    """
    This function gets a list of hashtags (strings) and compute all possible combinations.
    This way, thinking of each hashtag as a vertex in a graph, we can compute all the connections
    between them.

    Parameters
    ----------
    listSrc : list
        This is a list of strings. Each string contains a hashtag.

    Returns
    -------
    listDst : list
        This is a list of tuples with length two, where each tuple represents and edge in the graph.
    """

    aux = []

    for subset in itertools.combinations(listSrc, 2):
        aux.append(subset)

    listDst = list(set(aux))
    listDst.sort()

    return listDst


def computeDegree(batch_size=100):
    """
    It computes the average degree of a vertex in a Twitter hashtag graph in a temporal window that covers
    the last 60 seconds.
    A Twitter hashtag graph is a graph connecting all the hashtags that have been mentioned together in a single tweet.
    This function will get the input values from a file given in argv[1] and will write the output to the file
    specified in argv[2] (command-line parameters).
    The purpose of this function is to obtain a feature that will give us relevant insights about the relationships
    of each hashtag.

    Parameters
    ----------
    batch_size : int
        This variable represents the maximum amount of tweets that will be processed in one go. This parameter
        ease the scalability of the algorithm as it can trade-off between speed and RAM memory. Using a low value
        makes this code usable in low-RAM machines, as it will not load all tweets at the same time. Using an
        enough high value makes that all tweets can be loaded in one go, speeding up the algorithm.
        Default value: 100
    """


    # Checking the paths given by the user
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



    cleaned_tweets = []
    unprocessed_tweets = []
    end_reached = False
    old_batch = pd.DataFrame()
    window_size = 60

    with open(path_2_input, 'r') as f_input, open(path_2_output, 'a') as f_output:

        #Processing input
        #################

        while not end_reached:

            del unprocessed_tweets[:]
            del cleaned_tweets[:]

            # Read a batch
            ind = 0

            while (not end_reached) and (ind < batch_size):

                aux = f_input.readline()

                if not aux or aux == '\n':
                    end_reached = True
                else:
                    unprocessed_tweets.append(aux)
                    ind += 1

            if len(unprocessed_tweets):

                # Pre-process input tweets so we have them cleaned and organized in a Pandas' DataFrame
                cleaned_df = tw_cl.clean(unprocessed_tweets)

                if len(cleaned_df):

                    # Creating DataFrame
                    empty_l2 = [[] for a in range(0,len(cleaned_df))]
                    data = {'hashtags': empty_l2, 'edges': empty_l2}
                    df = pd.DataFrame(data=data)
                    df['text'] = cleaned_df['text']
                    df['timestamp'] = pd.to_datetime(cleaned_df['created_at'])


                    # Concatenate relevant rows of the previous batch
                    df = pd.concat([old_batch, df], ignore_index=True)
                    begin = len(old_batch)

                    for index, newest_tweet in df.iterrows():

                        if index >= begin:

                            # Get tweets within the time window
                            newest_timestamp = newest_tweet['timestamp']
                            oldest_valid_timestamp = newest_timestamp-pd.DateOffset(seconds=window_size)
                            windowed_tweets_df = df[ (df['timestamp'] >= oldest_valid_timestamp)  &  (df.index <= index) ]

                            # Extract hashtags
                            if index in windowed_tweets_df.index: # Some tweets were removed because of non allowed values, so we need to check this
                                aux_hashtags = getHashtags(windowed_tweets_df.loc[index, 'text'])
                                windowed_tweets_df.set_value(index, 'hashtags', aux_hashtags)
                                df.set_value(index, 'hashtags', aux_hashtags) # Done in this way as a workaoround to SettingWithCopyWarning issue

                            # We can only use those tweets that have at least 2 hashtags
                            length = lambda x: len(x)
                            valid_tweets_df = windowed_tweets_df[ windowed_tweets_df['hashtags'].apply(length) >= 2 ]

                            # Continue only if we have something to process
                            if len(valid_tweets_df):

                                # Get nodes
                                flat_hashtags = [item for sublist in valid_tweets_df['hashtags'] for item in sublist]
                                nodes = list(set(flat_hashtags)) # Remove repeated elements
                                nodes.sort()

                                # Converting a list of hashtags (nodes) in a list of tuples (edges)
                                if index in valid_tweets_df.index: # Maybe the row pointed by index had only one hashtag
                                    aux_edges = getEdges(valid_tweets_df.loc[index, 'hashtags'])
                                    valid_tweets_df.set_value(index, 'edges', aux_edges)
                                    df.set_value(index, 'edges', aux_edges) # Done in this way as a workaoround to SettingWithCopyWarning issue

                                # Because the same edges can appear in different tweets, we need to remove repeated edges
                                flat_edges = [item for sublist in valid_tweets_df['edges'] for item in sublist]
                                unique_edges = list(set(flat_edges)) # Remove repeated elements
                                unique_edges.sort()

                                # Initialize dictionary of nodes
                                nodes_degree = {}

                                for elem in nodes:
                                    nodes_degree[elem] = 0

                                # Compute node's degree
                                for node in nodes:
                                    for s_tuple in unique_edges:
                                        if node in s_tuple:
                                            nodes_degree[node] += 1

                                average_degree = sum(nodes_degree.values())/float(len(nodes))

                            else:
                                average_degree = 0


                            #Processing output
                            ##################

                            f_output.write("%.2f\n" % average_degree)

                    # When processing the first tweets of the new batch, we need to take into
                    # account some relevant tweets of the previous batch
                    old_batch = valid_tweets_df.copy()


        print('Full batch processed')


if __name__ == '__main__':
    computeDegree()