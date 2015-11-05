# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 11:08:29 2015

@author: Ricardo Guerrero Gómez-Olmedo
"""

import itertools
import json
import os
import pandas as pd
import re
import sys


# Global variable
unicode_tweets = 0


def removeUnicode(cad):
    """
    This function take cares of removing unwanted unicode characters.
    It does not replace them but just remove.

    If cad has any unicode character, unicode_tweets will be incremented
    (once per string, not per character).

    Parameters
    ----------
    cad : string
        Input string with potentially unwanted unicode characters.

    Returns
    -------
    ascii_cad : string
        Same as cad but with only allowed ascii characters
    """

    global unicode_tweets

    ascii_cad = cad.encode('ascii', 'ignore')

    if ascii_cad != cad:
        unicode_tweets += 1

    return ascii_cad


def replaceEscape(cad):
    """
    This function will replace each scape character and multiple whitespaces the following way:

        \/ -> /
        \\ -> \
        \' -> '
        \” -> “
        \n -> space
        \t -> space

        Also, all whitespace escape characters should be replaced with a single space.

    as it is stated in the FAQ of the challenge.

    Parameters
    ----------
    cad : string
        Input string ascii-encoded with potentially unwanted escape characters.

    Returns
    -------
    cad : string
        Same as cad but without any possible escape characters.
    """

    escape_ch = [("\/", "/"), ("\\\\", "\\"), (r"\'", "'"), (r'\"', '"'), ("\n", " "), ("\t", " ")]

    for elem in escape_ch:
        cad = cad.replace(elem[0], elem[1])

    # Removing multiple whitespaces
    re.sub(r"  +", " ", cad)

    return cad


def format_json_tweets(unprocessed_tweets):
    """
    This function accepts a list of json-formatted tweets.
    It stores them as a Pandas' DataFrame preserving only the content of the tweet, i.e. the text,
    and it's time of creation.
    It will apply a cleanup to the text in order to remove potentially harmful characters, such as
    unicode characters or escape characters.

    Parameters
    ----------
    unprocessed_tweets : list
        It is a list of json-formatted tweets with many data such as text, created_at, geo, is_translator
        Each line contains one tweet.

    Returns
    -------
    df : pandas.DataFrame
        df is a DataFrame that contains the content and timestamp of each tweet.
    """

    df = pd.DataFrame(json.loads(line) for line in unprocessed_tweets)

    # Remove unwanted data
    df = df[['text', 'created_at']]
    df = df[ pd.notnull(df['text']) ]
    df = df[ pd.notnull(df['created_at']) ]

    # Remove unicode characters
    df['text'] = df['text'].apply(removeUnicode)

    # Replace escape characters
    df['text'] = df['text'].apply(replaceEscape)

    return df


def clean(tweet_batch=None, batch_size=100):
    """
    This function process a list of json-formatted tweet converting them to a Pandas' DataFrame
    and removing potentially harmful characters.
    It has two modes of operation:
        -In standalone mode: accepts as a parameter a list of tweets to process and will return
        a DataFrame with the results. This mode is used when clean is called from an external function.
        -In non standalone mode: it uses command-line parameter as input file (argv[1]) and output
        file (argv[2]). This way the tweets are read from a file, processed and written to another
        file. It's important that the input file is not the same as the output file.

    Parameters
    ----------
    tweet_batch : list
         tweet_batch could be a list of json-formatted tweets or `None`. In the former case, the standalone
         mode will be used. In the latter the standalone mode will be disabled.
         Default value: None

    batch_size: int
        When called in standalone mode, this variable represents the maximum amount of tweets
        that will be processed in one go. This parameter ease the scalability of the algorithm
        as it can trade-off between speed and RAM memory. Using a low value makes this code
        usable in low-RAM machines, as it will not load all tweets at the same time. Using an
        enough high value makes that all tweets can be loaded in one go, speeding up the
        algorithm.
        In non standalone mode, this parameter will be ignored.
        Default value: 100

    Returns
    -------
    df : pandas.DataFrame
        df is a DataFrame that contains the content and timestamp of each tweet.

    Note: it returns df only in non standalone mode, i.e. when clean is called by another function.
    """

    standalone_mode = True if tweet_batch is None else False

    if standalone_mode:

        # Checking the parameters given by the user

        try:
            path_2_input = sys.argv[1]
            path_2_output = sys.argv[2]
        except IndexError:
            print(
            "The number of parameters given was insufficient. You need to provide at least 2: the input file and the output file")

        # Exists those files?

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

            # Processing input
            #################

            while not end_reached:

                del unprocessed_tweets[:]
                del cleaned_tweets[:]

                # Read a batch
                ind = 0

                while (not end_reached) and (ind < batch_size):

                    aux = f_input.readline()

                    if not aux:
                        end_reached = True
                    else:
                        unprocessed_tweets.append(aux)
                        ind += 1

                # Process a batch
                if len(unprocessed_tweets):
                    df = format_json_tweets(unprocessed_tweets)


                    # Processing output
                    ##################

                    # Apply output format
                    cleaned_tweets_ds = df['text'] + ' (timestamp: ' + df['created_at'] + ')'
                    cleaned_tweets = [x for x in cleaned_tweets_ds]

                    # Write cleaned batch
                    for tweet in cleaned_tweets:
                        f_output.write(tweet + '\n')

            # Add summary
            f_output.write("\n%d tweets contained unicode." % unicode_tweets)

        print('Full batch processed')

    else:
        return format_json_tweets(tweet_batch)


if __name__ == '__main__':
    clean()
