# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 11:08:29 2015

@author: Ricardo Guerrero
"""


import itertools
import json
import os
import pandas as pd
import re
import sys


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
    This function will replace each scape character and multiple whitespaces the following way:

        \/ -> /
        \\ -> \
        \' -> '
        \” -> “
        \n -> space
        \t -> space

        Also, all whitespace escape characters should be replaced with a single space.

    As it is stated in the FAQ of the challenge

    :param cad:
    :return:
    '''

    patterns = [r"\\/", r"\'", r'\"', "\n", "\t", r"  +"]
    replacements = [r"/" , r"'", r'"', r" ", r" ", r" "]
    #TODO remove the second escape character

    for pat, rep in itertools.izip(patterns, replacements):
        re.sub(pat, rep, cad)

    return cad

def process_tweets(unprocessed_tweets):
    '''
    # Clean a batch
    '''


    df = pd.DataFrame(json.loads(line) for line in unprocessed_tweets)

    # Remove unwanted data
    df = df[['text', 'created_at']]

    # Remove unicode characters
    df['text'] = df['text'].apply(removeUnicode)

    # Replace escape characters
    df['text'] = df['text'].apply(removeEscape)

    return df


def clean(tweet_batch = None, batch_size = 100):


    standalone_mode = True if tweet_batch is None else False


    if standalone_mode:


        # Checking the parameters given by the user

        try:
            path_2_input = sys.argv[1]
            path_2_output = sys.argv[2]
        except IndexError:
            print("The number of parameters given was insufficient. You need to provide at least 2: the input file and the output file")

        #Exists those files?

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
                    df = process_tweets(unprocessed_tweets)


                    #Processing output
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
        return process_tweets(tweet_batch)



if __name__ == '__main__':
    clean()


