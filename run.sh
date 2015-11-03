#!/usr/bin/env bash

# Script to run the algorithms developed for the Insight Data Engineer challenge 2015

# I'll execute my programs, with the input directory tweet_input and output the files in the directory tweet_output
python ./src/tweets_cleaned.py ./tweet_input/tweets.txt ./tweet_output/ft1.txt
python ./src/average_degree.py ./tweet_input/tweets.txt ./tweet_output/ft2.txt



