*Twitter* Insights *Data* Engineering
=====================================

This software is meant to simulate consuming the Twitter API and compute features for  Hashtags data analysis.

As we want to solve the data engineer's part of the problem, we will take care of the whole process of computing the right features. But, as a workaround to simulate the consumption of the API, we are going to process tweets from a file where each line has JSON-formatted tweet equals as those returned by the API.

A tweet can contain unicode characters, escape characters and some other unwanted elements. Hence, the first step is removing them. Also, the JSON contains many fields such as text, created\_at, geo, is\_translator ... but in this case, what is required from our data scientist only needs *text* and *created_at* , so we are focusing only on this fields.

The feature we are going to compute is the average degree of a vertex in a Twitter hashtag graph in a temporal window that covers the last 60 seconds.
But... what is this? Well, a Twitter hashtag graph is a graph connecting all the hashtags that have been mentioned together in a single tweet. The purpose of this feature will reveal to us some relevant insights about the relationships of each hashtag.

There are two main scripts: the first one *tweets_cleaned.py* take cares of pre-processing the input data, i.e. make the cleaning. The second one: *average_degree.py* will compute the average degree feature.


Requirements
============

* Pandas.
  As described in their web: "pandas is an open source, BSD-licensed library providing high-performance, easy-to-use data structures and data analysis tools for the Python programming language.".

  It can be downloaded from [1], but it's very important to take care of Pandas' own dependencies [2].
  The best way to install it is using via pypi, as it will automatically install all dependencies required:
 
	pip install pandas


How can I use it?
=================

The easiest way is just execute the *run.sh* file:

	./run.sh

as it will run the two scripts to compute the feature. But if you want to call only one of those, you can do as this:

	python ./src/tweets_cleaned.py ./tweet_input/tweets.txt ./tweet_output/ft1.txt

or 

	python ./src/average_degree.py ./tweet_input/tweets.txt ./tweet_output/ft2.txt



References
==========

[1] [http://pandas.pydata.org/](http://pandas.pydata.org/ "http://pandas.pydata.org/")

[2] [http://pandas.pydata.org/pandas-docs/stable/install.html#dependencies](http://pandas.pydata.org/pandas-docs/stable/install.html#dependencies "http://pandas.pydata.org/pandas-docs/stable/install.html#dependencies")
