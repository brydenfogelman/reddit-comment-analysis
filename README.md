# Reddit Comment Analysis

## Overview

The purpose of the project was to get experience handling large amount of data. The project invovled over 50 millions reddit comments. Technolgies and frameworks such as MongoDB, SQL, Amazon Web Services and Databricks were used in the analysis of the data. Machine learning tasks were also performed on the data. The comment text was used as in the input into the model, certain methods such as Feature Hashing were utilized in order to optimize the implementation. All machine learning was supervised learning and involved a mix or regression and classification problems.

The Dataset can be found here: https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/

## Background
Reddit is a massive online community wherein users can post content into content-specific sub-communities called ‘subreddits’ to be viewed by others within and without the subreddit. Users can ‘upvote’ or ‘downvote’ posts to shift their rankings on the website and in this way as a community dictate which posts are more easily seen by the wider community and which are not. 

Users can also comment on and discuss posted content under a chosen pseudonym in each post’s ‘thread’ and comments can also be upvoted or downvoted as well as ‘gilded’ to show the community’s reaction to them. Reddit also uses other metrics to score comments such as ‘controversiality’ and ‘distinguished’.

A dataset containing all comments made on Reddit in January 2015 was released by a Reddit user, detailing for each comment; the sub-reddit it belongs to, the user who posted it, the number of upvotes and downvotes it received as well as other information as will be shown in the MongoDB section of this report.

## Objectives
Given this dataset, a lot can be learned about the nature of online communication and various data handling tools can be used to process the data. This project will focus on the use of databases, specifically the MongoDB noSQL query language, and word processing to come to an understanding of the dynamics of the Reddit online community and how the way people communicate online leads to different reactions from the community.


Specific tasks have been selected to implement various tools and are as follows:

Data Exploration:
* Set up a MongoDB database for the dataset
* Use Spark to extract information about the dataset
* Use SQL and Databricks to create visualizations

Machine Learning and Featurization Tasks:
* Compare Feature Hashing vs Bag of Words representation
* Determine if words are a good predictor of the score of the comment
* Determine if score is a good predictor of gilded comments
* Train a classifier to match comments to certain subreddits

Cluster Tasks:
* Create a multi node cluster on Databricks or Amazon Web Services (EMR or EC2) to run Spark tasks
* Perform computations and machine learning tasks on all the data efficiently



The full report can be found here: https://github.com/brydenfogelman/reddit-comment-analysis/blob/master/reddit-comment-analysis-report.pdf
