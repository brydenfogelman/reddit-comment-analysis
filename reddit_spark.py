import bz2
import json
import time
import boto.ec2
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import * # CountVectorizer, Tokenizer, RegexTokenizer, HashingTF
from pyspark.ml.regression import * # RandomForestRegressor, LinearRegression, DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def timeit(method):
    '''
    Decorator to time functions.
    '''
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print '%r took %2.2f sec\n' % \
              (method.__name__, te-ts)
        return result
    return timed

@timeit
def load_data(filename, test=True, mb=1):
    '''
    Returns either the a DataFrame containing all the tweets or a test DataFrame containing numTest comments.
    
    @params:
        test - boolean, if True return test DataFrame
        mb - the number of megabytes to load from the data set
    '''
    
    # # load compressed file
    # comments_file = bz2.BZ2File(filename, "r")
    
    # # convert the megabytes to bytes
    # size = mb * (1024 ** 2)
    
    # # load a test dataset of size mb
    # if test:
    #     # create RDD using string returned by reading the comments file
    #     # specify bytesize of file to read
    #     commentRDD = sc.parallelize(comments_file.readlines(size))
    #     # read RDD as json and convert to a DataFrame
    #     df = spark.read.json(commentRDD)
    # # load full dataset
    # else:
    df = spark.read.json(filename)
    
    # return a new DataFrame that doesn't contain deleted comments
    return df.filter("body != '[deleted]'")

if __name__ == "__main__":
    """
        Loading DF
    """

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "AKIAIRK3L5HHJOPY27QA")
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "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")

    # filename = 'RC_2015-01.bz2'
    filename = 's3a://big-data-project-bryden-fogelman/RC_2015-01.bz2'

    # load the comments into a DataFrame
    commentDF = load_data(filename)

    # commentDF = spark.read.json(filename)
    # Display comments and information
    print "Snippet of Comment DataFrame:"
    commentDF.select('id', 'body', 'ups', 'downs', 'gilded', 'subreddit').show(5)
    print "Column names of comment DataFrame:"
    print commentDF.columns
    print "\nThe total number of comments: %s (deleted comments removed)" % commentDF.count()
    
    # sc.stop()

