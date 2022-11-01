#load all libraries
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.ml.feature import Tokenizer,StopWordsRemover, CountVectorizer,IDF,StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vector
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#main program

#place your path to Spark: findspark.init('PATH-TO-SPARK-FOLDER')
findspark.init('/home/user/spark-3.3.0-bin-hadoop3')

#create spark session and give name the app.
spark = SparkSession.builder.appName('NLP').getOrCreate()

#import data
data = spark.read.csv("SMSSpamCollection",inferSchema=True,sep='\t')

#data column titles in file is meaningless; give names: class and text respectively
data = data.withColumnRenamed('_c0','class').withColumnRenamed('_c1','text')

#Clean and Prepare the Data

#make use of the length function found in Spark to get size of ham/spam messages
data = data.withColumn('length',length(data['text']))

#ML/NLP section inc feature engineering

#feature tools
tokenizer = Tokenizer(inputCol="text", outputCol="token_text")

stopremove = StopWordsRemover(inputCol='token_text',outputCol='stop_tokens')

count_vec = CountVectorizer(inputCol='stop_tokens',outputCol='c_vec')

idf = IDF(inputCol="c_vec", outputCol="tf_idf")

ham_spam_to_num = StringIndexer(inputCol='class',outputCol='label')

clean_up = VectorAssembler(inputCols=['tf_idf','length'],outputCol='features')

#classification model creation; Naive Bayes; others to look at: log. regress, random forests
nb = NaiveBayes()

#create pipeline
data_prep_pipe = Pipeline(stages=[ham_spam_to_num,tokenizer,stopremove,count_vec,idf,clean_up])

#clean data
cleaner = data_prep_pipe.fit(data)

clean_data = cleaner.transform(data)

#start model evaluation
clean_data = clean_data.select(['label','features'])

#split data into training and test sets
(training,testing) = clean_data.randomSplit([0.7,0.3])

spam_predictor = nb.fit(training)

#create some test results
test_results = spam_predictor.transform(testing)

#compare label vs. prediction; looking at accuracy (acc)
acc_eval = MulticlassClassificationEvaluator()
acc = acc_eval.evaluate(test_results)

print(f"Accuracy of Model at Predicting Spam Messages was {acc}")


