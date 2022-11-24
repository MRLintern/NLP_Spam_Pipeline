### Introduction

This is a small spam message detector which makes use of Spark's Natural Language Processing (NLP) library and its Data Pipelining functionality. 
The dataset is included however it can also be downloaded [here](https://archive.ics.uci.edu/ml/datasets/SMS+Spam+Collection). The program uses Naive Bayes Algorithm to classify which messages are ham and which ones are spam. Note: Using Spark is a bit overkill; you don't need Spark to perform this type of work, but it was interesting doing so.

### Requirements

* `Python 3.8.10`. This is what I have installed.
* `Anaconda`. This will come with the latest version of Python, along with `Jupyter Notebook`, `Spider IDE` and a range of other useful Data Science tools.
* `Ubuntu 20.04`.
* `Apache Spark`. Dowload it from [here](https://spark.apache.org/downloads.html). Download the tar file and unzip at the command line.
* `pyspark`
* `pip3` for package management.
* `py4j`
* `Java SDK`
* `Scala`
* `findspark`. This will make `PySpark` importable as a regular library. See [here](https://pypi.org/project/findspark/).

### Getting the Application

The repository will come with everything thats needed.

* `$ git clone https://github.com/MRLintern/NLP_Spam_Pipeline.git`

Run each cell one at a time to see the data transformation.
If you're only interested in the end result, run the python script which will display the accuracy of the model. I used `gedit` for the editor and ran the script at the command line. Note: I had problems doing this in VSCode.

* `$ python3 nlp_spam_detection.py`

The model accuracy came in at around 93% running both the script and jupyter notebook.

## Results

The accuracy of the model came in around 92%.

