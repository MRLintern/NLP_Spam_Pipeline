### Introduction

This is a small spam message detector which makes use of Spark's Natural Language Processing (NLP) library and its Data Pipelining functionality. 
The dataset is included however it can also be downloaded [here](https://archive.ics.uci.edu/ml/datasets/SMS+Spam+Collection). The program uses Naive Bayes Algorithm to classify which messages are ham and which ones are spam. Note: Using Spark is a bit overkill; you don't need Spark to perform this type of work, but it was interesting doing so.

### Requirements

* `Python 3.8.10`. This is what I have installed.
* `Anaconda`. This will come with the latest version of Python, along with `Jupyter Notebook`, `Spider IDE` and a range of other useful Data Science tools.
* `Ubuntu 20.04`.
* `Apache Spark`. Dowload it from [here](https://spark.apache.org/downloads.html). Download the tar file and unzip at the command line.
* `pip3` for package management.
* `findspark`. This will make `PySpark` importable as a regular library. See [here](https://pypi.org/project/findspark/).

### Installation

This section covers the principles of getting Spark set up on your machine. If you don't want to install `Anaconda`, you can still install `Jupyter Notebook`.

`$ sudo apt install python3-pip`
`$ pip3 install jupyter`


