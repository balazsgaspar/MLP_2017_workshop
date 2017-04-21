# MLP\_2017\_workshop

## Introduction

This is an accompanying website for MLPrague 2017 workshop *Advanced data analysis on Hadoop clusters*.
Specifically, source codes for the machine learning part are provided.
Description of the used data can be found here too, see below.

Source codes are created for [Spark](http://spark.apache.org/).

## Problem Statement

Tha practical part of machine learning can be divided into two parts:
1) Community detection in telecommunication networks
2) Churn prediction in telecommunication industry

As churn prediction part assumes results from community detection, it is necesarry to run the codes described in Community Detection section first.
Then the churn prediction part can be executed by running the *main.py* script.

## Community Detection

Given the phone call records, the task is to find communities in a network created from these phone calls.
Customers represent vertices in such a network and edges link customers who called to each other.

The presented solution creates a graph from one-month call records.
Only customers with at least 10 calls are linked together.
[Label Propagation Algorithm](https://en.wikipedia.org/wiki/Label_Propagation_Algorithm) is used for community detection.

Scala source codes for Spark can be found in phase\_0\_community\_detection/ directory.
The scala script assumes mlp\_sampled\_cdr\_records.parquet data available.

This script will create two new data files: lpa\_20160301\_20160401.parquet and lpa\_20160401\_20160501.parquet.

## Churn Prediction

In this part, the task is to predict customers who are likely to churn.
All source codes for this part are written in python are assumed to be run by PySpark.
Created machine learning model uses features extracted from one month and predicts potential churners for the next month.
For example, it takes phone call records from March and predicts which customers are likely to churn in April.
Features are built from the input data described below.

This part is divided into three phases:
1) Data preparation - creates various features from the input data
2) Data preprocessing - imputing and trasforming features; it also adds some new derived features
3) Classification - trains a classification model on a train dataset and applies it on a test dataset

Evaluation of the model is performed outside of those phases for the sake of detailed illustration.

## Other Information

Directory *scripts* contains various python scripts for data exploration.
Script *scripts/move_data.py* illustrates how to save parquet data from a remote AWS S3 repository to local repository.


## Input Data Description

*mlp\_sampled\_cdr\_records.parquet* - phone call records from two months

* record\_type: string - type of voice records
* date\_key: string - date of the call
* duration: integer - duration of the call in seconds
* frommsisdn\_prefix: string - operator prefix
* frommsisdn: long - home operator number (either receiving or calling - according to the record type)
* tomsisdn\_prefix: string - operator prefix
* tomsisdn: long - number of the second customer (can be either of the home operator or not)


*mlp\_sampled\_ebr\_base\_20160401.parquet*, *mlp\_sampled\_ebr\_base\_20160501.parquet* - information about home operator customers

* msisdn: long - number of the customer
* customer\_type: string - either private or business
* commitment\_from\_key: string - date of the commitment start
* commitment\_to\_key: string - date of the commitment end
* rateplan\_group: long - name of the rateplan group
* rateplan\_name: long - name of the raplan

*mlp_sampled_ebr_churners_20151201_20160630.parquet* - list of churned customers from two months

* msisdn: long - number of the customer
* date\_key: string - date of the churn

## Description of Features

NOTE: 'callcenters' are numbers behaving like callcenters - i.e. they call to a huge number of phone numbers.
We select TOP 12 such 'callcenters' from data.

* churned - binary label attribute
* msisdn
* customer\_type 
* rateplan\_group
* rateplan\_name
* committed - whether the customer is committed at this point
* committed\_days - for how long is the customer committed
* commitment\_remaining - how many days till the end of the commitment
* callcenter\_calls\_count - count of phone calls with so called 'callcenters'
* callcenter\_calls\_duration - total duration of phone calls with so called 'callcenters'
* cc\_cnt\_X1 - count of phone calls with call center X1, where X1 is the number of the callcenter
* cc\_dur\_X1 - duration of phone calls with call center X1, where X1 is the number of the callcenter
* cc\_avg\_X1 - average duration of phone calls with call center X1, where X1 is the number of the callcenter
* cc\_std\_X1 - standard deviation of duration of phone calls with call center X1, where X1 is the number of the callcenter
* com\_degree - vertex degree in the graph used for community detection
* com\_degree\_total - vertex  degree within the community
* com\_count\_in\_group - number of vertices in the same community
* com\_degree\_in\_group - sum of degrees in the vertex's community
* com\_score - score computed as degree / degree\_in\_group 
* com\_group\_leader - boolean; whether the vertex has maximal score within the group
* com\_group\_follower - boolean; whether the vertex has minimal score within the group
* com\_churned\_cnt - how many customers from the community churned 
* com\_leader\_churned\_cnt - how many customer leaders from the community churned

... rest of the features represent various characteristics about phone calls.
Duration of calls is always expressed in seconds.
More specifically, "dur" represents duration, "cnt" count, "avg" average, "std" standard deviation.
There may be phone calls to people belonging to the same operator ("\_t\_") or different operator ("\_not\_t\_"), or it is not differentiated ("all").
Moreover, there may be distinction between incoming and outgoing calls.




