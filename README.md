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

## Other Information

Directory *scripts* contains various python scripts for data exploration.
Script *move_data.py* illustrates how to save parquet data from a remote AWS S3 repository to local repository.



