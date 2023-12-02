# David Jones – Data Engineer: Coding Test

This document will provide clear instructions for the candidates for the take home coding tests.

## Instructions for the candidates: 

Return the solution within 2 working days.
You can use technology/languages/tools/libraries of your choice.
Please share your git repository with the solution. Better if you can do a PR.
As next steps, we will schedule a 60-90 mins in person code review / interview.

## Problem Statement: Customer Segmentation

You are provided with with two files

* **Input:** CustData.json
  * This is a sample input file. Generate 5000 rows of data.
* **Output:** CustSeg.csv 

Processes this data and perform customer segmentation based on their total purchase amount. The segmentation should be done according to the following criteria: 

* "Low": Total purchase amount less than $100.
* "Medium": Total purchase amount between $100 and $500.
* "High": Total purchase amount greater than $500.

Implement customer segmentation based on the specified purchase amount ranges.

## Run Script

In the `./src` folder, run `python .\cust_seg.py`. The output file will be created in the `./src/CustSeg.csv/` folder.

## Run Unit Tests
In the `./tests` folder, run `pytest -vv -s --log-cli-level=INFO`. Expected output should match `./tests/data/CustSeg.csv`

