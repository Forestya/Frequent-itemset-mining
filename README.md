# Frequent Item Set Mining in E-commerce Transaction Logs

## Project Overview
This project aims to detect frequent item sets in E-commerce transaction logs using Apache Spark. The dataset consists of customer purchase transaction logs collected over time, with each record containing an invoice number, a description of the item, the quantity purchased, the date of the invoice, and the unit price.

## Problem Definition
The task is to detect the top-k frequent item sets from the log for each month. The support of an item set X in a month M is computed as: 

Support(X) = (Number of transactions containing X in M) / (Total number of transactions in M)

## Output Format
The output format is "MONTH/YEAR,(Item1|Item2|Item3), support value", where the three items are ordered alphabetically. The results are sorted first by time in ascending order, then by the support value in descending order, and finally by the item set in alphabetical order.

## Code Execution
The code should take three parameters: the input file, the output folder, and the value of k. The command to run the code is as follows:

```bash
$ spark-submit project2_rdd.py "file:///home/sample.csv" "file:///home/output" 2
