# LogFileAnalyzer
Objective: Get top k users from the log file

```
Three solutions:
/solution/version 3 java/: Using Hadoop map-reduce approach, get top k users;
/solution/version 2 py/:   Read file in chunks from the disk, Maintain a dictionary file kept on the disk, get top k users;
/solution/version 1 py/:   Read file in chunks from the disk, Maintain an in-memory dictionary of unique users, get top k users;

How to solve "get top k users" part?
>> Just maintain a heap of size 'k', which will retain only 'k' most frequent IP values.


Sample data set
/logs/...


Codes to generate sample data set:
/test.maker/...

```
Coding Languages: python, java, shell script