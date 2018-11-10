#!/bin/sh

#rm -r masterpieces
#mkdir masterpieces

#wget https://storage.cloud.google.com/apache-beam-samples/shakespeare/kinglear.txt
#mv kinglear.txt masterpieces

#wget https://storage.cloud.google.com/apache-beam-samples/shakespeare/othello.txt
#mv othello.txt masterpieces

#wget https://storage.cloud.google.com/apache-beam-samples/shakespeare/romeoandjuliet.txt
#mv romeoandjuliet.txt masterpieces

#hdfs dfs -rm -R /user/ubuntu/masterpieces
#hadoop fs -put masterpieces

#hdfs dfs -rm -R /user/ubuntu/masterpieces_wordcount

/opt/spark/bin/spark-submit /home/ubuntu/shake/Spark/wordcount.py file:///home/ubuntu/shake/Spark/masterpieces/shakespeare_*.txt > log.txt

#rm -r masterpieces_wordcount
#mkdir masterpieces_wordcount
#hdfs dfs -get /user/ubuntu/masterpieces_wordcount/*  masterpieces_wordcount

#hadoop fs -text masterpieces_wordcount/* | sort -n -k2,2 | tail -n5
