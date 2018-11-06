#!/bin/sh

rm -r masterpieces
rm -r masterpieces_wordcount
mkdir masterpieces
mkdir masterpieces_wordcount

wget https://storage.cloud.google.com/apache-beam-samples/shakespeare/kinglear.txt
mv kinglear.txt masterpieces

wget https://storage.cloud.google.com/apache-beam-samples/shakespeare/othello.txt
mv othello.txt masterpieces

wget https://storage.cloud.google.com/apache-beam-samples/shakespeare/romeoandjuliet.txt
mv romeoandjuliet.txt masterpieces

hadoop fs -put masterpieces

yarn jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar\
 -input masterpieces\
 -output masterpieces_wordcount\
 -file mapper.py\
 -file reducer.py\
 -mapper "python mapper.py"\
 -reducer "python reducer.py"\
 -combiner "python reducer.py"


hadoop fs -text masterpieces_wordcount/* | sort -n -k2,2 | tail -n5