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

hdfs dfs -rm -R /user/ubuntu/masterpieces_wordcount

#mapred streaming\
yarn jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.1.jar\
 -input masterpieces\
 -output masterpieces_wordcount\
 -mapper mapper.py\
 -reducer reducer.py\
 -combiner reducer.py\
 -file mapper.py\
 -file reducer.py

# -mapper /bin/cat \
# -reducer /usr/bin/wc\
# -D mapreduce.job.reduces=0


rm -r masterpieces_wordcount
mkdir masterpieces_wordcount
hdfs dfs -get /user/ubuntu/masterpieces_wordcount/*  masterpieces_wordcount

hadoop fs -text masterpieces_wordcount/* | sort -n -k2,2 | tail -n5
