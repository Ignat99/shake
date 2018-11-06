#!/bin/sh

#rm -r masterpieces
#rm -r masterpieces_wordcount
#mkdir masterpieces
#mkdir masterpieces_wordcount

#wget https://storage.cloud.google.com/apache-beam-samples/shakespeare/kinglear.txt
#mv kinglear.txt masterpieces

#wget https://storage.cloud.google.com/apache-beam-samples/shakespeare/othello.txt
#mv othello.txt masterpieces

#wget https://storage.cloud.google.com/apache-beam-samples/shakespeare/romeoandjuliet.txt
#mv romeoandjuliet.txt masterpieces

#hadoop fs -put masterpieces

#yarn jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar\
#mapred streaming\
yarn jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.1.1.jar\
 -input masterpieces\
 -output masterpieces_wordcount\
 -mapper mapper.py\
 -reducer reducer.py\
 -file mapper.py\
 -file reducer.py

# -mapper /bin/cat \
# -reducer /usr/bin/wc\
# -D mapreduce.job.reduces=0

# -combiner "python reducer.py"


hadoop fs -text masterpieces_wordcount/* | sort -n -k2,2 | tail -n5
