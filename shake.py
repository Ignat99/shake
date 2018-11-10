from __future__ import print_function
from nltk.tokenize import RegexpTokenizer
#from nltk.stem.snowball import RussianStemmer
from nltk.stem import *

def tokenize(doc):
    tokenizer = RegexpTokenizer(r'[а-яёa-z0-9]+')
    res = copy.deepcopy(doc)
    tokens  = tokenizer.tokenize(res['text'])
    res['tokens'] = list(filter(lambda x: len(x) < 15, tokens))
    return res

def stem(doc):
    #stemmer = RussianStemmer()
    stemmer = SnowballStemmer("english", ignore_stopwords=True)
    res = copy.deepcopy(doc)
    res['stemmed'] = [stemmer.stem(token) for token in res['tokens']]
    return res

RDDread = sc.textFile ("file:///home/ubuntu/shake/mas/shakespeare_*.txt")
#stemmed_docs = clean_text_rdd.map(tokenize).map(stem).cache()
stemmed_docs = RDDread.map(tokenize).map(stem).cache()

# Calculate Word counts

def get_words(doc):
    return [(word, 1) for word in set(doc['stemmed'])]

word_counts = stemmed_docs.flatMap(get_words)\
            .reduceByKey(lambda x, y: x+y).collect()

# Calculate Inverse Document Frequency (IDF)

doc_count = stemmed_docs.count()

def get_idf(doc_count, doc_with_word_count):
    return math.log(doc_count/doc_with_word_count)

idf = word_counts.mapValues(lambda word_count: get_idf(doc_count, word_count))

# Get list of stop words

idf_border = 1.12
stop_words_list =idf.filter(lambda x: x[1] < idf_border).keys().collect()
stop_words = set(sop_words)

