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

stemmed_docs = clean_text_rdd.map(tokenize).map(stem).cache()
