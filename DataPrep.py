import pandas as pd
import findspark
import functools as ft
import pickle
import nltk
import string
import re

from nltk.corpus import stopwords
from nltk.corpus import wordnet
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('omw-1.4')
nltk.download('averaged_perceptron_tagger')

from nltk.stem import WordNetLemmatizer


import findspark
from pyspark import SparkContext

import os, sys
from pyspark.sql import SparkSession

from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from pyspark.mllib.classification import NaiveBayesModel

class data_prepper():
    
    def __init__(self):
        super().__init__()

        findspark.init()

        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        self.spark = SparkSession.builder.master('local[*]').appName("ml_example").getOrCreate()

        self.sc = self.spark.sparkContext
        
    def clean_data(self, text):
        mo = data_prepper.clear_out(self, text)
        mo = data_prepper.remove_stopwords(self, mo)
        mo = data_prepper.lem_df(self, mo)
        mo = data_prepper.tfidf_vectorizer(self, mo)
        
        return mo
        
    def clear_out(self, text):
        mo = text.lower()
        mo = re.sub(r'https?:\/\/\S+', '', mo)               # Removing all links
        mo = re.sub(r"/", ' ', mo)
        mo = re.sub(r'@[A-Za-z0-9]+', '', mo)                # Removing all @mentions
        mo = re.sub(r'#', '', mo)                           
        mo = re.sub(r"([^0-9A-Za-z \t])", "", mo)
        mo = re.sub(r"\w*\d\w*", "", mo)
        mo = re.sub(r"^\d+\w|^\w+\d+\w|^\w+\d$", "", mo)
        mo = mo.encode('ascii', 'ignore').decode('ascii')
    
        return mo
    
    def remove_stopwords(self, text):
    
        alphabet_list = list(string.ascii_lowercase)
        stop_words = stopwords.words('english')

        for i in range(len(alphabet_list)):
            if (alphabet_list[i] in stop_words):
                continue
            else:
                stop_words.append(alphabet_list[i])
        
        
        result_words = [word for word in text.split(' ') if word not in stop_words]
        str_result = ' '.join(result_words)
        
        return str_result

    
    def lem_df(self, text):
        
        lemmatizer = WordNetLemmatizer()
        
        sentence = nltk.word_tokenize(text)
        result_words = [lemmatizer.lemmatize(word) for word in sentence]
        str_result = ' '.join(result_words)

        return str_result
    
    def tfidf_vectorizer(self, text):
        
        data = [{'CONTENT': text}]
        docs = self.spark.createDataFrame(data)
        
        # print(docs.show())
        
        tokenize = Tokenizer(inputCol='CONTENT', outputCol='WORDS')
        words_data = tokenize.transform(docs.dropna())

        hashingTF = HashingTF(inputCol="WORDS", outputCol="tf_features", numFeatures=20)
        featurized_data = hashingTF.transform(words_data)

        idf = IDF(inputCol="tf_features", outputCol="features")
        idfModel = idf.fit(featurized_data)
        rescaledData = idfModel.transform(featurized_data)

        # rescaledData.show()
        final_data = rescaledData.drop('CONTENT', 'WORDS', 'tf_features')
        
        return final_data.first()['features']
        