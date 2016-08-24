import requests
from bs4 import BeautifulSoup, Comment
import string
import sys
import mysql.connector
from operator import itemgetter

class Extract:

    """
    Takes optional paramaters for what year range to use in ngram retrieval
    """
    def __init__(self, ngram_year=2008, ngram_smooth=3):
        self.punctbl = str.maketrans(string.punctuation.replace("'", ''), " " * (len(string.punctuation)-1)) #remove all except apostrophe
        self.ngram_year = ngram_year
        self.ngram_smooth = ngram_smooth

    """
    Generate a value representing a word's 'fitness' as a potential keyword, based on it's TF and IDF
    Will and should be changed
    """
    def tfidfComp(self, tf, idf):
        return tf/idf if idf is not None else 0 #temporary solution

    """
    Query SQL db for frequency of a word
    """
    def requestFreq(self, word, cur):
        try:
            cur.execute('select freq from unigrams_%s%s where word="%s";' % (word[0], word[1] if len(word) > 1 and word[1] != "'" else "", word))
            resp = cur.fetchall()
        except:
            return None
        if len(resp) == 0: #no results
            return None
        else:
            return sum([item[0] for item in resp]) #count all spellings

    """
    @:param url: url of site to be mined
    @:param tfThresh: minimum mentions of a term for it to be elegible as a keyword
    @:param sqlconn: a MySQL connection object for accessing term frequency data
    @:return a dict including title, description, a list of images, and keywords
    """
    def mine(self, url, tfThresh, sqlconn):
        data = {'title':'', 'desc':'', 'imgs':[], 'kw':[]}

        doc = BeautifulSoup(requests.get(url).content, 'html.parser').html
        #Use test file...
        #with open('Test.html') as file:
        #    doc = BeautifulSoup(file.read(), 'html.parser').html

        #find all relevant text, excluding comments and scripts
        doctext = ' '.join(doc.findAll(text=lambda text: not (isinstance(text, Comment) or text.parent.name=='script')))

        doctext = doctext.translate(self.punctbl) #strip punctuation
        words = doctext.split()

        cur = sqlconn.cursor()

        tfdict = {}
        for word in words:
            if word not in tfdict:
                tfdict[word] = 1
            else:
                tfdict[word] += 1

        idfdict = {word[0]: self.requestFreq(word[0], cur) for word in tfdict.items() if word[1] >= tfThresh}
        print(idfdict)
        terms = sorted(idfdict.keys(), key=lambda item: self.tfidfComp(tfdict[item], idfdict[item]), reverse=True) #sort descending by tf/idf ratio

        print(len(terms))
        print(terms)

        return data

#example
config = {'user':'root',
          'password':'qwertyuiop',
          'host':'127.0.0.1',
          'database':'ngrams',
          'use_pure':False}
conn = mysql.connector.connect(**config)

e = Extract()
e.mine('https://adxeed.com/blog/view/Case+study%3A+How+cross-network+advertising+helps+improve+overall+ROI', 3, conn)