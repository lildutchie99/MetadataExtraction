import requests
from bs4 import BeautifulSoup, Comment
import mysql.connector
import sys
import time
import string
import math
from operator import itemgetter
from functional import compose
from itertools import groupby
import re

class Extract:

    """
    Debug only function for optimizing
    """
    def br(self):
        print('Total time: %fs' % (time.clock()-self.starttime,))
        sys.exit()

    """
    Takes optional paramaters for what year range to use in ngram retrieval
    """
    def __init__(self):
        self.punctbl = str.maketrans(string.punctuation.replace("'", ''), " " * (len(string.punctuation)-1)) #remove all except apostrophe
        self.taglemma_props = '{"tokenize.whitespace":"true","annotators":"tokenize,pos,lemma","outputFormat":"json"}'
        self.prefixes = ['ante', 'anti', 'circum', 'co', 'de', 'dis', 'em', 'en', 'epi', 'ex', 'extra', 'fore',
                         'homo', 'hyper', 'il', 'im', 'in', 'ir', 'im', 'in', 'infra', 'inter', 'intra', 'macro',
                         'micro', 'mid', 'mis', 'mono', 'non', 'omni', 'para', 'post', 'pre', 're', 'semi', 'sub',
                         'super', 'therm', 'trans', 'tri', 'un', 'uni']
        self.prefix_regexp = re.compile('^(' + ')|('.join(self.prefixes) + ').*')
        self.num_regexp = re.compile('\\d+')
        self.pos_filter = re.compile('^((JJ)|(NN)|(RB)|(VB)).*')

    """
    Weighted mean utility function
    """
    def wavg(self, w, data):
        assert len(w) == len(data)
        return sum([w[i]*data[i] for i in range(len(w))]) / sum(w)

    """
    Generate a value representing a word's 'fitness' as a potential keyword, based on it's TF and IDF
    Will and should be changed
    """
    def termFitness(self, term):
        global dict
        dat = dict[term]
        return math.log(dat[0])/dat[3] if dat[3] is not None else dat[0]*1000000000 #temporary solution

    """
    Query SQL db for frequency of a word
    """
    def requestFreq(self, word, cur):
        try:
            #use case-insensitive reference
            cur.execute('select freq from unigrams_%s%s_ci where word="%s" limit 1;'
                        % (word[0], word[1] if len(word) > 1 and word[1] not in string.punctuation else "", word))
            resp = cur.fetchall()
        except:
            return None
        if len(resp) == 0: #no results
            return None
        else:
            return resp[0][0]

    """
    @:param url: url of site to be mined
    @:param params: dictionary of assorted parameters relating to filtering and ranking
    @:param sqlconn: a MySQL connection object for accessing term frequency data
    @:return a dict including title, description, a list of images, and keywords
    """
    def mine(self, url, params, sqlconn):

        doc = BeautifulSoup(requests.get(url).content, 'html.parser').html
        #Use test file...
        #with open('Test.html') as file:
        #    doc = BeautifulSoup(file.read(), 'html.parser').html

        #for testing optimization... (ignore time needed to load page, varies and not relevant to optimiztion)
        self.starttime = time.clock()

        #find all relevant text, excluding comments and scripts
        doctext = ' '.join(doc.findAll(text=lambda text: not (isinstance(text, Comment) or text.parent.name=='script')))

        doctext = doctext.translate(self.punctbl) #strip punctuation
        words = doctext.lower().split()

        cur = sqlconn.cursor()

        global dict
        dict = {} #all the info about a term - [TF, POS, lemma, IDF]

        #first count term frequency
        for word in words:
            if word not in dict:
                dict[word] = [1]
            else:
                dict[word][0] += 1

        #remove blacklisted words
        for word in [w for w in dict.keys()]:
            cur.execute("select word from blacklist where word=%s;", (word,))
            r = cur.fetchall()
            if r != [] or self.num_regexp.match(word) or len(word)==1:
                del dict[word]

        #check provided server URL
        url = params['nlpserv_url']
        if not url.endswith('/'):
            url += '/'

        #POS tag and lemma for each word
        for word in [k for k in dict.keys()]:
            pre = self.prefix_regexp.match(word)
            if pre:
                stripped = word[pre.endpos:]
                origfreq = self.requestFreq(word, cur)
                strfreq = self.requestFreq(stripped, cur)
                if strfreq > origfreq if strfreq is not None and origfreq is not None else \
                        (dict[stripped][0] > dict[word][0] if stripped in dict and word in dict else False): #if info not present in ngram db, defer to TF. If TF undefined, assume the words aren't related
                    if stripped in dict:
                        dict[stripped][0] += dict[word][0]
                    else:
                        dict[stripped][0] = dict[word][0]
                    del dict[word]
                    word = stripped
            resp = requests.post('%s?properties=%s' % (url, self.taglemma_props), data=word).json()
            dict[word].append(resp['sentences'][0]['tokens'][0]['pos'])
            dict[word].append(resp['sentences'][0]['tokens'][0]['lemma'])

            if not self.pos_filter.match(dict[word][1]): #filter out unnessecary POSs
                del dict[word]

        #group words with same lemma
        tlist = sorted(list(dict.items()), key=compose(itemgetter(2), itemgetter(1))) #ordered by lemma
        dict = {key:[sum([g[1][0] for g in groups]), groups[0][1][1][0:2], key] for (key, groups) in [(k, list(g)) for (k,g) in groupby(tlist, key=compose(itemgetter(2), itemgetter(1)))]}

        #remove terms with a TF below tfCutoff
        dict = {key:dict[key] for key in dict if dict[key][0] >= params['tfCutoff']}

        for word in dict:
            dict[word].append(self.requestFreq(word, cur))

        print('Term dictionary: [TF, POS, lemma, IDF]:')
        print(dict)

        terms = sorted(dict.keys(), key=self.termFitness, reverse=True) #sort descending by "fitness" determined by self.tfidfComp

        print('\nTerms ranked by fitness (%d total):' % len(terms))
        print(terms)

        print("Total time: %fs" % (time.clock()-self.starttime,))

#example
config = {'user':'root',
          'password':'qwertyuiop',
          'host':'127.0.0.1',
          'database':'adxeed',
          'use_pure':False}
conn = mysql.connector.connect(**config)

e = Extract()
params = {'tfCutoff': 4,
          'groupCutoff': 3,
          'nlpserv_url': 'http://localhost:9000'}
e.mine('https://adxeed.com/blog/view/Case+study%3A+How+cross-network+advertising+helps+improve+overall+ROI', params, conn)