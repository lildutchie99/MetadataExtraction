import requests
from bs4 import BeautifulSoup, Comment
import mysql.connector
import sys
import time
import string
import math
from operator import itemgetter
from functional import compose
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

    """
    Adapted from https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Longest_common_substring
    """
    def longest_common_substring(self, s1, s2):
        m = [[0] * (1 + len(s2)) for i in range(1 + len(s1))]
        longest, x_longest = 0, 0
        for x in range(1, 1 + len(s1)):
            for y in range(1, 1 + len(s2)):
                if s1[x - 1] == s2[y - 1]:
                    m[x][y] = m[x - 1][y - 1] + 1
                    if m[x][y] > longest:
                        longest = m[x][y]
                        x_longest = x
                else:
                    m[x][y] = 0
        return (x_longest-longest, s2.find(s1[x_longest - longest: x_longest]), longest, s1[x_longest - longest: x_longest])

    """
    Generate a value representing a word's 'fitness' as a potential keyword, based on it's TF and IDF
    Will and should be changed
    """
    def termFitness(self, term):
        dat = self.dict[term]
        return math.log(dat[0])/dat[1] if dat[1] is not None else dat[0]*1000000000 #temporary solution

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

        data = {'title':'', 'desc':'', 'imgs':[], 'kw':[]}

        doc = BeautifulSoup(requests.get(url).content, 'html.parser').html
        #Use test file...
        #with open('Test.html') as file:
        #    doc = BeautifulSoup(file.read(), 'html.parser').html

        #for testing optimization... (ignore time needed to load page)
        self.starttime = time.clock()

        #find all relevant text, excluding comments and scripts
        doctext = ' '.join(doc.findAll(text=lambda text: not (isinstance(text, Comment) or text.parent.name=='script')))

        doctext = doctext.translate(self.punctbl) #strip punctuation
        words = doctext.lower().split()

        cur = sqlconn.cursor()

        dict = {} #all the info about a term - TF, IDF, etc.
        self.dict = dict #for reference by other functions

        #first count term frequency
        for word in words:
            if word not in dict:
                dict[word] = [1]
            else:
                dict[word][0] += 1

        #remove terms with a TF below tfCutoff
        dict = {key:dict[key] for key in dict if dict[key][0] >= params['tfCutoff']}

        #remove blacklisted words
        for word in [w for w in dict.keys()]:
            cur.execute("select word from blacklist where word=%s;", (word,))
            r = cur.fetchall()
            if r != []:
                del dict[word]
            if re.compile('\\d+').match(word):
                del dict[word]

        #combine words with same root
        plural = re.compile(',?e?s,?')

        keys = list(dict.keys())
        group = [] #list of keys to group

        for w1idx in range(len(keys)):
            for w2idx in range(w1idx, len(keys)):
                w1 = keys[w1idx]
                w2 = keys[w2idx]
                if w1 != w2:
                    pos1, pos2, l, sub = self.longest_common_substring(w1, w2)
                    if l < params['groupCutoff']:
                        continue
                    pre = [w1[:pos1], w2[:pos2]]
                    suf = [w1[pos1+l:], w2[pos2+l:]]
                    if plural.match(','.join(suf)) and pre==['','']:
                        group.append((w1, w2))

        for g in group:
            merger = g[0] if dict[g[0]][0] > dict[g[1]][0] else g[1] #group into one with higher TF
            mergee = g[0] if merger==g[1] else g[1]
            dict[merger][0] += dict[mergee][0]
            del dict[mergee]

        for word in dict:
            dict[word].append(self.requestFreq(word, cur))

        print(sorted(dict.items(), key=compose(itemgetter(0), itemgetter(1)), reverse=True))

        terms = sorted(dict.keys(), key=self.termFitness, reverse=True) #sort descending by "fitness" determined by self.tfidfComp

        print(len(terms))
        print(terms)

        print("Total time: %fs" % (time.clock()-self.starttime,))

        return data

#example
config = {'user':'root',
          'password':'qwertyuiop',
          'host':'127.0.0.1',
          'database':'adxeed',
          'use_pure':False}
conn = mysql.connector.connect(**config)

e = Extract()
params = {'tfCutoff': 4,
          'groupCutoff': 3}
e.mine('https://www.codingwithkids.com/#!/', params, conn)