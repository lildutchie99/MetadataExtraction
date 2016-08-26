import mysql.connector
import re
import statistics as stats
import string

#Utility to load Google unigram data into MySQL database

def load(conn, suffix):
    #--------------------PARAMS--------------------------------
    fname = 'C:\\Users\\Ben\\Downloads\\ngrams\\googlebooks-eng-all-1gram-20120701-' + suffix
    tcounts_fname = "C:\\Users\\Ben\\Downloads\\ngrams\\googlebooks-eng-all-totalcounts-20120701.txt"
    years = range(2000, 2009)
    #----------------------------------------------------------

    tcounts = {}
    with open(tcounts_fname) as file:
        for line in file.read().split():
            items = line.split(',')
            if int(items[0]) in years:
                tcounts[int(items[0])] = int(items[1])

    cur = conn.cursor()

    print('Creating tables...')

    for ch in range(ord('a'), ord('z')+1):
        cur.execute('create table if not exists unigrams_%s%s(word varchar(30), freq float);' % (suffix, chr(ch)))
    #create default table for short/weird spellings
    cur.execute('create table if not exists unigrams_%s(word varchar(30), freq float);' % (suffix,))
    conn.commit()

    print('Parsing file: %s...' % (fname,))

    words = {}
    with open(fname, encoding='utf-8') as file:
        for line in file:
            items = line.split('\t')
            year = int(items[1])
            if year in years:
                word = items[0]
                if word not in words:
                    words[word] = [float(items[2])/tcounts[year]]
                else:
                    words[word].append(float(items[2])/tcounts[year])

    print('Finding median frequency...')

    #take median frequency across relevant years
    for word in words:
        words[word] = stats.median(words[word])

    print('Uploading file: %s...' % (fname,))

    commit = 0 #save changes every 50 entries
    filter = re.compile("^[qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM'.\\-]*$")
    for word in words:
        if len(word) > 30:
            continue #skip really long words
        if not filter.match(word): #no punctuation
            continue
        try:
            cur.execute('insert into unigrams_%s%s(word, freq) values("%s", %s);' % (suffix, word[1] if len(word) > 1 and word[1] not in string.punctuation else "", word, words[word]))
        except:
            print(word)
            print(words[word])
            raise
        if commit >= 200:
            print('Commiting...')
            conn.commit()
            print('Done.')
            commit = 0
        commit += 1

    conn.commit()

    print("Success!")

    cur.close()

"""
Creates case-insensitive versions of the unigram tables
"""
def make_ci_tables(conn):
    cur = conn.cursor()
    for c1 in range(ord('a'), ord('z')+1):
        for c2 in range(ord('a'), ord('z')+1):
            print('Making CI table %s%s' % (chr(c1),chr(c2)))
            cur.execute('create table unigrams_%s%s_ci select word, sum(freq) as freq from unigrams_%s%s group by word;' % (chr(c1), chr(c2), chr(c1), chr(c2)))
            conn.commit()
    for c in range(ord('a'), ord('z')+1):
            print('Making CI table %s' % (chr(c),))
            cur.execute('create table unigrams_%s_ci select word, sum(freq) from unigrams_%s group by word;' % (chr(c), chr(c)))
            conn.commit()
    cur.close()

#creds...
conn = mysql.connector.connect(user='root', password='qwertyuiop', host='127.0.0.1', database='adxeed', use_pure=False)

#for ch in range(ord('a'), ord('z')+1):
#    load(conn, chr(ch))

make_ci_tables(conn)

conn.close()