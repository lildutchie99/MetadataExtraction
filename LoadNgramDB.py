import mysql.connector
import re
import sys

#Utility to load Google unigram data into MySQL database

def load(conn, suffix):
    #--------------------PARAMS--------------------------------
    fname = 'C:\\Users\\Ben\\Downloads\\ngrams\\googlebooks-eng-all-1gram-20120701-' + suffix
    years = range(2000, 2009)
    #----------------------------------------------------------

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
            if int(items[1]) in years:
                word = items[0] if '_' not in items[0] else items[0][:items[0].find('_')]
                if word not in words:
                    words[word] = [int(items[2])]
                else:
                    words[word].append(int(items[2]))

    print('Finding mean frequency...')

    #take mean frequency
    for word in words:
        words[word] = sum(words[word]) / len(words[word])

    print('Uploading file: %s...' % (fname,))

    commit = 0 #save changes every 50 entries
    filter = re.compile("^[qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM']*$")
    for word in words:
        if len(word) > 30:
            continue #skip really long words
        if not filter.match(word):
            continue
        try:
            cur.execute('insert into unigrams_%s%s(word, freq) values("%s", %s);' % (suffix, word[1] if len(word) > 1 and word[1] != "'" else "", word, words[word]))
        except:
            print(word)
            print(words[word])
            raise
        if commit >= 50:
            conn.commit()
            commit = 0
        commit += 1

    conn.commit()

    print("Success!")

    cur.close()

conn = mysql.connector.connect(user='root', password='qwertyuiop', host='127.0.0.1', database='ngrams', use_pure=False)

for ch in range(ord('b'), ord('z')+1):
    load(conn, chr(ch))

conn.close()