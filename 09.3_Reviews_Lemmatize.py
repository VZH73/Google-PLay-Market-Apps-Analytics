# Now let's see what we can do with the actual reviews.
# Can we extract any insights from them?
# It seems it will be very difficult to conduct any full-scale NLP analysis with so many apps and their reviews
# So, in order to simplify, as a first step of NLP will be tokenization and lemmatization, keeping only
# VERBS, NOUNS, ADJECTIVES and ADVERBS, as main holders of the review meaning, skipping all the rest.
# This way we will decrease the occupied space and speed up further processing
# For the tokenization and lemmatization we will be using spacy library as it offers best performance,
# altogether with using GPU
# Result will be written in a separate folder, to keep original text reviews for whatever reason in future
import pandas as pd
import os
#from tqdm.auto import tqdm
import spacy
spacy.require_gpu()
nlp = spacy.load("en_core_web_sm")

#from threading import Thread
#from queue import Queue
from multiprocessing import Process, Queue
import logging

def MakeLemma(appId):
    try:
        fname_from = 'ReviewsPKL//{}.pkl'.format(appId)
        fname_to = 'ReviewsLemma//{}.pkl'.format(appId)
        if os.path.exists(fname_from):
            if not os.path.exists(fname_to):
                reviews = pd.read_pickle(fname_from)

                # Skip apps with no reviews
                ff = reviews[reviews['content'].str.len()>0]
                if len(ff) > 0:
                    reviews.dropna(subset=['content'], inplace=True)
                    reviews['content'] = [[t.lemma_ for t in d if t.pos in [100, 92, 84, 86]] for d in nlp.pipe(reviews['content'])]
                    reviews.to_pickle(fname_to)
    except:
        pass

def worker(th_name,q):
    while True:
        appId = q.get()
        logging.debug('Thread: {}. App: {}. Apps left: {} '.format(th_name, appId, q.qsize()), end='\r')
        MakeLemma(appId)
        #q.task_done()

if __name__ == '__main__':
    q = Queue()
    df = pd.read_csv('Dataset//app_data_processed.csv', usecols=['appId'])
    appIds = list(df['appId'])

    for appId in appIds:
        q.put(appId)

    t1 = Process(target=worker, args=('t1',q,))
    t1.start()
    t2 = Process(target=worker, args=('t2',q,))
    t2.start()
    t3 = Process(target=worker, args=('t3',q,))
    t3.start()
    t4 = Process(target=worker, args=('t4',q,))
    t4.start()
    t5 = Process(target=worker, args=('t5',q,))
    t5.start()
    t6 = Process(target=worker, args=('t6',q,))
    t6.start()
    t7 = Process(target=worker, args=('t7',q,))
    t7.start()
    t8 = Process(target=worker, args=('t8',q,))
    t8.start()
    t9 = Process(target=worker, args=('t9',q,))
    t9.start()
