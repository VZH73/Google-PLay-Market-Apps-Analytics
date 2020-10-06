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
import spacy
spacy.require_gpu()
nlp = spacy.load("en_core_web_sm")

from multiprocessing import Process, Queue
import logging

def MakeLemma(app_Id):
    try:
        file_from = '{}\\{:02d}_ReviewsPKL\\{}.pkl'.format(Data_Folder,13,app_Id)
        file_to = '{}\\{:02d}_ReviewsLemma\\{}.pkl'.format(Data_Folder,18,app_Id)
        if os.path.exists(file_from):
            if not os.path.exists(file_to):
                reviews = pd.read_pickle(file_from)

                # Skip apps with no reviews
                ff = reviews[reviews['content'].str.len()>0]
                if len(ff) > 0:
                    reviews.dropna(subset=['content'], inplace=True)
                    reviews['content'] = [[t.lemma_ for t in d if t.pos in [100, 92, 84, 86]] for d in nlp.pipe(reviews['content'])]
                    reviews.to_pickle(file_to)
    except:
        pass

def worker(th_name,q):
    while True:
        app_Id = q.get()
        logging.debug('Thread: {}. App: {}. Apps left: {} '.format(th_name, app_Id, q.qsize()), end='\r')
        MakeLemma(app_Id)
        #q.task_done()

if __name__ == '__main__':
    Data_Folder = 'Data'

    q = Queue()
    df = pd.read_csv('{}\\{:02d}_app_data_processed.csv'.format(Data_Folder,8), usecols=['appId'])
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
