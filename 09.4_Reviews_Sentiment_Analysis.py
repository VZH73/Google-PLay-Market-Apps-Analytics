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
from textblob import TextBlob

from multiprocessing import Pool,current_process,freeze_support

def Get_Single_Sentiment(text):
    b = TextBlob(text)
    return pd.Series([b.sentiment.polarity, b.sentiment.subjectivity])

def Make_Sentiment_Analysis(appId):
    try:
        file_from = ''.join(['ReviewsLemma//', appId, '.pkl'])
        file_to = ''.join(['ReviewsSentiment//', appId, '.pkl'])
        if not os.path.exists(file_to) :
            if os.path.exists(file_from):
                reviewsP = pd.read_pickle(''.join(['ReviewsPKL//', appId, '.pkl']))
                reviewsP.dropna(subset=['content'], inplace=True)
                reviewsL = pd.read_pickle(file_from)

                reviewsL[['polarity', 'subjectivity']] = reviewsP['content'].apply(Get_Single_Sentiment)

                reviewsL.to_pickle()
                print("Process {} Processed {}".format(current_process().name, appId), end="\r")

    except:
        print("Failed on {}".format(appId))

if __name__ == '__main__':
    q = Queue()
    df = pd.read_csv('Dataset//app_data_processed.csv', usecols=['appId'])
    appIds = list(df['appId'])

    freeze_support()
    # Create 10 processes
    pool = Pool(processes=10, )

    # Map them to the list of appIds to process
    pool.map(Make_Sentiment_Analysis, appIds)

    print("Done")
