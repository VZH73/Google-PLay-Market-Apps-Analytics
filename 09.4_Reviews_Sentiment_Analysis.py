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

def Make_Sentiment_Analysis(app_Id):
    try:
        file_from = '{}\\{:02d}_ReviewsLemma\\{}.pkl'.format(Data_Folder,18,app_Id)
        file_to = '{}\\{:02d}ReviewsSentiment\\{}.pkl'.format(Data_Folder,19,app_Id)
        if not os.path.exists(file_to) :
            if os.path.exists(file_from):
                reviewsP = pd.read_pickle('{}\\{:02d}_ReviewsPKL\\{}.pkl'.format(Data_Folder,13,app_Id))
                reviewsP.dropna(subset=['content'], inplace=True)
                reviewsL = pd.read_pickle(file_from)

                reviewsL[['polarity', 'subjectivity']] = reviewsP['content'].apply(Get_Single_Sentiment)

                reviewsL.to_pickle()
                print("Process {} Processed {}".format(current_process().name, app_Id), end="\r")

    except:
        print("Failed on {}".format(app_Id))

if __name__ == '__main__':
    Data_Folder = 'Data'
    q = Queue()
    df = pd.read_csv('{}\\{:02d}_app_data_processed.csv'.format(Data_Folder,8), usecols=['appId'])
    appIds = list(df['appId'])

    freeze_support()
    # Create 10 processes
    pool = Pool(processes=10, )

    # Map them to the list of appIds to process
    pool.map(Make_Sentiment_Analysis, appIds)

    print("Done")
