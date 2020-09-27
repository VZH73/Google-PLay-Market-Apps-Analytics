# In this file we will run through the list of all image features that we've got from the trained encoder
# We will compare every pair of different images for the proximity of their image features
# In other words we will collect the value that tells how similar are two images for every possible pair
# Then, we will store in a dictionary how frequent is that distance among other pairs.
# We need that to understand the threshold that will measure if images are very similar
# and could be treated as a style duplicates.
import numpy as np
from collections import defaultdict
import pickle

from multiprocessing import Pool, freeze_support, Manager, Queue
from functools import partial

def Get_Freq(multi_dict, f, k):
    freq = defaultdict(int)
    for i in range(k+1, len(f)):
        d = int(np.linalg.norm(f[k] - f[i])/10)*10
        freq[d]+=1

    multi_dict.append(freq)
    print(len(multi_dict))

if __name__ == '__main__':
    AUTOENCODER_PATH = "ImageSimilarity"
    VER = 3
    FEATURES_PATH = f"{AUTOENCODER_PATH}/features{VER}.pickle"
    FREQ_PATH = f"{AUTOENCODER_PATH}/frequencies{VER}.pickle"

    features_idx = pickle.loads(open(FEATURES_PATH, "rb").read())
    countImgs = int(len(features_idx["name"]))

    pool = Pool(processes=20)
    mgr = Manager()
    multi_d = mgr.list()

    freeze_support()
    with mgr.Pool(20) as pool:
        pool.map(partial(Get_Freq, multi_d, features_idx["features"]), range(countImgs))

    print("Multiprocess finished")

    proximity_freq = defaultdict(int)
    for i in range(len(multi_d)):
        for k,v in multi_d[i].items():
            proximity_freq[k] += v
    print("Biggus diccus is ready")

    with open(FREQ_PATH, "wb") as f:
        f.write(pickle.dumps(proximity_freq))

    print("Done")

