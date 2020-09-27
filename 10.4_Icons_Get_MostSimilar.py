import numpy as np
from collections import defaultdict
import pickle

from multiprocessing import Pool, freeze_support, Manager, Queue
from functools import partial

def Get_Sim_Img(multi_dict, f, k):
    # Find all similar images
    indices = []
    for i in range(len(f)):
        d = np.linalg.norm(f[k] - f[i])
        if d<3000 and d>0:
            indices.append(i)
    if len(indices) > 0 or True:
        multi_dict[k] = indices
        print(len(multi_dict))

if __name__ == '__main__':
    AUTOENCODER_PATH = "ImageSimilarity"
    VER = 3
    FEATURES_PATH = f"{AUTOENCODER_PATH}/features{VER}.pickle"
    SIM_IMG_PATH = f"{AUTOENCODER_PATH}/similar_images{VER}.pickle"

    features_idx = pickle.loads(open(FEATURES_PATH, "rb").read())
    countImgs = int(len(features_idx["name"]))

    pool = Pool(processes=20)
    mgr = Manager()
    multi_d = mgr.dict()

    freeze_support()
    with mgr.Pool(20) as pool:
        pool.map(partial(Get_Sim_Img, multi_d, features_idx["features"]), range(countImgs))

    print(f"Multiprocess finished. Dict len is {len(multi_d)}")

    with open(SIM_IMG_PATH, "wb") as f:
        f.write(pickle.dumps(multi_d.copy()))

    #for k,v in multi_d.items():
    #    print(f"Key - {k}; value - {v}")
    print("Done")