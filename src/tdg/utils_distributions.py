import numpy as np
def zipf_popularity(n_items:int, exponent:float=1.07):
    ranks = np.arange(1, n_items+1)
    weights = 1 / np.power(ranks, exponent)
    return weights/weights.sum()
