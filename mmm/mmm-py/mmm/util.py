from typing import Iterable, List, Union
import numpy as np


def get_subsequents(actions: np.ndarray, seqs: Union[int, List[int]]):
    if not isinstance(seqs, Iterable):
        seqs = [seqs]
    relateds = []
    for seq in seqs:
        while (seq := actions[seq][-1]) != 0:
            relateds.append(seq)
    return np.sort(np.unique(relateds))
