from typing import Iterable, List, Union
import zstandard
from pathlib import Path
import numpy as np
import json
from mmm.nyse_py import TimeBasedQueueReplay, TimeBasedVolumeReplay, create_trajectory_summaries


NUM_FEATURES = 9

def wrapper(func):
    def wrapped(actions: np.ndarray, target_indices: List[int],  latencies_ns: Union[List[int], int],  depth: int,is_inclusive: bool):
        assert actions.shape[1] == NUM_FEATURES
        assert np.all(np.array(latencies_ns) >= 0)
        assert depth > 0

        target_indices = np.array(target_indices)
        target_indices = np.where(target_indices >= 0, target_indices, len(actions) - target_indices)
        # print(target_indices)
        assert not np.any(np.diff(target_indices) < 0), "`target indicies` should be increase monotonically"

        if isinstance(latencies_ns, Iterable):
            assert len(target_indices) == len(latencies_ns)
            latencies_ns = np.array(latencies_ns)
        else:
            latencies_ns = np.repeat(latencies_ns, len(target_indices))
        res = func(actions, target_indices, latencies_ns, depth, False, is_inclusive)
        assert len(res) == len(target_indices)
        return res
    return wrapped


def compile_trajectory_with_volume_level(actions: np.ndarray, target_indices: List[int],  latencies_ns: Union[List[int], int],  level: int, is_inclusive = False):
    from mmm.nyse_py import compile_trajectory_with_volume_level
    return wrapper(compile_trajectory_with_volume_level)(actions, target_indices, latencies_ns, level, is_inclusive)

def compile_trajectory_with_volume_spread(actions: np.ndarray, target_indices: List[int],  latencies_ns: Union[List[int], int],  spread: int, is_inclusive = False):
    from mmm.nyse_py import compile_trajectory_with_volume_spread
    return wrapper(compile_trajectory_with_volume_spread)(actions, target_indices, latencies_ns, spread, is_inclusive)

def compile_trajectory_with_queue_level(actions: np.ndarray, target_indices: List[int],  latencies_ns: Union[List[int], int],  level: int, is_inclusive = False):
    from mmm.nyse_py import compile_trajectory_with_queue_level
    return wrapper(compile_trajectory_with_queue_level)(actions, target_indices, latencies_ns, level, is_inclusive)

def compile_trajectory_with_queue_spread(actions: np.ndarray, target_indices: List[int],  latencies_ns: Union[List[int], int],  spread: int, is_inclusive = False):
    from mmm.nyse_py import compile_trajectory_with_queue_spread
    return wrapper(compile_trajectory_with_queue_spread)(actions, target_indices, latencies_ns, spread, is_inclusive)

# def preprocess(source_file: Path, out_dir: Path):
#     from mmm.nyse_py import preprocess
#     preprocess(str(Path(source_file).expanduser()), str(Path(out_dir).expanduser()))

def preprocess_meta(source_file: Path, out_dir: Path):
    from mmm.nyse_py import preprocess_meta
    preprocess_meta(str(Path(source_file).expanduser()), str(Path(out_dir).expanduser()))

def preprocess(source_file_list: List[Path], out_dir: Path):
    from mmm.nyse_py import preprocess
    preprocess(source_file_list,str(Path(out_dir).expanduser()))

def load_actions(path: Path):
    return np.frombuffer(zstandard.decompress(Path(path).expanduser().read_bytes()), dtype=np.uint64).reshape((-1, NUM_FEATURES))

def load_bbo(path: Path):
    return np.frombuffer(zstandard.decompress(Path(path).expanduser().read_bytes()), dtype=np.int64).reshape((-1, 2))

def load_json_zst(path: Path):
    return json.loads(zstandard.decompress(Path(path).expanduser().read_bytes()))

if __name__ == "__main__":
    actions = load_actions(Path("../../../sample/Nyse20211004/AAPL.bin.zst"))
    for i in range(len(actions)):
        print(actions[i])
