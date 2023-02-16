from mmm.nasdaq import load_actions, preprocess, compile_trajectory_with_volume_level, preprocess_meta, load_json_zst
from mmm.util import get_subsequents
from pprint import pprint



if __name__ == "__main__":
    #preprocess("./test_data/S100121-v50.txt.gz", "./test_data")
    #print(load_json_zst("~/data/nasdaq-itch/prep/S101521-v50/market_stats.json.zst")["AAPL"])

    actions = load_actions("./data/S010322-v50/AAPL.bin.zst")
    traj = compile_trajectory_with_volume_level(actions, [0,actions.shape[0]-2, actions.shape[0]-1], [0,0,0], 10, False)
    traj_full = compile_trajectory_with_volume_level(actions, [0,actions.shape[0]-2, actions.shape[0]-1], [0,0,0], 10, True)
    print(traj)
    print("-------------------------------")
    print(traj_full)
    # # trajectory = compile_trajectory_with_volume_spread(actions, [0,10,100,10000,1000000], [0,10,100,1000,10000], 50000)
    # # trajectory = compile_trajectory_with_queue_level(actions, [0,10,100,10000,1000000], [0,10,100,1000,10000], 5)
    # # trajectory = compile_trajectory_with_queue_spread(actions, [0,10,100,10000,1000000], [0,10,100,1000,10000], 50000)
    # pprint(trajectory[-1]);
