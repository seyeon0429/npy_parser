from mmm.nasdaq import preprocess as preprocess_nasdaq
from mmm.nasdaq import load_json_zst as load_json_zst_nasdaq
from mmm.nyse import preprocess as preprocess_nyse
from mmm.nyse import load_json_zst as load_json_zst_nyse
from pprint import pprint
import numpy as np
import os

if __name__ == "__main__":
    ## ITCH
    preprocess_nasdaq("./test_data/S100121-v50.txt.gz", "./test_data")
    stats_all = load_json_zst_nasdaq("./test_data/S100121-v50/market_stats.json.zst")
    stats_AAPL = load_json_zst_nasdaq("./test_data/S100121-v50/AAPL.json.zst")
    print(stats_all["AAPL"])
    print(stats_AAPL)

    ## TAQ
    taq_data_channel = sorted([f"./test_data/" + x for x in os.listdir(f"./test_data/")
                               if (".gz" in x) and ("EQY_US_ARCA_IBF_" in x)])
    # partition data by date
    taq_data = {}
    for filename in taq_data_channel:
        date = filename.split('_')[-1].split('.')[0]
        if date not in taq_data.keys():
            taq_data[date] = []
        taq_data[date].append(filename)
    print(taq_data)
    for date, paths in taq_data.items():
        preprocess_nyse(paths, "./test_data")

    stats_all = load_json_zst_nyse("./test_data/EQY_US_ARCA_IBF_20211004/market_stats.json.zst")
    stats_AAPL = load_json_zst_nyse("./test_data/EQY_US_ARCA_IBF_20211004/AAPL.json.zst")
    print(stats_AAPL)

    #for key, item in stats.items():
    #    if key == "FCUV":
    #        print(key)
    #        print(np.array(stats[key]["interval_lp_volume"]))
    # actions = load_actions("~/data/nasdaq-itch/prep/S101521-v50/AAPL.bin.zst")
    # print(len(actions))
    # print(actions[0])
    # print(get_subsequents(actions, 0))
    # trajectory = compile_trajectory_with_volume_level(actions, [0,10,100,10000,1000000], [0,10,100,1000,10000], 5)
    # # trajectory = compile_trajectory_with_volume_spread(actions, [0,10,100,10000,1000000], [0,10,100,1000,10000], 50000)
    # # trajectory = compile_trajectory_with_queue_level(actions, [0,10,100,10000,1000000], [0,10,100,1000,10000], 5)
    # # trajectory = compile_trajectory_with_queue_spread(actions, [0,10,100,10000,1000000], [0,10,100,1000,10000], 50000)
    # pprint(trajectory[-1]);
