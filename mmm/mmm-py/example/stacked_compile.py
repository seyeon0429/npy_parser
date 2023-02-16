from mmm.nasdaq import load_actions, preprocess, compile_trajectory_with_volume_level



if __name__ == "__main__":
    preprocess("~/data/nasdaq-itch/raw/S101521-v50.txt.gz", "~/data/nasdaq-itch/prep/")
    actions = load_actions("~/data/nasdaq-itch/prep/S101521-v50/AAPL.bin.zst")
    stack_interval = 50000
    stack_size = 3

    trajs = []
    for i in range(stack_size-1, -1, -1):
        print(i)
        trajs.append(compile_trajectory_with_volume_level(actions, [0,10,100,10000,1000000], 10000 + stack_interval * i, 5))

    stacked_trajectory = []
    for i in range(len(trajs[0])):
        traj_1 = trajs[0]
        stacked_state = [trajs[s][i][3] for s in range(stack_size)]
        stacked_trajectory.append((traj_1[0], traj_1[1], traj_1[2], stacked_state))
    print(stacked_trajectory[-1][3])
