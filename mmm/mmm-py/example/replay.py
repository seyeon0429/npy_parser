from mmm.nasdaq import TimeBasedQueueReplay, TimeBasedVolumeReplay



if __name__ == "__main__":
    replay = TimeBasedVolumeReplay.by_spread("/home/sean/data/nasdaq-itch/prep/S101521-v50/AAPL.bin.zst", 10000)
    # replay = TimeBasedQueueReplay.by_level("/home/sean/data/nasdaq-itch/prep/S101521-v50/AAPL.bin.zst", 5)
    done = False

    i = 0
    while not done:
        (time, state, messages, done) = replay.step()
        if i % 10000 == 0:
            print(i, len(messages))
        i += 1
