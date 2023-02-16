# from mmm.merged_py import merge_file
from mmm.merged_py import VolumeReplay

if __name__=="__main__":
    # v=VolumeReplay("../../../sample/Nasdaq20211004/AAPL.bin.zst",4)
    v=VolumeReplay("../../../sample/Nasdaq20211004/AAPL.bin.zst",4).digitize(100000000)

    for d in v:
        print(d)