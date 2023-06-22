
from src.constants import REGMKT_END_TIME_NS, REGMKT_START_TIME_NS, INTERVAL_SEC
from src.constants import T_N, R_N, A_N, TICKER_SYM, DATE_FROM_PATH, START_TIME_NS, END_TIME_NS, INTERVAL_NS

from collections import namedtuple
from logging import raiseExceptions
import os
import pandas as pd
import numpy as np

from src.lib import Exchange

import time
from datetime import datetime, timedelta
import copy
# from utils import normalize_max



"""
All half days
"""
half_days = [
    "112621"
]

"""
List of all FOMC meetings dates
"""
fomc_days = []

def normalize_max(x,p):
    '''
    Normalize with respect to max of the first p mins (ex. first 5 mins)
    '''
    max = np.amax(x[:p])
    x_normed = x/max
    return x_normed

def filter_regmkt(data, start, end, time_col):
    # print(f'in filter_regmkt, {data.iloc[:, time_col]}')
    if time_col == -1:
        data = data.to_numpy(dtype=np.float64)
        data = data[start:end, :]
        return data
    else:
        data = data[data.iloc[:, time_col] >= start]
        data = data[data.iloc[:, time_col] < end]
        data = data.to_numpy(dtype=np.float64)
        return data

def rolling_window(a, window):
    shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
    strides = a.strides + (a.strides[-1],)
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

def ffill_zeros(np_array):
    df = pd.DataFrame(np_array)
    df.replace(to_replace=0, method='ffill', inplace=True)
    return np.squeeze(df.values)

def ffill_zeros_ohlc(open_np, high_np, low_np, close_np):
    open_np, high_np, low_np, close_np = np.expand_dims(open_np, 1), np.expand_dims(high_np, 1), np.expand_dims(low_np, 1), np.expand_dims(close_np, 1)
    df = pd.DataFrame(np.concatenate([open_np, high_np, low_np, close_np], axis=1), columns=["open", "high", "low", "close"])
    df['close'].replace(to_replace=0, method='ffill', inplace=True)
    df['open'] = np.where(df['open'] == 0, df['close'], df['open'])
    df['high'] = np.where(df['high'] == 0, df['close'], df['high'])
    df['low'] = np.where(df['low'] == 0, df['close'], df['low'])
    
    df['open'].replace(to_replace=0, method='bfill', inplace=True) ## front-fill & back-fill to make sure the first row is non-zero
    df['high'] = np.where(df['high'] == 0, df['open'], df['high'])
    df['low'] = np.where(df['low'] == 0, df['open'], df['low'])
    df['close'] = np.where(df['close'] == 0, df['open'], df['close'])
    
    return df['open'].values, df['high'].values, df['low'].values, df['close'].values

class HistoricalData(object):
    def __init__(self):
        print("Make sure to preprocess everything before running this code...")
        self.csv_dir = "./csv"
        

        self.T = T_N                        # number of intervals
        self.D = len(self.data_dirs)        # number of days
        self.N = len(self.securities)          # number of securities - only one for now

        print(f"time interval(T): {self.T}")
        print(f"number of days(D): {self.D}")
        print(f"number of securities(N): {self.N}")
        
        self.stats_map = {}
        self.subject_map = []

        self.reg_mkt_start = int((9.5-4)*3600)
        self.reg_mkt_end = int((16-4)*3600)

    def save_premkt_npy(self):
        # subject_map = []
        ohlcvt_atr_count = 0

        for s, sym in enumerate(self.data_dirs):

            for d, save_date in enumerate(self.data_dirs[sym]):
                # save_date = S081222-v50
                start = time.time()

                lob_dir = self.data_dirs[sym][save_date][0]
                bidask_dir = self.data_dirs[sym][save_date][1]
                ohlc_dir = self.data_dirs[sym][save_date][2]
                midpoint_dir = self.data_dirs[sym][save_date][3]

                print(f'\nLOB path: {lob_dir}')
                print(f'BIDASK path: {bidask_dir}')
                print(f'OHLCVMT path: {ohlc_dir}')
                print(f'MIDPOINT path: {midpoint_dir}')

                # date = data_dir.split('/')[-2]
                # nasdaq_path = data_dir.split('/')[-3]
                print(f"Loading {save_date} LOB and OHLCMV: {d} / {len(self.data_dirs[sym])} ...")

                folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/npy_ibkr_premkt_parsed_data', save_date)
                # folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/npy_ibkr_premkt_parsed_data', save_date)
                if not os.path.exists(folder_dir):
                    os.makedirs(folder_dir)
                
                # ## For each Securities
                skip_ohlcvt_atr = False

                save_dir_ohlcvt_atr = os.path.join(folder_dir, f'input_{sym}_OHLCVT_ATR_premkt')

                if os.path.exists(save_dir_ohlcvt_atr+'.npy'):
                    print('\_OHLCVT_ATR_premkt file exists!')
                    skip_ohlcvt_atr = True
                
                if skip_ohlcvt_atr:
                    print('\tSkipping as all files exist!\n')
                    continue

                sym_stats_map_tmp = {}

                midpoint_data_col = -1

                ohlcvt_index = [-1, -1, -1, -1, -1, -1, -1]
                if save_date in ['S081722-v50', 'S082922-v50', 'S083022-v50']:
                    ohlcvt_index = [0, 1, 2, 3, 5, 6, 7]
                    midpoint_data_col = 4
                elif save_date in ['S083122-v50']:
                    ohlcvt_index = [3, 4, 5, 6, 8, 9, 10]
                    midpoint_data_col = 7
                else:
                    ohlcvt_index = [5, 6, 7, 8, 9, 10, 11]

                lob_time_index_col = -1
                if save_date in ['S081722-v50']:
                    lob_time_index_col = -1
                else:
                    lob_time_index_col = 0
                
                ba_bb_col = [-1, -1]
                if save_date in ['S081722-v50']:
                    ba_bb_col = [0, 2]

                else:
                    ba_bb_col = [3, 5]


                ohlc_data = pd.read_csv(ohlc_dir)

                if np.where(np.sum(ohlc_data.to_numpy()[:,5:8],axis=1)==0)[0][-1] + 1 > 19800-7200-1:
                    print("### Not enough premkt data ###")
                    continue

                # ohlc_data = ohlc_data.to_numpy(dtype=np.float64)
                # ohlc_data = ohlc_data[self.reg_mkt_start-1:self.reg_mkt_end-1,:]
                ohlc_data = filter_regmkt(ohlc_data, 19800-7200, 19800, ohlcvt_index[-1])
                # print(f'ohlc_data shape: {np.shape(ohlc_data)}')
                interval_midpoint = None
                if midpoint_data_col != -1:
                    interval_midpoint = ohlc_data[:, midpoint_data_col]
                elif midpoint_dir is not None:
                    interval_midpoint = pd.read_csv(midpoint_dir)
                    interval_midpoint = filter_regmkt(interval_midpoint, 19800-7200, 19800, 0)
                    interval_midpoint = interval_midpoint[:, 5]
                else:
                    print(f'interval_midpoint is None!')
                    raise NotImplementedError()

                if os.path.exists(bidask_dir):
                    print(f'Using BIDASK ...')
                    bidask_data = pd.read_csv(bidask_dir)
                    bidask_data = filter_regmkt(bidask_data, 19800-7200, 19800, 0)
                    interval_best_ask_price = bidask_data[:, 5]
                    interval_best_bid_price = bidask_data[:, 7]
                else:
                    print(f'Using LOB ...')
                    lob_data = pd.read_csv(lob_dir)
                    lob_data = filter_regmkt(lob_data, 19800-7200, 19800, lob_time_index_col)
                    interval_best_ask_price = lob_data[:, ba_bb_col[0]]
                    interval_best_bid_price = lob_data[:, ba_bb_col[1]]
                


                interval_open = ohlc_data[:,ohlcvt_index[0]]
                interval_high = ohlc_data[:,ohlcvt_index[1]]
                interval_low = ohlc_data[:,ohlcvt_index[2]]
                interval_close = ohlc_data[:,ohlcvt_index[3]]
                interval_bid_execute_volume = ohlc_data[:,ohlcvt_index[4]]
                interval_ask_execute_volume = ohlc_data[:,ohlcvt_index[5]]

                # interval_best_ask_price = lob_data[:, ba_bb_col[0]]
                # interval_best_bid_price = lob_data[:, ba_bb_col[1]]

                interval_best_ask_price = ffill_zeros(interval_best_ask_price)
                interval_best_bid_price = ffill_zeros(interval_best_bid_price)
                
                interval_lob_midprice = (interval_best_ask_price + interval_best_bid_price) / 2

                ### ATR Calculation
                high_low = np.concatenate([np.array([0]), interval_high[1:] - interval_low[1:]])
                high_cp = np.abs(np.concatenate([np.array([0]), interval_high[1:] - interval_close[:-1]]))
                low_cp = np.abs(np.concatenate([np.array([0]), interval_low[1:] - interval_close[:-1]]))

                true_range = np.maximum(high_low, high_cp, low_cp)
                interval_atr = np.concatenate([np.zeros(14), np.mean(rolling_window(true_range, 14), 1)[1:]])

                ## OHLCVT_ATR
                if not skip_ohlcvt_atr:
                    data = np.concatenate(([
                        ## INTERVAL OHLC
                        np.expand_dims(interval_open, 1),
                        np.expand_dims(interval_high, 1),
                        np.expand_dims(interval_low, 1),
                        np.expand_dims(interval_close, 1),

                        ## OTHERS
                        np.expand_dims(interval_ask_execute_volume, 1),
                        np.expand_dims(interval_bid_execute_volume, 1),

                        np.expand_dims(interval_best_ask_price, 1),
                        np.expand_dims(interval_best_bid_price, 1),
                        np.expand_dims(interval_midpoint, 1),

                        ## ATR
                        np.expand_dims(interval_atr, 1),

                        ## TIME INDEX
                        np.expand_dims(np.arange(0-7200, 0), 1),
                    ]), axis=1)
                
                    print(f'\tIBKR PREMKT OHLCVT_ATR: Saving the data shape of {sym} as : {np.shape(data)}, data type: {type(data[0, 0])}')
                    np.save(save_dir_ohlcvt_atr+'.npy', data, allow_pickle=True)
                    ohlcvt_atr_count += 1

                end = time.time()
                #print(f"Time elapsed for calculating volume/lob (with mean/std...) of 1 day: {end - start}\n")
                print(f"Time elapsed for 1 day: {end - start:.3f} seconds\n")

        print(f'\n\nCreated {ohlcvt_atr_count} _OHLCVT_ATR_premkt npy files.\n\n')

    def save_npy(self):
        # subject_map = []
        ohlcvt_atr_count = 0

        for s, sym in enumerate(self.data_dirs):

            for d, save_date in enumerate(self.data_dirs[sym]):
                # save_date = S081222-v50
                start = time.time()

                lob_dir = self.data_dirs[sym][save_date][0]
                bidask_dir = self.data_dirs[sym][save_date][1]
                ohlc_dir = self.data_dirs[sym][save_date][2]
                midpoint_dir = self.data_dirs[sym][save_date][3]

                print(f'\nLOB path: {lob_dir}')
                print(f'BIDASK path: {bidask_dir}')
                print(f'OHLCVMT path: {ohlc_dir}')
                print(f'MIDPOINT path: {midpoint_dir}')

                # date = data_dir.split('/')[-2]
                # nasdaq_path = data_dir.split('/')[-3]
                print(f"Loading {save_date} LOB and OHLCMV: {d} / {len(self.data_dirs[sym])} ...")

                folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/npy_ibkr_parsed_data', save_date)
                # folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/npy_ibkr_parsed_data', save_date)
                if not os.path.exists(folder_dir):
                    os.makedirs(folder_dir)
                
                # ## For each Securities
                skip_ohlcvt_atr = False

                save_dir_ohlcvt_atr = os.path.join(folder_dir, f'input_{sym}_OHLCVT_ATR')

                if os.path.exists(save_dir_ohlcvt_atr+'.npy'):
                    print('\tOHLCVT_ATR file exists!')
                    skip_ohlcvt_atr = True
                
                if skip_ohlcvt_atr:
                    print('\tSkipping as all files exist!\n')
                    continue

                sym_stats_map_tmp = {}

                midpoint_data_col = -1

                ohlcvt_index = [-1, -1, -1, -1, -1, -1, -1]
                if save_date in ['S081722-v50', 'S082922-v50', 'S083022-v50']:
                    ohlcvt_index = [0, 1, 2, 3, 5, 6, 7]
                    midpoint_data_col = 4
                elif save_date in ['S083122-v50']:
                    ohlcvt_index = [3, 4, 5, 6, 8, 9, 10]
                    midpoint_data_col = 7
                else:
                    ohlcvt_index = [5, 6, 7, 8, 9, 10, 11]

                lob_time_index_col = -1
                if save_date in ['S081722-v50']:
                    lob_time_index_col = -1
                else:
                    lob_time_index_col = 0
                
                ba_bb_col = [-1, -1]
                if save_date in ['S081722-v50']:
                    ba_bb_col = [0, 2]

                else:
                    ba_bb_col = [3, 5]


                ohlc_data = pd.read_csv(ohlc_dir)
                # ohlc_data = ohlc_data.to_numpy(dtype=np.float64)
                # ohlc_data = ohlc_data[self.reg_mkt_start-1:self.reg_mkt_end-1,:]
                ohlc_data = filter_regmkt(ohlc_data, 19800, 43200, ohlcvt_index[-1])
                # print(f'ohlc_data shape: {np.shape(ohlc_data)}')
                interval_midpoint = None
                if midpoint_data_col != -1:
                    interval_midpoint = ohlc_data[:, midpoint_data_col]
                elif midpoint_dir is not None:
                    interval_midpoint = pd.read_csv(midpoint_dir)
                    interval_midpoint = filter_regmkt(interval_midpoint, 19800, 43200, 0)
                    interval_midpoint = interval_midpoint[:, 5]
                else:
                    print(f'interval_midpoint is None!')
                    raise NotImplementedError()

                if os.path.exists(bidask_dir):
                    print(f'Using BIDASK ...')
                    bidask_data = pd.read_csv(bidask_dir)
                    bidask_data = filter_regmkt(bidask_data, 19800, 43200, 0)
                    interval_best_ask_price = bidask_data[:, 5]
                    interval_best_bid_price = bidask_data[:, 7]
                else:
                    print(f'Using LOB ...')
                    lob_data = pd.read_csv(lob_dir)
                    lob_data = filter_regmkt(lob_data, 19800, 43200, lob_time_index_col)
                    interval_best_ask_price = lob_data[:, ba_bb_col[0]]
                    interval_best_bid_price = lob_data[:, ba_bb_col[1]]

                interval_open = ohlc_data[:,ohlcvt_index[0]]
                interval_high = ohlc_data[:,ohlcvt_index[1]]
                interval_low = ohlc_data[:,ohlcvt_index[2]]
                interval_close = ohlc_data[:,ohlcvt_index[3]]
                interval_bid_execute_volume = ohlc_data[:,ohlcvt_index[4]]
                interval_ask_execute_volume = ohlc_data[:,ohlcvt_index[5]]

                interval_best_ask_price = ffill_zeros(interval_best_ask_price)
                interval_best_bid_price = ffill_zeros(interval_best_bid_price)
                
                interval_lob_midprice = (interval_best_ask_price + interval_best_bid_price) / 2
                
                assert (interval_open != 0).all()
                assert (interval_high != 0).all()
                assert (interval_low != 0).all()
                assert (interval_close != 0).all()
                
                assert (interval_best_ask_price != 0).all()
                assert (interval_best_bid_price != 0).all()
                
                assert (interval_lob_midprice != 0).all()

                ### ATR Calculation
                high_low = np.concatenate([np.array([0]), interval_high[1:] - interval_low[1:]])
                high_cp = np.abs(np.concatenate([np.array([0]), interval_high[1:] - interval_close[:-1]]))
                low_cp = np.abs(np.concatenate([np.array([0]), interval_low[1:] - interval_close[:-1]]))

                true_range = np.maximum(high_low, high_cp, low_cp)
                interval_atr = np.concatenate([np.zeros(14), np.mean(rolling_window(true_range, 14), 1)[1:]])

                ## OHLCVT_ATR
                if not skip_ohlcvt_atr:
                    data = np.concatenate(([
                        ## INTERVAL OHLC
                        np.expand_dims(interval_open, 1),
                        np.expand_dims(interval_high, 1),
                        np.expand_dims(interval_low, 1),
                        np.expand_dims(interval_close, 1),

                        ## OTHERS
                        np.expand_dims(interval_ask_execute_volume, 1),
                        np.expand_dims(interval_bid_execute_volume, 1),

                        np.expand_dims(interval_best_ask_price, 1),
                        np.expand_dims(interval_best_bid_price, 1),
                        np.expand_dims(interval_midpoint, 1),

                        ## ATR
                        np.expand_dims(interval_atr, 1),

                        ## TIME INDEX
                        np.expand_dims(np.arange(0, 23400), 1),
                    ]), axis=1)
                
                    print(f'\tIBKR OHLCVT_ATR: Saving the data shape of {sym} as : {np.shape(data)}, data type: {type(data[0, 0])}')
                    np.save(save_dir_ohlcvt_atr+'.npy', data, allow_pickle=True)
                    ohlcvt_atr_count += 1

                end = time.time()
                #print(f"Time elapsed for calculating volume/lob (with mean/std...) of 1 day: {end - start}\n")
                print(f"Time elapsed for 1 day: {end - start:.3f} seconds\n")

        print(f'\n\nCreated {ohlcvt_atr_count} OHLCVT_ATR npy files.\n\n')

    def save_npy_bollinger(self):
        # subject_map = []
        ohlcvt_atr_count = 0

        sma_window = 5400
        std_mp_constant = 2.5

        for s, sym in enumerate(self.data_dirs):

            for d, save_date in enumerate(self.data_dirs[sym]):
                # save_date = S081222-v50
                start = time.time()

                lob_dir = self.data_dirs[sym][save_date][0]
                bidask_dir = self.data_dirs[sym][save_date][1]
                ohlc_dir = self.data_dirs[sym][save_date][2]
                midpoint_dir = self.data_dirs[sym][save_date][3]

                # print(f'\nLOB path: {lob_dir}')
                # print(f'BIDASK path: {bidask_dir}')
                # print(f'OHLCVMT path: {ohlc_dir}')
                # print(f'MIDPOINT path: {midpoint_dir}')

                # date = data_dir.split('/')[-2]
                # nasdaq_path = data_dir.split('/')[-3]
                print(f"Loading {save_date} LOB and OHLCMV: {d} / {len(self.data_dirs[sym])} ...")

                folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/npy_ibkr_parsed_data', save_date)
                # folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/npy_ibkr_parsed_data', save_date)
                if not os.path.exists(folder_dir):
                    os.makedirs(folder_dir)
                
                # ## For each Securities
                skip_ohlcvt_atr = False

                save_dir_ohlcvt_atr = os.path.join(folder_dir, f'input_{sym}_OHLCVT_ATR_bollinger_{sma_window}_{std_mp_constant*10}')

                if os.path.exists(save_dir_ohlcvt_atr+'.npy'):
                    print(f'\t_OHLCVT_ATR_bollinger_{sma_window}_{std_mp_constant} file exists!')
                    skip_ohlcvt_atr = True
                
                if skip_ohlcvt_atr:
                    print('\tSkipping as all files exist!\n')
                    continue

                sym_stats_map_tmp = {}

                midpoint_data_col = -1

                ohlcvt_index = [-1, -1, -1, -1, -1, -1, -1]
                if save_date in ['S081722-v50', 'S082922-v50', 'S083022-v50']:
                    ohlcvt_index = [0, 1, 2, 3, 5, 6, 7]
                    midpoint_data_col = 4
                elif save_date in ['S083122-v50']:
                    ohlcvt_index = [3, 4, 5, 6, 8, 9, 10]
                    midpoint_data_col = 7
                else:
                    ohlcvt_index = [5, 6, 7, 8, 9, 10, 11]

                lob_time_index_col = -1
                if save_date in ['S081722-v50']:
                    lob_time_index_col = -1
                else:
                    lob_time_index_col = 0
                
                ba_bb_col = [-1, -1]
                if save_date in ['S081722-v50']:
                    ba_bb_col = [0, 2]

                else:
                    ba_bb_col = [3, 5]


                ohlc_data = pd.read_csv(ohlc_dir)
                # ohlc_data = ohlc_data.to_numpy(dtype=np.float64)
                # ohlc_data = ohlc_data[self.reg_mkt_start-1:self.reg_mkt_end-1,:]
                ohlc_data = filter_regmkt(ohlc_data, 19800, 43200, ohlcvt_index[-1])
                # print(f'ohlc_data shape: {np.shape(ohlc_data)}')
                interval_midpoint = None
                if midpoint_data_col != -1:
                    interval_midpoint = ohlc_data[:, midpoint_data_col]
                elif midpoint_dir is not None:
                    interval_midpoint = pd.read_csv(midpoint_dir)
                    interval_midpoint = filter_regmkt(interval_midpoint, 19800, 43200, 0)
                    interval_midpoint = interval_midpoint[:, 5]
                else:
                    print(f'interval_midpoint is None!')
                    raise NotImplementedError()

                if os.path.exists(bidask_dir):
                    print(f'Using BIDASK ...')
                    bidask_data = pd.read_csv(bidask_dir)
                    bidask_data = filter_regmkt(bidask_data, 19800, 43200, 0)
                    interval_best_ask_price = bidask_data[:, 5]
                    interval_best_bid_price = bidask_data[:, 7]
                else:
                    print(f'Using LOB ...')
                    lob_data = pd.read_csv(lob_dir)
                    lob_data = filter_regmkt(lob_data, 19800, 43200, lob_time_index_col)
                    interval_best_ask_price = lob_data[:, ba_bb_col[0]]
                    interval_best_bid_price = lob_data[:, ba_bb_col[1]]

                interval_open = ohlc_data[:,ohlcvt_index[0]]
                interval_high = ohlc_data[:,ohlcvt_index[1]]
                interval_low = ohlc_data[:,ohlcvt_index[2]]
                interval_close = ohlc_data[:,ohlcvt_index[3]]
                interval_bid_execute_volume = ohlc_data[:,ohlcvt_index[4]]
                interval_ask_execute_volume = ohlc_data[:,ohlcvt_index[5]]

                

                interval_best_ask_price = ffill_zeros(interval_best_ask_price)
                interval_best_bid_price = ffill_zeros(interval_best_bid_price)
                
                interval_lob_midprice = (interval_best_ask_price + interval_best_bid_price) / 2

                ### ATR Calculation
                high_low = np.concatenate([np.array([0]), interval_high[1:] - interval_low[1:]])
                high_cp = np.abs(np.concatenate([np.array([0]), interval_high[1:] - interval_close[:-1]]))
                low_cp = np.abs(np.concatenate([np.array([0]), interval_low[1:] - interval_close[:-1]]))

                true_range = np.maximum(high_low, high_cp, low_cp)
                interval_atr = np.concatenate([np.zeros(14), np.mean(rolling_window(true_range, 14), 1)[1:]])

                ### Bollinger Calculations
                typical_price = (interval_high + interval_low + interval_close) / 3
                typical_price_window = rolling_window(typical_price, sma_window)[:-1,:]

                sma = np.squeeze(np.mean(typical_price_window, axis=1))
                std = np.squeeze(np.std(typical_price_window, axis=1))

                upper_bounds = sma + std_mp_constant * std
                lower_bounds = sma - std_mp_constant * std

                upper_bounds = np.expand_dims(np.concatenate([np.zeros(sma_window), upper_bounds]),axis=-1)
                lower_bounds = np.expand_dims(np.concatenate([np.zeros(sma_window), lower_bounds]),axis=-1)

                ## OHLCVT_ATR
                if not skip_ohlcvt_atr:
                    data = np.concatenate(([
                        ## INTERVAL OHLC
                        np.expand_dims(interval_open, 1),
                        np.expand_dims(interval_high, 1),
                        np.expand_dims(interval_low, 1),
                        np.expand_dims(interval_close, 1),

                        ## OTHERS
                        np.expand_dims(interval_ask_execute_volume, 1),
                        np.expand_dims(interval_bid_execute_volume, 1),

                        np.expand_dims(interval_best_ask_price, 1),
                        np.expand_dims(interval_best_bid_price, 1),
                        np.expand_dims(interval_midpoint, 1),

                        ## ATR
                        np.expand_dims(interval_atr, 1),

                        ## Bollinger
                        upper_bounds,
                        lower_bounds,

                        ## TIME INDEX
                        np.expand_dims(np.arange(0, 23400), 1),
                    ]), axis=1)
                
                    print(f'\tIBKR OHLCVT_ATR BOLLINGER: Saving the data shape of {sym} as : {np.shape(data)}, data type: {type(data[0, 0])}')
                    np.save(save_dir_ohlcvt_atr+'.npy', data, allow_pickle=True)
                    ohlcvt_atr_count += 1

                end = time.time()
                #print(f"Time elapsed for calculating volume/lob (with mean/std...) of 1 day: {end - start}\n")
                print(f"Time elapsed for 1 day: {end - start:.3f} seconds\n")

        print(f'\n\nCreated {ohlcvt_atr_count} OHLCVT_ATR npy files.\n\n')

class NasdaqData(HistoricalData):
    def __init__(self, start_date, end_date, sym):
        self.exchange = Exchange.NASDAQ
        # self.lob_data_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/to_parse/'
        # self.ohlcvt_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/shared/'
        # self.bidask_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/shared/'
        self.lob_data_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/to_parse/'
        self.ohlcvt_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/shared/'
        self.bidask_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/shared/'
        self.start_date_str = start_date
        self.end_date_str = end_date
        if "_" in sym:
            self.securities = sym.split("_")
        else:
            self.securities = [sym]
        # self.securities = self.get_securities()
        self.data_dirs = self.get_data_dirs()

        super().__init__()
        print(f"total data_dirs: {self.data_dirs}")

    # def get_securities(self):
    #     stock_syms = [sym] #TSLA, NVDA, SPY, :::: COST, QQQ
    #     return stock_syms

    """
    Finds all itch data directories that we want to use
    """
    def get_data_dirs(self):
        ibkr_data_dirs = {}
        print(f'SEEKING data from: {self.start_date_str} ~ {self.end_date_str}')
        start_date = datetime.strptime(self.start_date_str, "%m%d%y")
        end_date = datetime.strptime(self.end_date_str, "%m%d%y")

        for stock in self.securities:
            print('CURRENT STOCK: ',stock)
            ibkr_data_dirs[stock] = {}
            available_dates=[]
            for root, dirs, files in os.walk(self.ohlcvt_dir):
                for file in files:
                    if file.startswith(stock) and 'checkpoint' not in file:
                        # print(file_name)
                        curr_date = file.split('.')[0].split('_')[-1]
                        # curr_date = file[:-4].split('_')[-1]
                        if 'IB' in curr_date:
                            curr_date = curr_date.replace('IB', '')
                        curr_date = curr_date[:-4] + curr_date[-2:] ## 08172022 -> 081722
                        if datetime.strptime(curr_date, "%m%d%y") >= start_date and datetime.strptime(curr_date, "%m%d%y") <= end_date:
                            available_dates.append(curr_date)
            available_dates = list(np.sort(np.unique(available_dates)))
            print(f'Available Dates: {available_dates}')
            
            iter_date = copy.deepcopy(start_date)
            while iter_date <= end_date:
                iter_date_str = iter_date.strftime("%m%d%y") #081122
                #print(f'iter_date_str: {iter_date_str}')

                date = iter_date_str[:4] + '20' + iter_date_str[-2:]

                target_lobname = stock + '_STK_USD_lob_' + date + '_neg_bid.csv'                
                target_bidaskname = stock + '_STK_USD_bidask_' + date + '.csv'
                target_ohlcvtname = stock + '_STK_USD_ohlcvt_' + date + '.csv'
                target_midpointname = stock + '_STK_USD_midpoint_' + date + '.csv'
                if iter_date <= datetime.strptime("083122", "%m%d%y"):
                    target_ohlcvtname = stock + '_STK_USD_ohlcmvt_' + date + '.csv'
                    target_midpointname = None

                target_foldername = f"S{iter_date_str}-v50"

                if iter_date_str in available_dates:
                    ibkr_data_dirs[stock][target_foldername] = [f'{self.lob_data_dir}{target_lobname}', f'{self.bidask_dir}{target_bidaskname}', f'{self.ohlcvt_dir}{target_ohlcvtname}', f'{self.ohlcvt_dir}{target_midpointname}']

                iter_date = iter_date + timedelta(days=1)
        return ibkr_data_dirs

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--start_date', type=str, required=True, help='desired start date for npy in mmddyy, e.g. 090122')
    parser.add_argument('--end_date', type=str, required=True, help='desired end date for npy in mmddyy, e.g. 090122')
    parser.add_argument('--sym', type=str, required=True, help='desired stock sym for npy in mmddyy, e.g. SPY')

    args = parser.parse_args()

    nasdaq_data = NasdaqData(args.start_date, args.end_date, args.sym)
    nasdaq_data.save_npy()
    # nasdaq_data.save_premkt_npy()
    # nasdaq_data.save_npy_bollinger()
    # nasdaq_data.save_premkt_npy()
