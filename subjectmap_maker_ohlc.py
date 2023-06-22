
from src.constants import REGMKT_END_TIME_NS, REGMKT_START_TIME_NS, INTERVAL_SEC
from src.constants import T_N, R_N, A_N, TICKER_SYM, DATE_FROM_PATH, START_TIME_NS, END_TIME_NS, INTERVAL_NS

from collections import namedtuple
from logging import raiseExceptions
import os
import pandas as pd
import numpy as np

from pathlib import Path
from src.lib import Exchange
import zstandard
from pathlib import Path
import json

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

def filter_regmkt(data, start, end):
        data = data[data['interval_index'] >= start]
        data = data[data['interval_index'] < end]
        data = data.to_numpy()
        return data

def rolling_window(a, window):
    shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
    strides = a.strides + (a.strides[-1],)
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)


class HistoricalData(object):
    def __init__(self, sym):
        print("Make sure to preprocess everything before running this code...")
        self.csv_dir = "./csv"
        if "_" in sym:
            self.securities = sym.split("_")
        else:
            self.securities = [sym]
        # self.securities = self.get_securities()

        self.T = T_N                        # number of intervals
        self.D = len(self.data_dirs)        # number of days
        self.N = len(self.securities)       # number of securities

        print(f"time interval(T): {self.T}")
        print(f"number of days(D): {self.D}")
        print(f"number of securities(N): {self.N}")
        
        self.stats_map = {}
        self.subject_map = []

    # def get_securities(self):
    #     stock_syms = ["SPY"] #TSLA, NVDA, SPY, :::: COST, QQQ
    #     return stock_syms

    def ffill_zeros(self, np_array):
        df = pd.DataFrame(np_array)
        df.replace(to_replace=0, method='ffill', inplace=True)
        return np.squeeze(df.values)
    
    def ffill_zeros_ohlc(self, open_np, high_np, low_np, close_np):
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

    def load_json_zst(self, path: Path):
        return json.loads(zstandard.decompress(Path(path).expanduser().read_bytes()))
    
    def save_ohlcvt(self):
        # subject_map = []
        ohlcvt_atr_count = 0
        ohlcvt_atr_l1_count = 0
        ohlcvt_atr_bollinger_count = 0
        
        sma_window = 1800
        std_mp_constant = 2

        for d, data_dir in enumerate(self.data_dirs):
            start = time.time()

            date = data_dir.split('/')[-2]
            nasdaq_path = data_dir.split('/')[-3]
            print(f"computing {date} volume and LOB: {d} / {len(self.data_dirs)} ...")
            
            folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/','npy_itch_parsed_data', date)
            # folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/','npy_itch_parsed_data', date)
            if not os.path.exists(folder_dir):
                os.makedirs(folder_dir)
            
            
            ## For each Securities
            for n, sym in enumerate(self.securities):
                skip_ohlcvt_atr = False
                skip_ohlcvt_atr_l1 = False
                skip_ohlcvt_atr_bollinger = False
                
                save_dir_ohlcvt_atr = os.path.join(folder_dir, f'input_{sym}_OHLCVT_ATR')
                save_dir_ohlcvt_atr_l1 = os.path.join(folder_dir, f'input_{sym}_OHLCVT_ATR_L1')
                save_dir_ohlcvt_atr_bollinger = os.path.join(folder_dir, f'input_{sym}_OHLCVT_ATR_bollinger_{sma_window}_{std_mp_constant*10}')

                if not self.ohlcvt_atr:
                    skip_ohlcvt_atr = True
                if not self.ohlcvt_atr_l1:
                    skip_ohlcvt_atr_l1 = True
                if not self.ohlcvt_atr_bollinger:
                    skip_ohlcvt_atr_bollinger = True
                
                if self.overwrite:
                    pass
                else:
                    if os.path.exists(save_dir_ohlcvt_atr+'.npy'):
                        print(f'\t\t{sym} OHLCVT_ATR file exists!')
                        skip_ohlcvt_atr = True
                    if os.path.exists(save_dir_ohlcvt_atr_l1+'.npy'):
                        print(f'\t\t{sym} OHLCVT_ATR_L1 file exists!')
                        skip_ohlcvt_atr_l1 = True
                    if os.path.exists(save_dir_ohlcvt_atr_bollinger+'.npy'):
                        print(f'\t\t{sym} OHLCVT_ATR_BOLLINGER file exists!')
                        skip_ohlcvt_atr_bollinger = True
                    
                    if (skip_ohlcvt_atr and skip_ohlcvt_atr_l1 and skip_ohlcvt_atr_bollinger):
                        print('\tSkipping as all files exist!\n')
                        continue

                sym_stats_map_tmp = {}
                
                #### GET VOLUME, LOB ####
                try:
                    market_stat = self.load_json_zst(data_dir + f"{sym}.json.zst")
                except Exception as e:
                    # TODO XXX
                    # this means that sym is not in market stats
                    # most likely because the stock is listed in NYSE and
                    # not traded in itch
                    print(f"{sym} data not available in {data_dir}. This is most likely due to the fact that some NYSE stocks do not get traded in Nasdaq")
                    print("This should not print")
                    print(e)
                    continue
                    # raiseExceptions()
                
                ### Load Volume and LOB
                interval_ask_execute_volume = np.array(market_stat["interval_ask_execute_volume"], dtype=np.double)     # (57660, )
                interval_bid_execute_volume = np.array(market_stat["interval_bid_execute_volume"], dtype=np.double)     # (57660, )
                
                interval_open = np.array(market_stat["interval_open"], dtype=np.double)                                 # (57660, )
                interval_high = np.array(market_stat["interval_high"], dtype=np.double)                                 # (57660, )
                interval_low = np.array(market_stat["interval_low"], dtype=np.double)                                   # (57660, )
                interval_close = np.array(market_stat["interval_close"], dtype=np.double)                               # (57660, )

                lob_bid_price = np.array(market_stat["lob_bid_price"], dtype=np.double)                                 # (57660, 20)
                lob_ask_price = np.array(market_stat["lob_ask_price"], dtype=np.double)                                 # (57660, 20)
                lob_bid_shares = np.array(market_stat["lob_bid_shares"],dtype=np.double)                                # (57660, 20)
                lob_ask_shares = np.array(market_stat["lob_ask_shares"],dtype=np.double)                                # (57660, 20)
                
                interval_midprice = np.array(market_stat["interval_midprice"], dtype=np.double)                         # (57660, 20)
                
                ### Fill zeros
                interval_open, interval_high, interval_low, interval_close = self.ffill_zeros_ohlc(interval_open, interval_high, interval_low, interval_close)
                lob_bid_price, lob_ask_price = self.ffill_zeros(lob_bid_price), self.ffill_zeros(lob_ask_price)
                interval_midprice = self.ffill_zeros(interval_midprice)

                ### Regular market times
                regmkt_interval_ask_execute_volume = interval_ask_execute_volume[R_N:A_N]
                regmkt_interval_bid_execute_volume = interval_bid_execute_volume[R_N:A_N]

                regmkt_interval_open = interval_open[R_N:A_N] / 1e4
                regmkt_interval_high = interval_high[R_N:A_N] / 1e4
                regmkt_interval_low = interval_low[R_N:A_N] / 1e4
                regmkt_interval_close = interval_close[R_N:A_N] / 1e4

                regmkt_best_ask_price = lob_ask_price[R_N:A_N, 0] / 1e4
                regmkt_best_bid_price = lob_bid_price[R_N:A_N, -1] / 1e4

                regmkt_best_ask_shares = lob_ask_shares[R_N:A_N, 0]
                regmkt_best_bid_shares = lob_bid_shares[R_N:A_N, -1] * -1

                # regmkt_interval_midprice = interval_midprice[R_N:A_N] / 1e4
                regmkt_interval_midprice = (regmkt_best_ask_price + regmkt_best_bid_price) / 2
                
                assert (regmkt_interval_open != 0).all()
                assert (regmkt_interval_high != 0).all()
                assert (regmkt_interval_low != 0).all()
                assert (regmkt_interval_close != 0).all()
                
                assert (regmkt_best_ask_price != 0).all()
                assert (regmkt_best_bid_price != 0).all()
                
                assert (regmkt_interval_midprice != 0).all()

                ## ATR Calculation
                high_low = np.concatenate([np.array([0]), regmkt_interval_high[1:] - regmkt_interval_low[1:]])
                high_cp = np.abs(np.concatenate([np.array([0]), regmkt_interval_high[1:] - regmkt_interval_close[:-1]]))
                low_cp = np.abs(np.concatenate([np.array([0]), regmkt_interval_low[1:] - regmkt_interval_close[:-1]]))

                true_range = np.maximum(high_low, high_cp, low_cp)
                regmkt_interval_atr = np.concatenate([np.zeros(14),np.mean(rolling_window(true_range,14),1)[1:]])
                
                ### Bollinger Calculation
                typical_price = (regmkt_interval_high + regmkt_interval_low + regmkt_interval_close) / 3
                typical_price_window = rolling_window(typical_price, sma_window)[:-1,:]

                sma = np.squeeze(np.mean(typical_price_window, axis=1))
                std = np.squeeze(np.std(typical_price_window, axis=1))

                upper_bounds = sma + std_mp_constant * std
                lower_bounds = sma - std_mp_constant * std

                upper_bounds = np.expand_dims(np.concatenate([np.zeros(sma_window), upper_bounds]),axis=-1)
                lower_bounds = np.expand_dims(np.concatenate([np.zeros(sma_window), lower_bounds]),axis=-1)

                ### Set Directory Name
                ## OHLCVT_ATR
                if not skip_ohlcvt_atr:
                    data = np.concatenate(([
                        ## INTERVAL OHLC
                        np.expand_dims(regmkt_interval_open, 1),
                        np.expand_dims(regmkt_interval_high, 1),
                        np.expand_dims(regmkt_interval_low, 1),
                        np.expand_dims(regmkt_interval_close, 1),

                        ## OTHERS
                        np.expand_dims(regmkt_interval_ask_execute_volume, 1),
                        np.expand_dims(regmkt_interval_bid_execute_volume, 1),

                        np.expand_dims(regmkt_best_ask_price, 1),
                        np.expand_dims(regmkt_best_bid_price, 1),
                        np.expand_dims(regmkt_interval_midprice, 1),

                        ## ATR
                        np.expand_dims(regmkt_interval_atr, 1),

                        ## TIME INDEX
                        np.expand_dims(np.arange(0, 23400), 1),
                    ]), axis=1)
                
                    print(f'\t{sym} ITCH OHLCVT_ATR: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_ohlcvt_atr+'.npy', data)
                    ohlcvt_atr_count += 1
                
                ## OHLCVT_ATR_L1
                if not skip_ohlcvt_atr_l1:
                    data = np.concatenate(([
                        ## INTERVAL OHLC
                        np.expand_dims(regmkt_interval_open, 1),
                        np.expand_dims(regmkt_interval_high, 1),
                        np.expand_dims(regmkt_interval_low, 1),
                        np.expand_dims(regmkt_interval_close, 1),

                        ## OTHERS
                        np.expand_dims(regmkt_interval_ask_execute_volume, 1),
                        np.expand_dims(regmkt_interval_bid_execute_volume, 1),

                        np.expand_dims(regmkt_best_ask_price, 1),
                        np.expand_dims(regmkt_best_bid_price, 1),
                        np.expand_dims(regmkt_best_ask_shares, 1),
                        np.expand_dims(regmkt_best_bid_shares, 1),
                        np.expand_dims(regmkt_interval_midprice, 1),

                        ## ATR
                        np.expand_dims(regmkt_interval_atr, 1),

                        ## TIME INDEX
                        np.expand_dims(np.arange(0, 23400), 1),
                    ]), axis=1)
                
                    print(f'\t{sym} ITCH OHLCVT_ATR_L1: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_ohlcvt_atr_l1+'.npy', data)
                    ohlcvt_atr_l1_count += 1
                
                ## OHLCVT_ATR_BOLLINGER
                if not skip_ohlcvt_atr_bollinger:
                    data = np.concatenate(([
                        ## INTERVAL OHLC
                        np.expand_dims(regmkt_interval_open, 1),
                        np.expand_dims(regmkt_interval_high, 1),
                        np.expand_dims(regmkt_interval_low, 1),
                        np.expand_dims(regmkt_interval_close, 1),

                        ## OTHERS
                        np.expand_dims(regmkt_interval_ask_execute_volume, 1),
                        np.expand_dims(regmkt_interval_bid_execute_volume, 1),

                        np.expand_dims(regmkt_best_ask_price, 1),
                        np.expand_dims(regmkt_best_bid_price, 1),
                        np.expand_dims(regmkt_interval_midprice, 1),

                        ## ATR
                        np.expand_dims(regmkt_interval_atr, 1),

                        ## BOLLINGER
                        upper_bounds,
                        lower_bounds,

                        ## TIME INDEX
                        np.expand_dims(np.arange(0, 23400), 1),
                    ]), axis=1)
                
                    print(f'\t{sym} ITCH OHLCVT_ATR BOLLINGER: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_ohlcvt_atr_bollinger+'.npy', data)
                    ohlcvt_atr_bollinger_count += 1

            end = time.time()
            #print(f"Time elapsed for calculating volume/lob (with mean/std...) of 1 day: {end - start}\n")
            print(f"Time elapsed for 1 day: {end - start:.3f} seconds\n")

        print(f'\n\nCreated {ohlcvt_atr_count} OHLCVT_ATR npy files.')
        print(f'Created {ohlcvt_atr_l1_count} OHLCVT_ATR_L1 npy files.')
        print(f'Created {ohlcvt_atr_bollinger_count} OHLCVT_ATR_BOLLINGER npy files.\n\n')

    def save_volume_wLOB_premkt(self):
        # subject_map = []
        ohlcvt_atr_count = 0

        for d, data_dir in enumerate(self.data_dirs):
            start = time.time()

            date = data_dir.split('/')[-2]
            nasdaq_path = data_dir.split('/')[-3]
            print(f"computing {date} volume and LOB: {d} / {len(self.data_dirs)} ...")
            
            folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/','npy_itch_parsed_data', date)
            # folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/','npy_itch_parsed_data', date)
            if not os.path.exists(folder_dir):
                os.makedirs(folder_dir)
            
            
            ## For each Securities
            for n, sym in enumerate(self.securities):
                skip_ohlcvt_atr = False
                save_dir_ohlcvt_atr = os.path.join(folder_dir, f'input_{sym}_OHLCVT_ATR_premkt7200')

                if os.path.exists(save_dir_ohlcvt_atr+'.npy'):
                    print(f'\t\t{sym} OHLCVT_ATR file exists!')
                    skip_ohlcvt_atr = True
                
                if skip_ohlcvt_atr:
                    print('\tSkipping as all files exist!\n')
                    continue

                sym_stats_map_tmp = {}
                
                #### GET VOLUME, LOB ####
                try:
                    market_stat = self.load_json_zst(data_dir + f"{sym}.json.zst")
                except Exception as e:
                    # TODO XXX
                    # this means that sym is not in market stats
                    # most likely because the stock is listed in NYSE and
                    # not traded in itch
                    print(f"{sym} data not available in {data_dir}. This is most likely due to the fact that some NYSE stocks do not get traded in Nasdaq")
                    print("This should not print")
                    print(e)
                    continue
                    # raiseExceptions()
                
                ### Load Volume and LOB
                interval_ask_execute_volume = np.array(market_stat["interval_ask_execute_volume"], dtype=np.double)     # (57660, )
                interval_bid_execute_volume = np.array(market_stat["interval_bid_execute_volume"], dtype=np.double)     # (57660, )
                
                interval_open = np.array(market_stat["interval_open"], dtype=np.double)                                 # (57660, )
                interval_high = np.array(market_stat["interval_high"], dtype=np.double)                                 # (57660, )
                interval_low = np.array(market_stat["interval_low"], dtype=np.double)                                   # (57660, )
                interval_close = np.array(market_stat["interval_close"], dtype=np.double)                               # (57660, )

                lob_bid_price = np.array(market_stat["lob_bid_price"], dtype=np.double)                                 # (57660, 20)
                lob_ask_price = np.array(market_stat["lob_ask_price"], dtype=np.double)                                 # (57660, 20)
                
                interval_midprice = np.array(market_stat["interval_midprice"], dtype=np.double)                         # (57660, 20)

                ### Regular market times
                regmkt_interval_ask_execute_volume = interval_ask_execute_volume[R_N-7200:R_N]
                regmkt_interval_bid_execute_volume = interval_bid_execute_volume[R_N-7200:R_N]

                regmkt_interval_open = interval_open[R_N-7200:R_N] / 1e4
                regmkt_interval_high = interval_high[R_N-7200:R_N] / 1e4
                regmkt_interval_low = interval_low[R_N-7200:R_N] / 1e4
                regmkt_interval_close = interval_close[R_N-7200:R_N] / 1e4

                regmkt_interval_open = self.ffill_zeros(regmkt_interval_open)
                regmkt_interval_high = self.ffill_zeros(regmkt_interval_high)
                regmkt_interval_low = self.ffill_zeros(regmkt_interval_low)
                regmkt_interval_close = self.ffill_zeros(regmkt_interval_close)

                regmkt_best_ask_price = lob_ask_price[R_N-7200:R_N, 0] / 1e4
                regmkt_best_bid_price = lob_bid_price[R_N-7200:R_N, -1] / 1e4
                # regmkt_interval_midprice = interval_midprice[R_N:A_N] / 1e4
                regmkt_interval_midprice = (regmkt_best_ask_price + regmkt_best_bid_price) / 2

                ## ATR Calculations
                high_low = np.concatenate([np.array([0]), regmkt_interval_high[1:] - regmkt_interval_low[1:]])
                high_cp = np.abs(np.concatenate([np.array([0]), regmkt_interval_high[1:] - regmkt_interval_close[:-1]]))
                low_cp = np.abs(np.concatenate([np.array([0]), regmkt_interval_low[1:] - regmkt_interval_close[:-1]]))

                true_range = np.maximum(high_low, high_cp, low_cp)
                regmkt_interval_atr = np.concatenate([np.zeros(14),np.mean(rolling_window(true_range,14),1)[1:]])
                
                ### Set Directory Name
                ## OHLCVT
                if not skip_ohlcvt_atr:
                    data = np.concatenate(([
                        ## INTERVAL OHLC
                        np.expand_dims(regmkt_interval_open, 1),
                        np.expand_dims(regmkt_interval_high, 1),
                        np.expand_dims(regmkt_interval_low, 1),
                        np.expand_dims(regmkt_interval_close, 1),

                        ## OTHERS
                        np.expand_dims(regmkt_interval_ask_execute_volume, 1),
                        np.expand_dims(regmkt_interval_bid_execute_volume, 1),

                        np.expand_dims(regmkt_best_ask_price, 1),
                        np.expand_dims(regmkt_best_bid_price, 1),
                        np.expand_dims(regmkt_interval_midprice, 1),

                        ## ATR
                        np.expand_dims(regmkt_interval_atr, 1),

                        ## TIME INDEX
                        np.expand_dims(np.arange(0-7200, 0), 1),
                    ]), axis=1)
                
                    print(f'\t{sym} ITCH PREMKT OHLCVT_ATR: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_ohlcvt_atr+'.npy', data)
                    ohlcvt_atr_count += 1

            end = time.time()
            #print(f"Time elapsed for calculating volume/lob (with mean/std...) of 1 day: {end - start}\n")
            print(f"Time elapsed for 1 day: {end - start:.3f} seconds\n")

        print(f'\n\nCreated {ohlcvt_atr_count} PREMKT OHLCVT_ATR npy files.\n\n')

    def save_volume_related(self):
        # subject_map = []
        volume_count = 0

        for d, data_dir in enumerate(self.data_dirs):
            start = time.time()

            date = data_dir.split('/')[-2]
            nasdaq_path = data_dir.split('/')[-3]
            print(f"computing {date} volume and LOB: {d} / {len(self.data_dirs)} ...")
            
            folder_dir = os.path.join('/home/namhooncho/server','npy_itch_parsed_data', date)
            # folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/','npy_itch_parsed_data', date)
            if not os.path.exists(folder_dir):
                os.makedirs(folder_dir)
            
            
            ## For each Securities
            for n, sym in enumerate(self.securities):
                skip_volume = False
                save_dir_volume = os.path.join(folder_dir, f'input_{sym}_VOLUME')

                if os.path.exists(save_dir_volume+'.npy'):
                    print(f'\t\t{sym} VOLUME file exists!')
                    skip_volume = True
                
                if skip_volume:
                    print('\tSkipping as all files exist!\n')
                    continue

                sym_stats_map_tmp = {}
                
                #### GET VOLUME, LOB ####
                try:
                    market_stat = self.load_json_zst(data_dir + f"{sym}.json.zst")
                except Exception as e:
                    # TODO XXX
                    # this means that sym is not in market stats
                    # most likely because the stock is listed in NYSE and
                    # not traded in itch
                    print(f"{sym} data not available in {data_dir}. This is most likely due to the fact that some NYSE stocks do not get traded in Nasdaq")
                    print("This should not print")
                    print(e)
                    continue
                    # raiseExceptions()
                
                ### Load Volume and LOB
                interval_ask_execute_volume = np.array(market_stat["interval_ask_execute_volume"], dtype=np.double)     # (57660, )
                interval_bid_execute_volume = np.array(market_stat["interval_bid_execute_volume"], dtype=np.double)     # (57660, )

                interval_ask_cancel_volume = np.array(market_stat["interval_ask_cancel_volume"], dtype=np.double)       # (57660, )
                interval_bid_cancel_volume = np.array(market_stat["interval_bid_cancel_volume"], dtype=np.double)       # (57660, )

                interval_ask_add_volume = np.array(market_stat["interval_ask_add_volume"], dtype=np.double)             # (57660, )
                interval_bid_add_volume = np.array(market_stat["interval_bid_add_volume"], dtype=np.double)             # (57660, )

                lob_bid_price = np.array(market_stat["lob_bid_price"], dtype=np.double)                                 # (57660, 20)
                lob_ask_price = np.array(market_stat["lob_ask_price"], dtype=np.double)                                 # (57660, 20)
                
                interval_midprice = np.array(market_stat["interval_midprice"], dtype=np.double)                         # (57660, 20)

                ### Regular market times
                regmkt_interval_ask_execute_volume = interval_ask_execute_volume[R_N:A_N]
                regmkt_interval_bid_execute_volume = interval_bid_execute_volume[R_N:A_N]

                regmkt_interval_ask_cancel_volume = interval_ask_cancel_volume[R_N:A_N]
                regmkt_interval_bid_cancel_volume = interval_bid_cancel_volume[R_N:A_N]

                regmkt_interval_ask_add_volume = interval_ask_add_volume[R_N:A_N]
                regmkt_interval_bid_add_volume = interval_bid_add_volume[R_N:A_N]

                regmkt_best_ask_price = lob_ask_price[R_N:A_N, 0] / 1e4
                regmkt_best_bid_price = lob_bid_price[R_N:A_N, -1] / 1e4
                # regmkt_interval_midprice = interval_midprice[R_N:A_N] / 1e4
                regmkt_interval_midprice = (regmkt_best_ask_price + regmkt_best_bid_price) / 2

                
                ### Set Directory Name
                ## OHLCVT
                if not skip_volume:
                    data = np.concatenate(([

                        np.expand_dims(regmkt_interval_midprice, 1),
                        
                        np.expand_dims(regmkt_interval_ask_execute_volume, 1),
                        np.expand_dims(regmkt_interval_bid_execute_volume, 1),

                        np.expand_dims(regmkt_interval_ask_cancel_volume, 1),
                        np.expand_dims(regmkt_interval_bid_cancel_volume, 1),

                        np.expand_dims(regmkt_interval_ask_add_volume, 1),
                        np.expand_dims(regmkt_interval_bid_add_volume, 1),

                        np.expand_dims(regmkt_best_ask_price, 1),
                        np.expand_dims(regmkt_best_bid_price, 1),
                        
                        ## TIME INDEX
                        np.expand_dims(np.arange(0, 23400), 1),
                    ]), axis=1)
                
                    print(f'\t{sym} ITCH VOLUME: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_volume+'.npy', data)
                    volume_count += 1

            end = time.time()
            #print(f"Time elapsed for calculating volume/lob (with mean/std...) of 1 day: {end - start}\n")
            print(f"Time elapsed for 1 day: {end - start:.3f} seconds\n")

        print(f'\n\nCreated {volume_count} VOLUME npy files.\n\n')

    def sym_historical_stats_calculator(self):
        print('Begin Historical Mean and Std Calculator')
        historical_stats = []
        for sym in self.stats_map.keys():
            sd = self.stats_map[sym]
            
            lob_v_mean = sd['lob_v_S1']/sd['n_lob']
            lob_v_std = np.sqrt(sd['lob_v_S2']/sd['n_lob']-(sd['lob_v_S1']/sd['n_lob'])**2)

            lob_p_mean = sd['lob_p_S1']/sd['n_lob']
            lob_p_std = np.sqrt(sd['lob_p_S2']/sd['n_lob']-(sd['lob_p_S1']/sd['n_lob'])**2)
            
            vol_mean = sd['vol_S1']/sd['n_vol']
            vol_std = np.sqrt(sd['vol_S2']/sd['n_vol']-(sd['vol_S1']/sd['n_vol'])**2)
            
            vol2_mean = sd['vol2_S1']/sd['n_vol2']
            vol2_std = np.sqrt(sd['vol2_S2']/sd['n_vol2']-(sd['vol2_S1']/sd['n_vol2'])**2)

            vol5_mean = sd['vol5_S1']/sd['n_vol5']
            vol5_std = np.sqrt(sd['vol5_S2']/sd['n_vol5']-(sd['vol5_S1']/sd['n_vol5'])**2)
            
            vol10_mean = sd['vol10_S1']/sd['n_vol10']
            vol10_std = np.sqrt(sd['vol10_S2']/sd['n_vol10']-(sd['vol10_S1']/sd['n_vol10'])**2)
            
            vol20_mean = sd['vol20_S1']/sd['n_vol20']
            vol20_std = np.sqrt(sd['vol20_S2']/sd['n_vol20']-(sd['vol20_S1']/sd['n_vol20'])**2)
            
            vol30_mean = sd['vol30_S1']/sd['n_vol30']
            vol30_std = np.sqrt(sd['vol30_S2']/sd['n_vol30']-(sd['vol30_S1']/sd['n_vol30'])**2)
            
            vol60_mean = sd['vol60_S1']/sd['n_vol60']
            vol60_std = np.sqrt(sd['vol60_S2']/sd['n_vol60']-(sd['vol60_S1']/sd['n_vol60'])**2)
            
            historical_stats.append( {
                'security' : sym,
                'lob_v_mean' : lob_v_mean,
                'lob_v_std' : lob_v_std,
                'lob_p_mean' : lob_p_mean,
                'lob_p_std' : lob_p_std,
                'vol_mean' : vol_mean,
                'vol_std' : vol_std,
                'vol2_mean' : vol2_mean,
                'vol2_std' : vol2_std,
                'vol5_mean' : vol5_mean,
                'vol5_std' : vol5_std,
                'vol10_mean' : vol10_mean,
                'vol10_std' : vol10_std,
                'vol20_mean' : vol20_mean,
                'vol20_std' : vol20_std,
                'vol30_mean' : vol30_mean,
                'vol30_std' : vol30_std,
                'vol60_mean' : vol60_mean,
                'vol60_std' : vol60_std,
            })
            
        
        historical_stats = pd.DataFrame(historical_stats)
        historical_stats.to_csv('../data/NASDAQ_used/historical_stats_2109_2203.csv',index=False)
        print('ITCH historical_stats saved to "../data/NASDAQ_used/historical_stats_2109_2203.csv" ')
        

class NasdaqData(HistoricalData):
    def __init__(self, start_date, end_date, sym):
        self.exchange = Exchange.NASDAQ
        self.data_dir = '/media/seyeon04299/HardDisk/jupyter_server/parsed_itch_data/'
        # self.data_dir = '/media/seyeon04299/HardDisk/jupyter_server/parsed_itch_data/'
        self.start_date_str = start_date
        self.end_date_str = end_date
        self.data_dirs = self.get_data_dirs()
        
        self.ohlcvt_atr = False
        self.ohlcvt_atr_l1 = False
        self.ohlcvt_atr_bollinger = False
        
        self.overwrite = False

        print(f"itch data_dirs: {self.data_dirs}")
        super().__init__(sym)


    """
    Finds all itch data directories that we want to use
    """
    def get_data_dirs(self):
        itch_data_dirs = []

        folders = []
        for root, dirs, files in os.walk(self.data_dir):
            for folder_name in dirs:
                if folder_name.endswith('-v50'):
                    folders.append(folder_name)

        print(f'Loading data from: {self.start_date_str} ~ {self.end_date_str}')
        start_date = datetime.strptime(self.start_date_str, "%m%d%y")
        end_date = datetime.strptime(self.end_date_str, "%m%d%y")
        #print(f'start_date: {start_date}, end_date: {end_date}')

        iter_date = copy.deepcopy(start_date)
        while iter_date <= end_date:
            iter_date_str = iter_date.strftime("%m%d%y")
            #print(f'iter_date_str: {iter_date_str}')
            target_foldername = f"S{iter_date_str}-v50"
            if target_foldername in folders:
                itch_data_dirs.append(f'{self.data_dir}{target_foldername}/')
            iter_date = iter_date + timedelta(days=1)
        
        # remove all half days
        for half_day in half_days:
            data_dir = f"{self.data_dir}S" + half_day + "-v50/"
            if data_dir in itch_data_dirs:
                itch_data_dirs.remove(data_dir)

        # remove all fomc dates
        for fomc_day in fomc_days:
            data_dir = f"{self.data_dir}S" + fomc_day + "-v50/"
            if data_dir in itch_data_dirs:
                itch_data_dirs.remove(data_dir)

        itch_data_dirs = sorted(itch_data_dirs, key=lambda x: datetime.strptime(x.split('/')[-2][1:7], '%m%d%y'), reverse=True)
        
        return itch_data_dirs


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--start_date', type=str, required=True, help='desired dates for the testbed in mmddyy, e.g. 090122')
    parser.add_argument('--end_date', type=str, required=True, help='desired dates for the testbed in mmddyy, e.g. 090122')
    parser.add_argument('--sym', type=str, required=True, help='desired stock sym for npy in mmddyy, e.g. SPY')
    args = parser.parse_args()

    nasdaq_data = NasdaqData(args.start_date, args.end_date, args.sym)
    
    nasdaq_data.overwrite = False
    nasdaq_data.ohlcvt_atr = True
    nasdaq_data.ohlcvt_atr_l1 = True
    nasdaq_data.ohlcvt_atr_bollinger = False
    
    nasdaq_data.save_ohlcvt()
    # nasdaq_data.save_volume_related()
    # nasdaq_data.save_volume_wLOB_premkt()
    # nasdaq_data.sym_historical_stats_calculator()