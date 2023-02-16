
from src.constants import REGMKT_END_TIME_NS, REGMKT_START_TIME_NS, INTERVAL_SEC
from src.constants import T_N, R_N, A_N, TICKER_SYM, DATE_FROM_PATH, START_TIME_NS, END_TIME_NS, INTERVAL_NS

from collections import namedtuple
from logging import raiseExceptions
import os
import pandas as pd
import numpy as np

from pathlib import Path
from src.lib import Exchange
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

    def get_securities(self):
        stock_syms = ["AAPL", "TSLA", "SPY"] #TSLA, NVDA, SPY, :::: COST, QQQ
        return stock_syms

    def load_actions(self, path):
        if self.exchange == Exchange.NYSE:
            return mmm.nyse.load_actions(path)
        elif self.exchange == Exchange.NASDAQ:
            return mmm.nasdaq.load_actions(path)
        else:
            raise NotImplementedError()

    def ffill_zeros(self, np_array):
        df = pd.DataFrame(np_array)
        df.replace(to_replace=0, method='ffill', inplace=True)
        return np.squeeze(df.values)

    def load_json_zst(self, path: Path):
        return json.loads(zstandard.decompress(Path(path).expanduser().read_bytes()))
    

    def save_volume_wLOB(self):
        # subject_map = []
        full_count, lob_count, lob_cancel_count, lob_ohlc_count, lob_ibkr_like_count, lob_atr_ibkr_like_count = 0, 0, 0, 0, 0, 0

        for d, data_dir in enumerate(self.data_dirs):
            start = time.time()

            date = data_dir.split('/')[-2]
            nasdaq_path = data_dir.split('/')[-3]
            print(f"computing {date} volume and LOB: {d} / {len(self.data_dirs)} ...")
            
            folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/','npy_itch_parsed_data_7', date)
            if not os.path.exists(folder_dir):
                os.makedirs(folder_dir)
            
            
            ## For each Securities
            for n, sym in enumerate(self.securities):
                # skip_full, skip_lob, skip_lob_cancel, skip_lob_ohlc, skip_lob_ibkr_like = False, False, False, False, False
                skip_full, skip_lob, skip_lob_cancel, skip_lob_ohlc, skip_lob_ibkr_like, skip_lob_atr_ibkr_like = True, True, True, True, False, False
                save_dir_full = os.path.join(folder_dir, f'input_{sym}')
                save_dir_lob = os.path.join(folder_dir, f'input_{sym}_LOB')
                save_dir_lob_cancel = os.path.join(folder_dir, f'input_{sym}_LOB_CANCEL')
                save_dir_lob_ohlc = os.path.join(folder_dir, f'input_{sym}_LOB_OHLC')
                save_dir_lob_ibkr_like = os.path.join(folder_dir, f'input_{sym}_LOB_ibkr_like')
                save_dir_lob_atr_ibkr_like = os.path.join(folder_dir, f'input_{sym}_LOB_ATR_ibkr_like')

                if os.path.exists(save_dir_full+'.npy'):
                    print(f'\t\t{sym} FULL file exists!')
                    skip_full = True
                if os.path.exists(save_dir_lob+'.npy'):
                    print(f'\t\t{sym} LOB file exists!')
                    skip_lob = True
                if os.path.exists(save_dir_lob_cancel+'.npy'):
                    print(f'\t\t{sym} LOB_CANCEL file exists!')
                    skip_lob_cancel = True
                if os.path.exists(save_dir_lob_ohlc+'.npy'):
                    print(f'\t\t{sym} LOB_OHLC file exists!')
                    skip_lob_ohlc = True
                if os.path.exists(save_dir_lob_ibkr_like+'.npy'):
                    print(f'\t\t{sym} LOB_ibkr_like file exists!')
                    skip_lob_ibkr_like = True
                # if os.path.exists(save_dir_lob_atr_ibkr_like+'.npy'):
                #     print(f'\t{sym} LOB_ATR_ibkr_like file exists!')
                #     skip_lob_atr_ibkr_like = True
                
                if skip_full and skip_lob and skip_lob_cancel and skip_lob_ohlc and skip_lob_ibkr_like and skip_lob_atr_ibkr_like:
                    print('\tSkipping as all files exist!\n')
                    continue



                #if n%50==0:
                #    print("Computed {}/{} stocks...".format(n+1,len(self.securities)))
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
                interval_ask_execute_volume = np.array(market_stat["interval_ask_execute_volume"])  # (57660, )
                interval_bid_execute_volume = np.array(market_stat["interval_bid_execute_volume"])  # (57660, )

                # lob = np.array(market_stat["lob_level"],dtype=np.double)
                lob_bid_price = np.array(market_stat["lob_bid_price"],dtype=np.double)                  # (57660, 20)
                lob_ask_price = np.array(market_stat["lob_ask_price"],dtype=np.double)                  # (57660, 20)
                lob_bid_shares = np.array(market_stat["lob_bid_shares"],dtype=np.double)                # (57660, 20)
                lob_ask_shares = np.array(market_stat["lob_ask_shares"],dtype=np.double)                # (57660, 20)
                
                lob_ts = np.array(market_stat["lob_ts"],dtype=np.double)                                # (57660, )

                cancel_bid_price = np.array(market_stat["cancel_bid_price"],dtype=np.double)            # (57660, 20)
                cancel_ask_price = np.array(market_stat["cancel_ask_price"],dtype=np.double)            # (57660, 20)
                cancel_bid_shares = np.array(market_stat["cancel_bid_shares"],dtype=np.double)          # (57660, 20)
                cancel_ask_shares = np.array(market_stat["cancel_ask_shares"],dtype=np.double)          # (57660, 20)

                interval_open = np.array(market_stat["interval_open"],dtype=np.double)                  # (57660, )
                interval_open_ts = np.array(market_stat["interval_open_ts"],dtype=np.double)            # (57660, )
                interval_high = np.array(market_stat["interval_high"],dtype=np.double)                  # (57660, )
                interval_high_ts = np.array(market_stat["interval_high_ts"],dtype=np.double)            # (57660, )
                interval_low = np.array(market_stat["interval_low"],dtype=np.double)                    # (57660, )
                interval_low_ts = np.array(market_stat["interval_low_ts"],dtype=np.double)              # (57660, )
                interval_close = np.array(market_stat["interval_close"],dtype=np.double)                # (57660, )
                interval_close_ts = np.array(market_stat["interval_close_ts"],dtype=np.double)          # (57660, )

                interval_midprice = np.array(market_stat["interval_midprice"],dtype=np.double)          # (57660, )

                add_bid_msg_count = np.array(market_stat["add_bid_msg_count"],dtype=np.double)          # (57660, 20)
                add_ask_msg_count = np.array(market_stat["add_ask_msg_count"],dtype=np.double)          # (57660, 20)

                add_ask_price = np.array(market_stat["add_ask_price"],dtype=np.double)                  # (57660, 20)
                add_ask_shares = np.array(market_stat["add_ask_shares"],dtype=np.double)                # (57660, 20)
                add_bid_price = np.array(market_stat["add_bid_price"],dtype=np.double)                  # (57660, 20)
                add_bid_shares = np.array(market_stat["add_bid_shares"],dtype=np.double)                # (57660, 20)

                
                ### Regular market times
                interval_ask_execute_volume = interval_ask_execute_volume[R_N:A_N]
                interval_bid_execute_volume = interval_bid_execute_volume[R_N:A_N]

                lob_bid_price = lob_bid_price[R_N:A_N,:]
                lob_ask_price = lob_ask_price[R_N:A_N,:]
                lob_bid_shares = lob_bid_shares[R_N:A_N,:]
                lob_ask_shares = lob_ask_shares[R_N:A_N,:]

                interval_midprice = interval_midprice[R_N:A_N]

                ibkr_like_lob_bid_price = lob_bid_price / 1e4
                ibkr_like_lob_ask_price = lob_ask_price / 1e4
                ibkr_like_lob_bid_shares = np.floor_divide(lob_bid_shares, -100) * -1
                ibkr_like_lob_ask_shares = np.floor_divide(lob_ask_shares, 100)

                lob_ts = lob_ts[R_N:A_N]
                
                cancel_bid_price = cancel_bid_price[R_N:A_N,:]
                cancel_ask_price = cancel_ask_price[R_N:A_N,:]
                cancel_bid_shares = cancel_bid_shares[R_N:A_N,:]
                cancel_ask_shares = cancel_ask_shares[R_N:A_N,:]
                # ibkr_like_cancel_bid_price = cancel_bid_price[R_N:A_N,:] / 1e4
                # ibkr_like_cancel_ask_price = cancel_ask_price[R_N:A_N,:] / 1e4
                # ibkr_like_cancel_bid_shares = np.floor_divide(cancel_bid_shares[R_N:A_N,:], 100) * -1
                # ibkr_like_cancel_ask_shares = np.floor_divide(cancel_ask_shares[R_N:A_N,:], 100)

                interval_open = interval_open[R_N:A_N]
                interval_open_ts  = interval_open_ts[R_N:A_N]
                interval_high = interval_high[R_N:A_N]
                interval_high_ts  = interval_high_ts[R_N:A_N]
                interval_low = interval_low[R_N:A_N]
                interval_low_ts = interval_low_ts[R_N:A_N]
                interval_close = interval_close[R_N:A_N]
                interval_close_ts = interval_close_ts[R_N:A_N]

                ibkr_like_interval_open = interval_open / 1e4
                ibkr_like_interval_high = interval_high / 1e4
                ibkr_like_interval_low = interval_low / 1e4
                ibkr_like_interval_close = interval_close / 1e4
                ibkr_like_interval_midprice = interval_midprice / 1e4

                ibkr_like_interval_open = self.ffill_zeros(ibkr_like_interval_open)
                ibkr_like_interval_high = self.ffill_zeros(ibkr_like_interval_high)
                ibkr_like_interval_low = self.ffill_zeros(ibkr_like_interval_low)
                ibkr_like_interval_close = self.ffill_zeros(ibkr_like_interval_close)
                ibkr_like_interval_midprice = self.ffill_zeros(ibkr_like_interval_midprice)

                add_bid_msg_count = add_bid_msg_count[R_N:A_N,:]
                add_ask_msg_count = add_ask_msg_count[R_N:A_N,:]

                add_ask_price = add_ask_price[R_N:A_N,:]
                add_bid_price = add_bid_price[R_N:A_N,:]
                add_ask_shares = add_ask_shares[R_N:A_N,:]
                add_bid_shares = add_bid_shares[R_N:A_N,:]

                ## ATR Calculations
                high_low = np.concatenate([np.array([0]), ibkr_like_interval_high[1:] - ibkr_like_interval_low[1:]])
                high_cp = np.abs(np.concatenate([np.array([0]), ibkr_like_interval_high[1:] - ibkr_like_interval_close[:-1]]))
                low_cp = np.abs(np.concatenate([np.array([0]), ibkr_like_interval_low[1:] - ibkr_like_interval_close[:-1]]))

                true_range = np.maximum(high_low, high_cp, low_cp)
                interval_atr = np.concatenate([np.zeros(14),np.mean(rolling_window(true_range,14),1)[1:]])
                
                ### Set Directory Name
                ## FULL
                if not skip_full:
                    data = np.concatenate(([
                        ## LOB
                        lob_bid_price,
                        lob_ask_price,
                        lob_bid_shares,
                        lob_ask_shares,

                        ## CANCEL

                        cancel_bid_price,
                        cancel_ask_price,
                        cancel_bid_shares,
                        cancel_ask_shares,

                        ## ORDER FLOW

                        add_bid_price,
                        add_ask_price,
                        add_bid_shares,
                        add_ask_shares,

                        add_bid_msg_count,
                        add_ask_msg_count,

                        ## INTERVAL OHLC

                        np.expand_dims(interval_open, 1),
                        np.expand_dims(interval_open_ts, 1),
                        np.expand_dims(interval_high, 1),
                        np.expand_dims(interval_high_ts, 1),
                        np.expand_dims(interval_low, 1),
                        np.expand_dims(interval_low_ts, 1),
                        np.expand_dims(interval_close, 1),
                        np.expand_dims(interval_close_ts, 1),

                        ## OTHERS

                        np.expand_dims(interval_ask_execute_volume, 1),
                        np.expand_dims(interval_bid_execute_volume, 1),
    
                        np.expand_dims(interval_midprice, 1),
                        np.expand_dims(lob_ts, 1),
                    ]), axis=1)
                
                    print(f'\t{sym} FULL: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_full+'.npy', data)
                    full_count += 1

                ## LOB
                if not skip_lob:
                    data = np.concatenate(([
                        ## LOB
                        lob_bid_price,
                        lob_ask_price,
                        lob_bid_shares,
                        lob_ask_shares,

                        ## OTHERS

                        np.expand_dims(interval_ask_execute_volume, 1),
                        np.expand_dims(interval_bid_execute_volume, 1),
    
                        np.expand_dims(interval_midprice, 1),
                        np.expand_dims(lob_ts, 1),
                    ]), axis=1)
                
                    print(f'\t{sym} LOB: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_lob+'.npy', data)
                    lob_count += 1
                
                ## LOB_CANCEL
                if not skip_lob_cancel:
                    data = np.concatenate(([
                        ## LOB
                        lob_bid_price,
                        lob_ask_price,
                        lob_bid_shares,
                        lob_ask_shares,

                        ## CANCEL

                        cancel_bid_price,
                        cancel_ask_price,
                        cancel_bid_shares,
                        cancel_ask_shares,

                        ## OTHERS

                        np.expand_dims(interval_ask_execute_volume, 1),
                        np.expand_dims(interval_bid_execute_volume, 1),
    
                        np.expand_dims(interval_midprice, 1),
                        np.expand_dims(lob_ts, 1),
                    ]), axis=1)
                
                    print(f'\t{sym} LOB_CANCEL: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_lob_cancel+'.npy', data)
                    lob_cancel_count += 1
                
                ## LOB_OHLC
                if not skip_lob_ohlc:
                    data = np.concatenate(([
                        ## LOB
                        lob_bid_price,
                        lob_ask_price,
                        lob_bid_shares,
                        lob_ask_shares,

                        ## INTERVAL OHLC

                        np.expand_dims(interval_open, 1),
                        np.expand_dims(interval_open_ts, 1),
                        np.expand_dims(interval_high, 1),
                        np.expand_dims(interval_high_ts, 1),
                        np.expand_dims(interval_low, 1),
                        np.expand_dims(interval_low_ts, 1),
                        np.expand_dims(interval_close, 1),
                        np.expand_dims(interval_close_ts, 1),

                        ## OTHERS

                        np.expand_dims(interval_ask_execute_volume, 1),
                        np.expand_dims(interval_bid_execute_volume, 1),
    
                        np.expand_dims(interval_midprice, 1),
                        np.expand_dims(lob_ts, 1),
                    ]), axis=1)
                
                    print(f'\t{sym} LOB_OHLC: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_lob_ohlc+'.npy', data)
                    lob_ohlc_count += 1

                ## LOB_ibkr_like
                if not skip_lob_ibkr_like:
                    data = np.concatenate(([
                        ## LOB
                        ibkr_like_lob_bid_price,
                        ibkr_like_lob_ask_price,
                        ibkr_like_lob_bid_shares,
                        ibkr_like_lob_ask_shares,

                        ## OTHERS

                        np.expand_dims(interval_ask_execute_volume, 1),
                        np.expand_dims(interval_bid_execute_volume, 1),
    
                        np.expand_dims(ibkr_like_interval_midprice, 1),
                        np.expand_dims(lob_ts, 1),
                    ]), axis=1)
                
                    print(f'\t{sym} LOB_ibkr_like: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_lob_ibkr_like+'.npy', data)
                    lob_ibkr_like_count += 1

                ## LOB_ATR_ibkr_like
                if not skip_lob_atr_ibkr_like:
                    data = np.concatenate(([
                        ## LOB
                        ibkr_like_lob_bid_price,
                        ibkr_like_lob_ask_price,
                        ibkr_like_lob_bid_shares,
                        ibkr_like_lob_ask_shares,

                        ## OTHERS

                        np.expand_dims(interval_ask_execute_volume, 1),
                        np.expand_dims(interval_bid_execute_volume, 1),

                        ### ATR
                        np.expand_dims(interval_atr, 1),

                        np.expand_dims(ibkr_like_interval_midprice, 1),
                        np.expand_dims(lob_ts, 1),
                    ]), axis=1)
                    
                    print(f'\t{sym} LOB_ATR: Saving the data shape of {sym} as : {np.shape(data)} :: interval atr mean: {np.mean(interval_atr)}')
                    np.save(save_dir_lob_atr_ibkr_like+'.npy', data, allow_pickle=True)
                    lob_atr_ibkr_like_count += 1

            end = time.time()
            #print(f"Time elapsed for calculating volume/lob (with mean/std...) of 1 day: {end - start}\n")
            print(f"Time elapsed for 1 day: {end - start} seconds\n")

        print(f'\n\nCreated {full_count} full npy files, {lob_count} lob npy files, {lob_cancel_count} lob_cancel npy files, {lob_ohlc_count} lob_ohlc npy files, {lob_ibkr_like_count} lob_ibkr_like npy files, {lob_atr_ibkr_like_count} lob_ibkr_atr_like npy files.\n\n')


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
        print('historical_stats saved to "../data/NASDAQ_used/historical_stats_2109_2203.csv" ')
        


class NasdaqData(HistoricalData):
    def __init__(self, start_date, end_date, sym):
        self.exchange = Exchange.NASDAQ
        self.data_dir = '/media/seyeon04299/HardDisk/jupyter_server/itch_parsed_data_7/'
        self.start_date_str = start_date
        self.end_date_str = end_date
        self.data_dirs = self.get_data_dirs()

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

        return itch_data_dirs


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--start_date', type=str, required=True, help='desired dates for the testbed in mmddyy, e.g. 090122')
    parser.add_argument('--end_date', type=str, required=True, help='desired dates for the testbed in mmddyy, e.g. 090122')
    parser.add_argument('--sym', type=str, required=True, help='desired stock sym for npy in mmddyy, e.g. SPY')
    args = parser.parse_args()

    nasdaq_data = NasdaqData(args.start_date, args.end_date, args.sym)
    nasdaq_data.save_volume_wLOB()
    # nasdaq_data.sym_historical_stats_calculator()