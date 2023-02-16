
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

    

    

    def save_npy(self):
        # subject_map = []
        full_count, lob_count, lob_cancel_count, lob_ohlc_count, lob_atr_count = 0, 0, 0, 0, 0

        for s, sym in enumerate(self.data_dirs):

            for d, save_date in enumerate(self.data_dirs[sym]):
                # save_date = S081222-v50
                start = time.time()

                lob_dir = self.data_dirs[sym][save_date][0]
                ohlc_dir = self.data_dirs[sym][save_date][1]

                print(f'\nLOB path: {lob_dir}')
                print(f'OHLCVMT path: {ohlc_dir}')

                # date = data_dir.split('/')[-2]
                # nasdaq_path = data_dir.split('/')[-3]
                print(f"Loading {save_date} LOB and OHLCMV: {d} / {len(self.data_dirs)} ...")
                
                
                # ## Create npz save folder if not exist
                # if not os.path.exists(folder_dir):
                #     os.makedirs(folder_dir)
                
                folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/npy_ibkr_parsed_data', save_date)
                # folder_dir = os.path.join('/media/seyeon04299/HardDisk/jupyter_server/npy_ibkr_parsed_data', save_date)
                if not os.path.exists(folder_dir):
                    os.makedirs(folder_dir)
                
                # ## For each Securities
                # for n, sym in enumerate(self.securities):
                skip_lob, skip_lob_ohlc, skip_lob_atr = False, False, False

                save_dir_lob = os.path.join(folder_dir, f'input_{sym}_LOB_ibkr_like')
                save_dir_lob_ohlc = os.path.join(folder_dir, f'input_{sym}_LOB_OHLC_ibkr_like')
                save_dir_lob_atr = os.path.join(folder_dir, f'input_{sym}_LOB_ATR_ibkr_like')

                if os.path.exists(save_dir_lob+'.npy'):
                    print('\tLOB file exists!')
                    skip_lob = True
                if os.path.exists(save_dir_lob_ohlc+'.npy'):
                    print('\tLOB_OHLC file exists!')
                    skip_lob_ohlc = True

                if os.path.exists(save_dir_lob_atr+'.npy'):
                    print('\tLOB_OHLC file exists!')
                    skip_lob_atr = True
                
                if skip_lob and skip_lob_ohlc and skip_lob_atr:
                    print('\tSkipping as all files exist!\n')
                    continue

                sym_stats_map_tmp = {}

                lob_data = pd.read_csv(lob_dir)
                lob_data = lob_data.to_numpy(dtype=np.float64)
                lob_data = lob_data[self.reg_mkt_start-1:self.reg_mkt_end-1,:]
                # lob_data = filter_regmkt(lob_data, 19800, 43200)

                ohlc_data = pd.read_csv(ohlc_dir)
                ohlc_data = ohlc_data.to_numpy(dtype=np.float64)
                ohlc_data = ohlc_data[self.reg_mkt_start-1:self.reg_mkt_end-1,:]
                # ohlc_data = filter_regmkt(ohlc_data, 19800, 43200)

                lob_ask_price, lob_ask_shares, lob_bid_price, lob_bid_shares = None, None, None, None
                if lob_data.shape[1] == 83:
                    lob_ask_price = lob_data[:, 3::4]
                    lob_ask_shares = lob_data[:, 4::4]
                    lob_bid_price = lob_data[:, 5::4]
                    lob_bid_shares = lob_data[:, 6::4]
                elif lob_data.shape[1] == 80:
                    lob_ask_price = lob_data[:, 0::4]
                    lob_ask_shares = lob_data[:, 1::4]
                    lob_bid_price = lob_data[:, 2::4]
                    lob_bid_shares = lob_data[:, 3::4]
                else:
                    print(f"LOB path: {lob_dir}\nCheck your lob data shape: {np.shape(lob_data)}")
                    raise NotImplementedError()
                # lob_ts = lob_data[:,-1]                   ## FIXME lob timestamp in lob data?

                interval_open = ohlc_data[:,5]
                interval_high = ohlc_data[:,6]
                interval_low = ohlc_data[:,7]
                interval_close = ohlc_data[:,8]
                interval_midprice = (lob_ask_price[:,0]+lob_bid_price[:,0])/2
                interval_bid_execute_volume = ohlc_data[:,-3]
                interval_ask_execute_volume = ohlc_data[:,-2]
                # interval_timestamp = ohlc_data[:,7]
                interval_timestamp = lob_data[:,1]

                ### ATR Calculation
                high_low = np.concatenate([np.array([0]), interval_high[1:] - interval_low[1:]])
                high_cp = np.abs(np.concatenate([np.array([0]), interval_high[1:] - interval_close[:-1]]))
                low_cp = np.abs(np.concatenate([np.array([0]), interval_low[1:] - interval_close[:-1]]))

                true_range = np.maximum(high_low, high_cp, low_cp)
                interval_atr = np.concatenate([np.zeros(14), np.mean(rolling_window(true_range, 14), 1)[1:]])

                ## timestamp from index to ns
                # interval_timestamp = 1e9*(4*3600 + (interval_timestamp + 1))

                ## LOB
                data = np.concatenate(([
                    ## LOB
                    lob_bid_price[:,::-1],
                    lob_ask_price,
                    lob_bid_shares[:,::-1],
                    lob_ask_shares,

                    ## OTHERS

                    np.expand_dims(interval_ask_execute_volume, 1),
                    np.expand_dims(interval_bid_execute_volume, 1),

                    np.expand_dims(interval_midprice, 1),
                    np.expand_dims(interval_timestamp, 1),
                ]), axis=1)
                
                if not skip_lob:
                    print(f'\tLOB: Saving the data shape of {sym} as : {np.shape(data)}, data type: {type(data[0, 0])}')
                    np.save(save_dir_lob+'.npy', data, allow_pickle=True)
                    lob_count += 1

                
                ## LOB_OHLC
                
                data = np.concatenate(([
                    ## LOB
                    lob_bid_price[:,::-1],
                    lob_ask_price,
                    lob_bid_shares[:,::-1],
                    lob_ask_shares,

                    ## INTERVAL OHLC

                    np.expand_dims(interval_open, 1),
                    np.expand_dims(interval_high, 1),
                    np.expand_dims(interval_low, 1),
                    np.expand_dims(interval_close, 1),

                    ## OTHERS

                    np.expand_dims(interval_ask_execute_volume, 1),
                    np.expand_dims(interval_bid_execute_volume, 1),

                    np.expand_dims(interval_midprice, 1),
                    np.expand_dims(interval_timestamp, 1),
                ]), axis=1)
                
                if not skip_lob_ohlc:
                    print(f'\tLOB_OHLC: Saving the data shape of {sym} as : {np.shape(data)}, data type: {type(data[0, 0])}')
                    np.save(save_dir_lob_ohlc+'.npy', data, allow_pickle=True)
                    lob_ohlc_count += 1

                ### LOB ATR
                data = np.concatenate(([
                    ## LOB
                    lob_bid_price[:,::-1],
                    lob_ask_price,
                    lob_bid_shares[:,::-1],
                    lob_ask_shares,

                    ## OTHERS

                    np.expand_dims(interval_ask_execute_volume, 1),
                    np.expand_dims(interval_bid_execute_volume, 1),

                    ### ATR
                    np.expand_dims(interval_atr, 1),

                    np.expand_dims(interval_midprice, 1),
                    np.expand_dims(interval_timestamp, 1),
                ]), axis=1)
                
                if not skip_lob_atr:
                    print(f'\tLOB_ATR: Saving the data shape of {sym} as : {np.shape(data)}')
                    np.save(save_dir_lob_atr+'.npy', data, allow_pickle=True)
                    lob_atr_count += 1


                end = time.time()
                #print(f"Time elapsed for calculating volume/lob (with mean/std...) of 1 day: {end - start}\n")
                print(f"Time elapsed for 1 day: {end - start} seconds\n")

        print(f'\n\nCreated {full_count} full npy files, {lob_count} lob npy files, {lob_cancel_count} lob_cancel npy files, {lob_ohlc_count} lob_ohlc npy files, {lob_atr_count} lob_atr npy files.\n\n')

class NasdaqData(HistoricalData):
    def __init__(self, start_date, end_date, sym):
        self.exchange = Exchange.NASDAQ
        # self.data_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/to_parse/'
        # self.ohlcvmt_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/shared/'
        self.data_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/to_parse/'
        self.ohlcvmt_dir = '/media/seyeon04299/HardDisk/jupyter_server/ibkr_data/shared/'
        self.start_date_str = start_date
        self.end_date_str = end_date
        self.securities = [sym]
        # self.securities = self.get_securities()
        self.data_dirs = self.get_data_dirs()

        super().__init__()
        print(f"total data_dirs: {self.data_dirs}")

    # def get_securities(self):
    #     stock_syms = ["TSLA", "NVDA", "GOOGL", "QQQ", "SPY"] #TSLA, NVDA, SPY, :::: COST, QQQ
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
            for root, dirs, files in os.walk(self.data_dir):
                for file in files:
                    if file.startswith(stock) and 'checkpoint' not in file:
                        # print(file_name)
                        curr_date = file.split('_')[-3]
                        # curr_date = file[:-4].split('_')[-1]
                        if 'IB' in curr_date:
                            curr_date = curr_date.replace('IB', '')
                        curr_date = curr_date[:-4] + curr_date[-2:] ## 08172022 -> 081722
                        if datetime.strptime(curr_date, "%m%d%y") >= start_date and datetime.strptime(curr_date, "%m%d%y") <= end_date:
                            available_dates.append(curr_date)
            available_dates = list(np.sort(np.unique(available_dates)))
            print(f'Available Dates: {available_dates}')
            
            # if start_date < datetime.strptime(available_dates[0], "%m%d%y"):
            #     start_date = datetime.strptime(available_dates[0], "%m%d%y")
            #     print('New start_date : ', start_date)
            
            # if end_date > datetime.strptime(available_dates[-1], "%m%d%y"):
            #     end_date = datetime.strptime(available_dates[-1], "%m%d%y")
            #     print('New end_date : ', end_date)
            
            
            iter_date = copy.deepcopy(start_date)
            while iter_date <= end_date:
                iter_date_str = iter_date.strftime("%m%d%y") #081122
                #print(f'iter_date_str: {iter_date_str}')

                date = iter_date_str[:4]+'20'+iter_date_str[-2:]
                # target_lobname = stock+'_STK_lob_IB'+date+'_neg_bid.csv'
                # target_ohlcname = stock+'_STK_ohlcmvt_IB'+date+'.csv'

                target_lobname = stock+'_STK_USD_lob_'+date+'_neg_bid.csv'
                # target_lobname = stock+'_STK_USD_lob_'+date+'.csv'
                target_ohlcname = stock+'_STK_USD_ohlcvt_'+date+'.csv'
                target_foldername = f"S{iter_date_str}-v50"

                if iter_date_str in available_dates:
                    ibkr_data_dirs[stock][target_foldername] = [f'{self.data_dir}{target_lobname}', f'{self.ohlcvmt_dir}{target_ohlcname}']


                iter_date = iter_date + timedelta(days=1)
            
        # # remove all half days
        # for half_day in half_days:
        #     data_dir = f"{self.data_dir}S" + half_day + "-v50/"
        #     if data_dir in ibkr_data_dirs:
        #         ibkr_data_dirs.remove(data_dir)

        # # remove all fomc dates
        # for fomc_day in fomc_days:
        #     data_dir = f"{self.data_dir}S" + fomc_day + "-v50/"
        #     if data_dir in ibkr_data_dirs:
        #         ibkr_data_dirs.remove(data_dir)


        return ibkr_data_dirs


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--start_date', type=str, required=True, help='desired dates for the testbed in mmddyy, e.g. 090122')
    parser.add_argument('--end_date', type=str, required=True, help='desired dates for the testbed in mmddyy, e.g. 090122')
    parser.add_argument('--sym', type=str, required=True, help='desired stock sym for npy in mmddyy, e.g. SPY')
    args = parser.parse_args()

    nasdaq_data = NasdaqData(args.start_date, args.end_date, args.sym)
    nasdaq_data.save_npy()