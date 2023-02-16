#! /bin/bash

# echo Path of the files:
# read file_wd

# file_wd=/home/namhooncho/Desktop/vwap_algo
file_wd=/home/seyeon04299/DATA/npy_parser

echo Enter start date mmddyy:
read start_date_str

echo Enter end date mmddyy:
read end_date_str

echo Enter sym:
read stock_sym

# echo Processing ITCH_LOB ...
# python3 $file_wd/subjectmap_maker.py --start_date $start_date_str --end_date $end_date_str --sym $stock_sym;

# echo Processing IBKR_LOB ...
# python3 $file_wd/subjectmap_maker_ibkr.py --start_date $start_date_str --end_date $end_date_str --sym $stock_sym;

echo Processing ITCH_OHLCVT ...
python3 $file_wd/subjectmap_maker_ohlc.py --start_date $start_date_str --end_date $end_date_str --sym $stock_sym;

echo Processing IBKR_OHLCVT ...
python3 $file_wd/subjectmap_maker_ibkr_ohlc.py --start_date $start_date_str --end_date $end_date_str --sym $stock_sym;
