# -*- coding: utf-8 -*-
"""
Created on Tue Mar 12 02:11:46 2019

@author: baoloc.lam
"""

import datetime
import pandas as pd
from StockData import StockData


stock_data = StockData()

# =============================================================================
# ########## Scrape tickers history info & events ##########

stock_data.main_url = 'https://cophieu68.vn/companylist.php?currentPage=1&o=s&ud=a'
stock_data.row_col_pair = ['tr', 'td']
stock_data.multi_pages = True
stock_data.page_start_string = "?currentPage="
stock_data.page_end_string = '&amp;o=s&amp;ud=a'
stock_data.main_table_config = {
    'tag_type': 'table',
    'attr': None,
    'attr_val': None,
    'pos': 3,
    'lookup_type': 'find_all'
}
stock_data.pagination_config = {
    'tag_type': 'ul',
    'attr': 'id',
    'attr_val': 'navigator',
    'pos': 2,
    'lookup_type': 'find'
}

excluded_rows = [0]
excluded_cols = [0, 2, 13]
result_column_names = [
    'symbol',
    'ipo_date',
    'ipo_vol',
    'ipo_price',
    'outstanding_shares',
    'treasury_stock',
    'listed_shares',
    'foreign_shares_lim',
    'remaining_foreign_shares',
    'market_price',
    'market_cap'
]

tickers_data = stock_data.get_live_snapshot(excluded_rows, excluded_cols, result_column_names)
print(tickers_data)

# =============================================================================



# =============================================================================
#  ########## Download stock data ##########

# stock_data.main_url = 'https://www.cophieu68.vn/export.php'
# stock_data.form_url = 'https://www.cophieu68.vn/account/login.php'
# stock_data.main_table_config = {
#     'tag_type': 'table',
#     'attr': None,
#     'attr_val': None,
#     'pos': 1,
#     'lookup_type': 'find_all'
# }
# stock_data.form_config = {
#     'pos': 2,
#     'fields': {
#         'username': 'loc.lam92@gmail.com',
#         'tpassword': 'homesweethome92'
#     }
# }

# ticker_col = 0
# excluded_rows = [0, 1]
# excluded_cols = []
# get_link_from_cols = [1, 2, 3, 4, 5]
# save_folder_directory = 'C:/Users/baoloc.lam/1-LocLam/Spyder Projects/vn_stock_trading/raw_data'

# start = datetime.datetime.now()
# stock_data.dwl_data(ticker_col, excluded_rows, excluded_cols, get_link_from_cols, save_folder_directory)
# print('Process time:', datetime.datetime.now() - start)

# =============================================================================


# =============================================================================
# ######### Data validation ##########

#merged = pd.merge(mt4_data,
#                  xls_data,
#                  how='outer',
#                  on=['<Ticker>', '<DTYYYYMMDD>'],
#                  suffixes=('_mt4', '_xls'))
#base_cols = ['<Open>_mt4', '<High>_mt4', '<Low>_mt4', '<Close>_mt4']
#ref_cols = ['<Open>_xls', '<High>_xls', '<Low>_xls', '<Close>_xls']
#
#validation = validate_data(same_df=True,
#                           df_base=merged,
#                           df_ref=merged,
#                           base_cols=base_cols,
#                           ref_cols=ref_cols)
#merged['mt4_null'] = validation['base_null']
#merged['xls_null'] = validation['ref_null']
#merged['data_mismatch'] = validation['data_mismatch']

# =============================================================================


# =============================================================================
# ########## Data verification ##########

#show_cols = ['<Ticker>', '<DTYYYYMMDD>']
#data_verify = merged[merged['data_mismatch'] == True][show_cols]
#data_verify = data_verify.reset_index()

# =============================================================================


# =============================================================================
# ########## Get verifying data #########

#url = 'https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml'
#
#table = {'tag_type': 'ul',
#         'attr': 'class',
#         'attr_val': 'list_tktt lichsugia',
#         'pos': 0,
#         'lookup_type': 'find_all'}
#
#form = {'pos': 0,
#        'fields': {'searchMarketStatisticsView.symbol': None,
#                   'strFromDate': None,
#                   'strToDate': None}}
#
#
#row_col_pair = ['li', 'div']
#
#exclude_rows = [0]
#
#exclude_cols = [0, 1, 6, 9]
#
#seed_columns = ['open_vnd',
#                'high_vnd',
#                'low_vnd',
#                'close_vnd',
#                'adj_close_vnd',
#                'vol_vnd']

#for col in seed_columns:
#    data_verify[col] = None

#total_lines = len(data_verify.index)
#count = 1
#
#for idx, row in data_verify.iterrows():
#    ticker = row[0]
#    date = str(row[1])
#    search_date = ''.join([date[-2:], '/', date[4:6], '/', date[:4]])
#
#    form['fields']['searchMarketStatisticsView.symbol'] = ticker
#    form['fields']['strFromDate'] = search_date
#    form['fields']['strToDate'] = search_date
#
#    try:
#        data_from_web = scrape_table(main_url=url,
#                                     form_url=url,
#                                     form=form,
#                                     main_table=table,
#                                     row_col_pair=row_col_pair,
#                                     header_row=0,
#                                     exclude_rows=exclude_rows,
#                                     exclude_cols=exclude_cols,
#                                     df_columns=seed_columns)
#
#        data_verify.loc[idx][seed_columns[0]] = float(data_from_web.iloc[0][0])
#        data_verify.loc[idx][seed_columns[1]] = float(data_from_web.iloc[0][1])
#        data_verify.loc[idx][seed_columns[2]] = float(data_from_web.iloc[0][2])
#        data_verify.loc[idx][seed_columns[3]] = float(data_from_web.iloc[0][3])
#        data_verify.loc[idx][seed_columns[4]] = float(data_from_web.iloc[0][4])
#        data_verify.loc[idx][seed_columns[5]] = float(data_from_web.iloc[0][5])
#    except Exception as e:
#        print('Can\'t get data for', ticker, 'at index', idx, 'on date', date)
#        print(e)
#
#    print('Progress:', ''.join([str(round(count*100/total_lines, 2)), '%']))
#    print('='*79)
#    count +=1

# =============================================================================