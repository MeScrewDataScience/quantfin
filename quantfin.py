# -*- coding: utf-8 -*-

import itertools
import json
import logging
import logging.config
import os
import pandas as pd
import qfutils
import random
from datetime import datetime as dt, time as t
from qfbrowsing import SelBrowsing, Bs4Browsing, SoupReader
from queue import Queue as multithreadingQueue
from threading import Thread
from multiprocessing import Process
from multiprocessing import Queue as multiprocessingQueue
from time import sleep


with open('logging_config.json') as jsonfile:
    logging_config = json.load(jsonfile)

logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)


class DataFeeder():

    def __init__(
        self,
        driver_exe=None,
        driver='chrome',
        headless=True
    ):
        
        self.symbol_index = 'symbol'
        self.date_index = 'date'
        self.driver = driver
        self.driver_exe = driver_exe
        self.headless = headless
        self.na_values = ['-', '_', 'na', 'n/a', 'NA', 'N/A']
        self._multi_workers_in_use = False
        self._dump_data_to_var = False
        
        # VNDirect properties
        self.vnd_df = pd.DataFrame()
        self.vnd_symbols_array = []
        self.vnd_unloadables = []
        self.vnd_prop = {
            'url': 'https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml',
            'symbol_xpath': '//*[@id="symbolID"]',
            'from_date_xpath': '//*[@id="fHistoricalPrice_FromDate"]',
            'to_date_xpath': '//*[@id="fHistoricalPrice_ToDate"]',
            'button_xpath': '//*[@id="fHistoricalPrice_View"]',
            'pagination_xpath': '//*[@id="tab-1"]/div[1]',
            'next_page': '>',
            'table_attr': [
                {
                    'tag_name': 'ul',
                    'row_col_pair': ['li', 'div'],
                    'attr': 'class',
                    'attr_val': 'list_tktt lichsugia',
                    'idx': 0,
                    'lookup_type': 'find_all'
                }
            ],
            'header': [0],
            'skiprows': [],
            'result_date_format': '%Y-%m-%d',
            'query_date_format': '%d/%m/%Y',
            'thousands': ',',
            'decimal': '.',
            'symbol_index': self.symbol_index,
            'date_index': self.date_index,
            'dict_cols': {
                'Mã CK': self.symbol_index,
                'Ngày': self.date_index,
                'Thayđổi(+-/%)': 'change_vnd',
                'Giámởcửa': 'open_vnd',
                'Giácaonhất': 'high_vnd',
                'Giáthấpnhất': 'low_vnd',
                'Giáđóngcửa': 'close_vnd',
                'Giábìnhquân': 'avg_vnd',
                'Giáđóngcửađiềuchỉnh': 'adj_close_vnd',
                'KLkhớplệnh': 'vol_vnd',
                'KLthỏathuận': 'vol_deal_vnd'
            },
            'na_values': self.na_values
        }
        self.vnd_prop['selected_cols'] = [
            self.symbol_index,
            self.date_index,
            self.vnd_prop['dict_cols']['Giáđóngcửađiềuchỉnh'],
            self.vnd_prop['dict_cols']['Giámởcửa'],
            self.vnd_prop['dict_cols']['Giácaonhất'],
            self.vnd_prop['dict_cols']['Giáthấpnhất'],
            self.vnd_prop['dict_cols']['Giáđóngcửa'],
            self.vnd_prop['dict_cols']['KLkhớplệnh']
        ]

        # CafeF properties
        self.cafef_df = pd.DataFrame()
        self.cafef_symbols_array = []
        self.cafef_unloadables = []
        self.cafef_prop = {
            'url': 'http://s.cafef.vn/Lich-su-giao-dich-VNINDEX-1.chn',
            'symbol_xpath': '//*[@id="ctl00_ContentPlaceHolder1_ctl03_txtKeyword"]',
            'from_date_xpath': '//*[@id="ctl00_ContentPlaceHolder1_ctl03_dpkTradeDate1_txtDatePicker"]',
            'to_date_xpath': '//*[@id="ctl00_ContentPlaceHolder1_ctl03_dpkTradeDate2_txtDatePicker"]',
            'button_xpath': '//*[@id="ctl00_ContentPlaceHolder1_ctl03_btSearch"]',
            'pagination_xpath': '//table[@class="CafeF_Paging"]',
            'next_page': '>',
            'table_attr': [
                {'id': 'GirdTable'},
                {'id': 'GridTable'},
                {'id': 'GirdTable2'},
                {'id': 'GridTable2'}
            ],
            'header': [0, 1],
            'skiprows': [],
            'result_date_format': '%d/%m/%Y',
            'query_date_format': '%d/%m/%Y',
            'thousands': ',',
            'decimal': '.',
            'symbol_index': self.symbol_index,
            'date_index': self.date_index,
            'dict_cols': {
                'Mã CK': self.symbol_index,
                'Ngày_Ngày': self.date_index,
                'Giá điều chỉnh_Giá điều chỉnh': 'adj_close_cafef',
                'Giá đóng cửa_Giá đóng cửa': 'close_cafef',
                'Giá bình quân_Giá bình quân': 'avg_cafef',
                'Thay đổi (+/-%)_Thay đổi (+/-%)': 'change_cafef',
                'Thay đổi (+/-%)_Thay đổi (+/-%).1': 'change_2_cafef',
                'GD khớp lệnh_KL': 'vol_cafef',
                'GD khớp lệnh_GT': 'vol_value_cafef',
                'GD thỏa thuận_KL': 'vol_deal_cafef',
                'GD thỏa thuận_GT': 'vol_deal_value_cafef',
                'Giá tham chiếu_Giá tham chiếu': 'ref_price_cafef',
                'Giá mở cửa_Giá mở cửa': 'open_cafef',
                'Giá cao nhất_Giá cao nhất': 'high_cafef',
                'Giá thấp nhất_Giá thấp nhất': 'low_cafef'
            },
            'na_values': self.na_values
        }
        self.cafef_prop['selected_cols'] = [
            self.symbol_index,
            self.date_index,
            self.cafef_prop['dict_cols']['Giá điều chỉnh_Giá điều chỉnh'],
            self.cafef_prop['dict_cols']['Giá mở cửa_Giá mở cửa'],
            self.cafef_prop['dict_cols']['Giá cao nhất_Giá cao nhất'],
            self.cafef_prop['dict_cols']['Giá thấp nhất_Giá thấp nhất'],
            self.cafef_prop['dict_cols']['Giá đóng cửa_Giá đóng cửa'],
            self.cafef_prop['dict_cols']['GD khớp lệnh_KL'],
            self.cafef_prop['dict_cols']['GD khớp lệnh_GT']
        ]

        # VCSC properties
        self.vcsc_df = pd.DataFrame()
        self.vcsc_symbols_array = []
        self.vcsc_unloadables = []
        self.vcsc_prop = {
            'url': 'http://ra.vcsc.com.vn/Stock',
            'symbol_xpath': [
                '//*[@id="content"]/div[2]/div/div/div[1]/div[1]/span/span[1]/span/span[2]',
                '//input[@class="select2-search__field"]'
            ],
            'from_date_xpath': '//*[@id="fromDate"]',
            'to_date_xpath': '//*[@id="toDate"]',
            'button_xpath': '//*[@id="btnView"]',
            'pagination_xpath': '//*[@id="content"]/div[2]/div/div/div[3]/div[2]',
            'next_page': 'Next',
            'table_attr': [{'class': 'tblTransaction'}],
            'header': [0],
            'skiprows': [],
            'result_date_format': '%d/%m/%Y',
            'query_date_format': '%Y-%m-%d',
            'thousands': '.',
            'decimal': ',',
            'symbol_index': self.symbol_index,
            'date_index': self.date_index,
            'dict_cols': {
                'Mã CK': self.symbol_index,
                'Ngày': self.date_index,
                'Thay đổi (+/-/%)': 'change_vcsc',
                'Giámở cửa': 'open_vcsc',
                'Cao nhất': 'high_vcsc',
                'Thấp nhất': 'low_vcsc',
                'Giá đóng cửa': 'close_vcsc',
                'KLGD khớp lệnh (CP)': 'vol_vcsc',
                'GTGD khớp lệnh (Tỷ VND)': 'vol_value_vcsc',
                'KLGD thỏa thuận (CP)': 'vol_deal_vcsc',
                'GTGD thỏa thuận (Tỷ VND)': 'vol_deal_value_vcsc',
                'Dư mua (CP)': 'overbought_vcsc',
                'Dư bán (CP)': 'oversold_vcsc',
                'Tổng GDGD (Tỷ VND)': 'total_vol_value_vcsc',
                'Tổng KLGD (CP)': 'total_vol_vcsc'
            },
            'na_values': self.na_values
        }
        self.vcsc_prop['selected_cols'] = [
            self.symbol_index,
            self.date_index,
            self.vcsc_prop['dict_cols']['Giámở cửa'],
            self.vcsc_prop['dict_cols']['Cao nhất'],
            self.vcsc_prop['dict_cols']['Thấp nhất'],
            self.vcsc_prop['dict_cols']['Giá đóng cửa'],
            self.vcsc_prop['dict_cols']['KLGD khớp lệnh (CP)'],
            self.vcsc_prop['dict_cols']['GTGD khớp lệnh (Tỷ VND)']
        ]

        # Stockbiz properties
        self.sb_df = pd.DataFrame()
        self.sb_symbols_array = []
        self.sb_unloadables = []
        self.sb_prop = {
            'url': 'https://www.stockbiz.vn/Stocks/A32/HistoricalQuotes.aspx',
            'symbol_xpath': [
                '//*[@id="ctl00_webPartManager_wp1770166562_wp1427611561_symbolSearch_txtSymbol"]'
            ],
            'from_date_xpath': '//*[@id="ctl00_webPartManager_wp1770166562_wp1427611561_dtStartDate_picker_picker"]',
            'to_date_xpath': '//*[@id="ctl00_webPartManager_wp1770166562_wp1427611561_dtEndDate_picker_picker"]',
            'button_xpath': '//*[@id="ctl00_webPartManager_wp1770166562_wp1427611561_btnView"]',
            'pagination_xpath': '//*[@id="ctl00_webPartManager_wp1770166562_wp1427611561_callbackData"]/div[2]',
            'next_page': 'Tiếp »',
            'table_attr': [{'class': 'dataTable'}],
            'header': [0],
            'skiprows': [],
            'result_date_format': '%d/%m/%Y',
            'query_date_format': '%d/%m/%Y',
            'thousands': '.',
            'decimal': ',',
            'symbol_index': self.symbol_index,
            'date_index': self.date_index,
            'dict_cols': {
                'Mã CK': self.symbol_index,
                'Ngày': self.date_index,
                'Thay đổi': 'change_sb',
                'Mở cửa': 'open_sb',
                'Cao nhất': 'high_sb',
                'Thấp nhất': 'low_sb',
                'Đóng cửa': 'close_sb',
                'Trung bình': 'avg_price_sb',
                'Đóng cửa ĐC': 'adj_close_sb',
                'Khối lượng': 'vol_sb'
            },
            'na_values': self.na_values
        }
        self.sb_prop['selected_cols'] = [
            self.symbol_index,
            self.date_index,
            self.sb_prop['dict_cols']['Đóng cửa ĐC'],
            self.sb_prop['dict_cols']['Mở cửa'],
            self.sb_prop['dict_cols']['Cao nhất'],
            self.sb_prop['dict_cols']['Thấp nhất'],
            self.sb_prop['dict_cols']['Đóng cửa'],
            self.sb_prop['dict_cols']['Khối lượng']
        ]

        # CP68 propertiesself.sb_df = pd.DataFrame()
        self.cp68_df_mt4 = pd.DataFrame()
        self.cp68_df_xls = pd.DataFrame()
        self.cp68_df_events = pd.DataFrame()
        self.cp68_symbols_array = []
        self.cp68_unloadables = []
        self.cp68_prop = {
            'url': 'https://www.cophieu68.vn/export.php',
            'login_url': 'https://www.cophieu68.vn/account/login.php',
            'table_attr': {
                'tag_name': 'table',
                'row_col_pair': ['tr', 'td'],
                'attr': None,
                'attr_val': None,
                'idx': 1,
                'lookup_type': 'find_all'
            },
            'login_form': {
                'idx': 2,
                'fields': {
                    'username': None,
                    'tpassword': None
                }
            },
            'other_forms': {
                'idx': None,
                'fields': {}
            },
            'failed_login': {
                'tag_name': 'span',
                'attr': 'style',
                'attr_val': 'color:#FF0000; font-weight: bold',
                'lookup_type': 'find'
            },
            'symbol_index': self.symbol_index,
            'date_index': self.date_index,
            'mt4': {
                'idx': 1,
                'dict_cols': {
                    '<Ticker>': self.symbol_index,
                    '<DTYYYYMMDD>': self.date_index,
                    '<Open>': 'open_mt4',
                    '<High>': 'high_mt4',
                    '<Low>': 'low_mt4',
                    '<Close>': 'close_mt4',
                    '<Volume>': 'vol_mt4'
                },
                'result_date_format': '%Y%m%d'
            },
            'xls': {
                'idx': 2,
                'dict_cols': {
                    '<Ticker>': self.symbol_index,
                    '<DTYYYYMMDD>': self.date_index,
                    '<OpenFixed>': 'adj_open_xls',
                    '<HighFixed>': 'adj_high_xls',
                    '<LowFixed>': 'adj_low_xls',
                    '<CloseFixed>': 'adj_close_xls',
                    '<Volume>': 'vol_xls',
                    '<Open>': 'open_xls',
                    '<High>': 'high_xls',
                    '<Low>': 'low_xls',
                    '<Close>': 'close_xls',
                    '<VolumeDeal>': 'vol_deal_xls',
                    '<VolumeFB>': 'vol_fb_xls',
                    '<volumeFS>': 'vol_fs_xls'
                },
                'result_date_format': '%Y%m%d'
            },
            'fin_report': {
                'idx': 3
            },
            'fin_index': {
                'idx': 4
            },
            'events': {
                'idx': 5,
                'dict_cols': {
                    'LoaiSuKien': 'event',
                    'NgayGDKHQ': 'ex_div_date',
                    'NgayThucHien': 'exe_date',
                    'TyLeCoTuc': 'payout_ratio',
                    'GhiChu': 'note'
                },
                'result_date_format': '%d%m%Y'
            },
            'skiprows': [0, 1],
            'skipcols': [],
            'symbol_col': 0,
            'na_values': self.na_values
        }
        self.cp68_prop['mt4']['selected_cols'] = [
            self.symbol_index,
            self.date_index,
            self.cp68_prop['mt4']['dict_cols']['<Open>'],
            self.cp68_prop['mt4']['dict_cols']['<High>'],
            self.cp68_prop['mt4']['dict_cols']['<Low>'],
            self.cp68_prop['mt4']['dict_cols']['<Close>'],
            self.cp68_prop['mt4']['dict_cols']['<Volume>']
        ]
        self.cp68_prop['xls']['selected_cols'] = [
            self.symbol_index,
            self.date_index,
            self.cp68_prop['xls']['dict_cols']['<CloseFixed>'],
            self.cp68_prop['xls']['dict_cols']['<Open>'],
            self.cp68_prop['xls']['dict_cols']['<High>'],
            self.cp68_prop['xls']['dict_cols']['<Low>'],
            self.cp68_prop['xls']['dict_cols']['<Close>'],
            self.cp68_prop['xls']['dict_cols']['<Volume>']
        ]
        self.cp68_prop['events']['selected_cols'] = [
            self.cp68_prop['events']['dict_cols']['LoaiSuKien'],
            self.cp68_prop['events']['dict_cols']['NgayGDKHQ'],
            self.cp68_prop['events']['dict_cols']['NgayThucHien'],
            self.cp68_prop['events']['dict_cols']['TyLeCoTuc'],
            self.cp68_prop['events']['dict_cols']['GhiChu']
        ]
    

    def multiprocessing(
        self,
        symbols_array,
        methods,
        date_format=None,
        records=None,
        cp68_username=None,
        cp68_password=None,
        cp68_data_source=None,
        cp68_to_folder=None,
        workers=4
    ):
        """
        This method calls one of the followings methods:
            - vnd_get_data,
            - cafef_get_data,
            - vcsc_get_data,
            - sb_get_data,
            - cp68_get_data,
            - cp68_mass_download
        and runs multiple workers on the task to fasten the scraping process.
        
        Parameter:
            methods         :   the list/tuple of methods to call. 3 methods are eligible for calling: 
                                    vnd_get_data,
                                    cafef_get_data,
                                    vcsc_get_data
            symbols_array   :   1-dimensional or 2-dimensional list/tuple
                template    :   (str(symbol), str(from_date), str(to_date))
                example     :   (('AAA', '01/12/2019', '31/12/2019')
                                ('A32', '01/01/2020', '10/01/2020'))
            workers         :   int. Number of workers to be run.
                                Maximum number of workers is 8.
                                Number of workers must not exceed
                                number of symbols
        """
        
        # Validate methods' eligibility
        if not isinstance(methods, list) and not isinstance(methods, tuple):
            methods = [methods]
        
        # Validate number of workers
        symbols_array_len = len(symbols_array)

        max_workers = os.cpu_count()
        if workers > max_workers:
            logger.warning(f'The maximum number of workers is {max_workers}. '
                           f'Number of workers will be reduced to {max_workers}')
            workers = max_workers
        
        if workers > symbols_array_len:
            logger.warning(f'The number of workers ({workers}) exceeds '
                           f'the number of symbols ({symbols_array_len}). '
                           f'Number of workers will be reduced to '
                           f'{symbols_array_len}')
            workers = symbols_array_len
        
        self._multi_workers_in_use = True
        
        method_set = {}
        for method in methods:

            if method in ('cp68', 'cp68_mass_download'):
                self._update_class_symbols(method, symbols_array, False)
            else:
                self._update_class_symbols(method, symbols_array, date_format)

            self._clear_unloadables(method)
            
            method_set[method] = {
                # 'symbols': self._get_attribute(method, 'symbols'),
                'func': self._get_attribute(method, 'function'),
                'queue': {},
                'workers': {}
            }

        # Break symbols array to evenly size chunks
        split_symbols_array = qfutils.split_array(symbols_array, workers)

        # Run multiple workers
        try:
            for i in range(workers):
                for method in methods:
                    method_set[method]['queue'][i] = multiprocessingQueue()
                    
                    if method == 'cp68':
                        args = (
                            method_set[method]['queue'][i],
                            method_set[method]['func'],
                            # method_set[method]['symbols'],
                            split_symbols_array[i],
                            records,
                            cp68_username,
                            cp68_password,
                            cp68_data_source
                        )
                    elif method == 'cp68_mass_download':
                        args = (
                            method_set[method]['queue'][i],
                            method_set[method]['func'],
                            # method_set[method]['symbols'],
                            split_symbols_array[i],
                            cp68_username,
                            cp68_password,
                            cp68_data_source,
                            cp68_to_folder
                        )
                    else:
                        args = (
                            method_set[method]['queue'][i],
                            method_set[method]['func'],
                            # method_set[method]['symbols'],
                            split_symbols_array[i],
                            date_format,
                            records
                        )
                    
                    method_set[method]['workers'][i] = Process(
                        target=self._queue_function,
                        args=args
                    )
            
            for i in range(workers):
                for method in methods:
                    method_set[method]['workers'][i].start()
            
            for i in range(workers):
                for method in methods:
                    method_set[method]['workers'][i].join()
                
                
        except KeyboardInterrupt:
            print('Keyboard interupted')
        
        finally:
            self._multi_workers_in_use = False
        
        # Combine all resulted dataframe to one result set
        result_df = pd.DataFrame()
        result_set = []

        for method in methods:

            if method == 'cp68_mass_download':
                continue
            
            logger.info(f'{method} - Joining results')
            
            for i in range(workers):
                if method_set[method]['queue'][i].empty():
                    continue
                
                temp_df = method_set[method]['queue'][i].get()
                
                result_df = pd.concat([result_df, temp_df], sort=True)
                logger.info(f'{method} - Process {i} - Query result '
                            'appended to consolidated dataframe')
            
            if not result_df.empty:            
                logger.info(f'{method} - Updating {method} class dataframe...')
                # Update class dataframe
                if method == 'cp68':
                    df_name = '_'.join(['df', cp68_data_source])
                    self._update_class_df(method, result_df, df_name)
                else:
                    self._update_class_df(method, result_df)
            
                logger.info(f'{method} - Appending results to result set...')
                result_set.append(result_df)
                logger.info(f'{method} - Results are added to result set')
            
            else:
                logger.error(f'{method} - Method returns null dataframe')
        
        return result_set
    

    def multithreading(
        self,
        symbols_array,
        methods,
        date_format=None,
        records=None,
        cp68_username=None,
        cp68_password=None,
        cp68_data_source=None,
        cp68_to_folder=None,
        workers=5
    ):
        """
        This method calls one of the followings methods:
            - vnd_get_data,
            - cafef_get_data,
            - vcsc_get_data,
            - sb_get_data,
            - cp68_get_data,
            - cp68_mass_download
        and runs multiple workers on the task to fasten the scraping process.

        Parameter:
            methods         :   the list/tuple of methods to call. 3 methods are eligible for calling: 
                                    vnd_get_data,
                                    cafef_get_data,
                                    vcsc_get_data
            symbols_array   :   1-dimensional or 2-dimensional list/tuple
                template    :   (str(symbol), str(from_date), str(to_date))
                example     :   (('AAA', '01/12/2019', '31/12/2019')
                                ('A32', '01/01/2020', '10/01/2020'))
            workers         :   int. Number of workers to be run.
                                Maximum number of workers is 20.
                                Number of workers must not exceed
                                number of symbols
        """
        
        # Validate methods' eligibility
        if not isinstance(methods, list) and not isinstance(methods, tuple):
            methods = [methods]
        
        # Validate number of workers
        symbols_array_len = len(symbols_array)

        if workers > 20:
            logger.warning('The maximum number of workers is 20. '
                           'Number of workers will be reduced to 20')
            workers = 20
        
        if workers > symbols_array_len:
            logger.warning(f'The number of workers ({workers}) exceeds '
                           f'the number of symbols ({symbols_array_len}). '
                           f'Number of workers will be reduced to '
                           f'{symbols_array_len}')
            workers = symbols_array_len
        
        self._multi_workers_in_use = True
        
        method_set = {}
        for method in methods:

            if method in ('cp68', 'cp68_mass_download'):
                self._update_class_symbols(method, symbols_array, False)
            else:
                self._update_class_symbols(method, symbols_array, date_format)

            self._clear_unloadables(method)
            
            method_set[method] = {
                'symbols': self._get_attribute(method, 'symbols'),
                'func': self._get_attribute(method, 'function'),
                'queue': {},
                'workers': {}
            }

        # Run multiple workers
        try:
            for i in range(workers):
                for method in methods:
                    method_set[method]['queue'][i] = multithreadingQueue()
                    
                    if method == 'cp68':
                        args = (
                            method_set[method]['queue'][i],
                            method_set[method]['func'],
                            method_set[method]['symbols'],
                            records,
                            cp68_username,
                            cp68_password,
                            cp68_data_source
                        )
                    elif method == 'cp68_mass_download':
                        args = (
                            method_set[method]['queue'][i],
                            method_set[method]['func'],
                            method_set[method]['symbols'],
                            cp68_username,
                            cp68_password,
                            cp68_data_source,
                            cp68_to_folder
                        )
                    else:
                        args = (
                            method_set[method]['queue'][i],
                            method_set[method]['func'],
                            method_set[method]['symbols'],
                            date_format,
                            records
                        )
                    
                    method_set[method]['workers'][i] = Thread(
                        target=self._queue_function,
                        args=args
                    )
            
            for i in range(workers):
                for method in methods:
                    method_set[method]['workers'][i].start()
            
            for i in range(workers):
                for method in methods:
                    method_set[method]['workers'][i].join()
            
        except KeyboardInterrupt:
            print('Keyboard interupted')
        
        finally:
            self._multi_workers_in_use = False
        
        # Combine all resulted dataframe to one result set
        result_df = pd.DataFrame()
        result_set = []

        for method in methods:

            if method == 'cp68_mass_download':
                continue
            
            logger.info(f'{method} - Joining results')
            
            for i in range(workers):
                if method_set[method]['queue'][i].empty():
                    continue
                
                temp_df = method_set[method]['queue'][i].get()
                
                result_df = pd.concat([result_df, temp_df], sort=True)
                logger.info(f'{method} - Thread {i} - Query result '
                            'appended to consolidated dataframe')
            
            if not result_df.empty:            
                logger.info(f'{method} - Updating {method} class dataframe...')
                # Update class dataframe
                if method == 'cp68':
                    df_name = '_'.join(['df', cp68_data_source])
                    self._update_class_df(method, result_df, df_name)
                else:
                    self._update_class_df(method, result_df)
            
                logger.info(f'{method} - Appending results to result set...')
                result_set.append(result_df)
                logger.info(f'{method} - Results are added to result set')
            
            else:
                logger.error(f'{method} - Method returns null dataframe')
        
        return result_set


    def vnd_get_data(self, symbols_array, date_format=None, records=None, retries=2):
        """
        This method scrapes price data from VNDirect website

        Parameter:
        symbols_array   :   1-dimensional or 2-dimensional list/tuple
            template    :   (str(symbol), str(from_date), str(to_date))
            example     :   (('AAA', datetime.date(2019, 12, 1), datetime.date(2019, 12, 31))
                            ('A32', datetime.date(2020, 1, 1), datetime.date(2020, 1, 10)))

        Return:
        A pandas DataFrame of the price data. Default indexes: symbol and date.
        """

        logger.info('VNDirect - vnd_get_data method is triggered')
        method = 'vnd'

        page_prop = self._get_attribute(method, 'properties')

        browser = SelBrowsing(self.driver, self.driver_exe, self.headless)
        browser.input_page_prop(page_prop)
        browser.load_page()
        
        if not self._multi_workers_in_use:
            self._update_class_symbols(method, symbols_array, date_format)
            self._clear_unloadables(method)
        
        try:

            result_df = self._load_html_data(method, browser, records)
            unloadables = self._get_attribute(method, 'unloadables')
            
            attempts = 1
            while attempts < retries and unloadables:
                logger.info(f'Retry to get data for {len(unloadables)} '
                            f'unloadable symbol(s). Attempt {attempts}')
                
                symbols_array = self._get_attribute(method, 'symbols')
                for symbol_data in unloadables:
                    self._update_status(
                        symbols_array,
                        symbol_data,
                        'to_check'
                    )
                temp_df = self._load_html_data(method, browser, records)
                result_df = qfutils.merge_dedup(result_df, temp_df, 'concat')
                attempts += 1
        
        except KeyboardInterrupt:
            print('Keyboard Interrupted')
        
        finally:
            browser.close()

        if not result_df.empty or len(result_df) > 0:
            result_df = qfutils.validate_index(
                df=result_df,
                symbol_index=self.symbol_index,
                date_index=self.date_index,
                sort_index=True
            )
        
        if not self._multi_workers_in_use:
            # Update VNDirect dataframe
            self._update_class_df(method, result_df)

        return result_df


    def cafef_get_data(self, symbols_array, date_format=None, records=None, retries=2):
        """
        This method scrapes price data from CafeF website

        Parameter:
        symbols_array   :   1-dimensional or 2-dimensional list/tuple
            template    :   (str(symbol), str(from_date), str(to_date))
            example     :   (('AAA', datetime.date(2019, 12, 1), datetime.date(2019, 12, 31))
                            ('A32', datetime.date(2020, 1, 1), datetime.date(2020, 1, 10)))

        Return:
        A pandas DataFrame of the price data. Default indexes: symbol and date.
        """
        
        logger.info('CafeF - cafef_get_data method is triggered')
        method = 'cafef'

        page_prop = self._get_attribute(method, 'properties')

        browser = SelBrowsing(self.driver, self.driver_exe, self.headless)
        browser.input_page_prop(page_prop)
        browser.load_page()
        
        if not self._multi_workers_in_use:
            self._update_class_symbols(method, symbols_array, date_format)
            self._clear_unloadables(method)
        
        try:

            result_df = self._load_html_data(method, browser, records)
            unloadables = self._get_attribute(method, 'unloadables')
            
            attempts = 1
            while attempts < retries and unloadables:
                logger.info(f'Retry to get data for {len(unloadables)} '
                            f'unloadable symbol(s). Attempt {attempts}')
                
                symbols_array = self._get_attribute(method, 'symbols')
                for symbol_data in unloadables:
                    self._update_status(
                        symbols_array,
                        symbol_data,
                        'to_check'
                    )
                temp_df = self._load_html_data(method, browser, records)
                result_df = qfutils.merge_dedup(result_df, temp_df, 'concat')
                attempts += 1
        
        except KeyboardInterrupt:
            print('Keyboard Interrupted')
        
        finally:
            browser.close()

        if not result_df.empty or len(result_df) > 0:
            result_df = qfutils.validate_index(
                df=result_df,
                symbol_index=self.symbol_index,
                date_index=self.date_index,
                sort_index=True
            )
        
        if not self._multi_workers_in_use:
            # Update CafeF dataframe
            self._update_class_df(method, result_df)

        return result_df


    def vcsc_get_data(self, symbols_array, date_format=None, records=None, retries=2):
        """
        This method scrapes price data from CafeF website

        Parameter:
        symbols_array   :   1-dimensional or 2-dimensional list/tuple
            template    :   (str(symbol), str(from_date), str(to_date))
            example     :   (('AAA', datetime.date(2019, 12, 1), datetime.date(2019, 12, 31))
                            ('A32', datetime.date(2020, 1, 1), datetime.date(2020, 1, 10)))

        Return:
        A pandas DataFrame of the price data. Default indexes: symbol and date.
        """
        
        logger.info('VCSC - vcsc_get_data method is triggered')
        method = 'vcsc'

        page_prop = self._get_attribute(method, 'properties')

        browser = SelBrowsing(self.driver, self.driver_exe, self.headless)
        browser.input_page_prop(page_prop)
        browser.load_page()
        
        if not self._multi_workers_in_use:
            self._update_class_symbols(method, symbols_array, date_format)
            self._clear_unloadables(method)
        
        try:

            result_df = self._load_html_data(method, browser, records)
            unloadables = self._get_attribute(method, 'unloadables')
            
            attempts = 1
            while attempts < retries and unloadables:
                logger.info(f'Retry to get data for {len(unloadables)} '
                            f'unloadable symbol(s). Attempt {attempts}')
                
                symbols_array = self._get_attribute(method, 'symbols')
                for symbol_data in unloadables:
                    self._update_status(
                        symbols_array,
                        symbol_data,
                        'to_check'
                    )
                temp_df = self._load_html_data(method, browser, records)
                result_df = qfutils.merge_dedup(result_df, temp_df, 'concat')
                attempts += 1

            regex = 'open|high|low|close'
            price_cols = result_df.filter(regex, axis=1).columns
            result_df[price_cols] = result_df[price_cols].div(1e3)

            regex = 'vol_value'
            vol_cols = result_df.filter(regex, axis=1).columns
            result_df[vol_cols] = result_df[vol_cols].mul(1e9)
        
        except KeyboardInterrupt:
            print('Keyboard Interrupted')
        
        finally:
            browser.close()

        if not result_df.empty or len(result_df) > 0:
            result_df = qfutils.validate_index(
                df=result_df,
                symbol_index=self.symbol_index,
                date_index=self.date_index,
                sort_index=True
            )
        
        if not self._multi_workers_in_use:
            # Update VCSC dataframe
            self._update_class_df(method, result_df)

        return result_df
    

    def sb_get_data(self, symbols_array, date_format=None, records=None, retries=2):
        """
        This method scrapes price data from CafeF website

        Parameter:
        symbols_array   :   1-dimensional or 2-dimensional list/tuple
            template    :   (str(symbol), str(from_date), str(to_date))
            example     :   (('AAA', datetime.date(2019, 12, 1), datetime.date(2019, 12, 31))
                            ('A32', datetime.date(2020, 1, 1), datetime.date(2020, 1, 10)))

        Return:
        A pandas DataFrame ofv the price data. Default indexes: symbol and date.
        """
        
        logger.info('Stockbiz - sb_get_data method is triggered')
        method = 'sb'

        page_prop = self._get_attribute(method, 'properties')

        browser = SelBrowsing(self.driver, self.driver_exe, self.headless)
        browser.input_page_prop(page_prop)
        browser.load_page()
        
        if not self._multi_workers_in_use:
            self._update_class_symbols(method, symbols_array, date_format)
            self._clear_unloadables(method)
        
        try:

            result_df = self._load_html_data(method, browser, records)
            unloadables = self._get_attribute(method, 'unloadables')
            
            attempts = 1
            while attempts < retries and unloadables:
                logger.info(f'Retry to get data for {len(unloadables)} '
                            f'unloadable symbol(s). Attempt {attempts}')
                
                symbols_array = self._get_attribute(method, 'symbols')
                for symbol_data in unloadables:
                    self._update_status(
                        symbols_array,
                        symbol_data,
                        'to_check'
                    )
                temp_df = self._load_html_data(method, browser, records)
                result_df = qfutils.merge_dedup(result_df, temp_df, 'concat')
                attempts += 1
        
        except KeyboardInterrupt:
            print('Keyboard Interrupted')
        
        finally:
            browser.close()

        if not result_df.empty or len(result_df) > 0:
            result_df = qfutils.validate_index(
                df=result_df,
                symbol_index=self.symbol_index,
                date_index=self.date_index,
                sort_index=True
            )
        
        if not self._multi_workers_in_use:
            # Update Stockbiz dataframe
            self._update_class_df(method, result_df)

        return result_df
    

    def cp68_get_data(
        self,
        symbols='all',
        records=None,
        login_username=None,
        login_password=None,
        data='mt4'
    ):
        logger.info('CP68 - cp68_get_data method is triggered')
        method = 'cp68'

        if dt.now().weekday() < 5 and dt.now().time() >= t(8)\
            and dt.now().time() <= t(15):
            logger.warning('CP68 - The website could disapprove data download during trading hours!')
        
        if not login_username or not login_password:
            logger.error('Login username and password is not provided')
            raise ValueError
        
        if data not in ('mt4', 'xls'):
            logger.error('CP68 - the selected data must be '
                         'either "mt4" or "xls"')
            raise ValueError
        
        if not isinstance(symbols, list):
            symbols = [symbols]
        
        # Open page and login
        page_prop = self._get_attribute(method, 'properties')
        page_prop['login_form']['fields']['username'] = login_username
        page_prop['login_form']['fields']['tpassword'] = login_password
        
        browser = Bs4Browsing()
        browser.input_page_prop(page_prop)
        browser.load_page()
        
        logger.info('Start to download the data')

        text_data, link_data = browser.get_text_link_data()
        
        if not self._multi_workers_in_use:
            if symbols == ['all']:
                symbols = [text[0] for text in text_data]
            
            self._update_class_symbols(method, symbols, False)
            self._clear_unloadables(method)

        try:
            result_df = self._load_file_data(
                method=method,
                browser=browser,
                text_data=text_data,
                link_data=link_data,
                data=data,
                records=records
            )
        
        except KeyboardInterrupt:
            print('Keyboard Interrupted')
        
        finally:
            browser.close()
        
        
        if not result_df.empty or len(result_df) > 0:
            result_df = qfutils.validate_index(
                df=result_df,
                symbol_index=self.symbol_index,
                date_index=self.date_index,
                sort_index=True
            )
        
        if not self._multi_workers_in_use:        
            df_name = '_'.join(['df', data])
            self._update_class_df(method, result_df, df_name)

        return result_df
     

    def cp68_mass_download(
        self,
        symbols='all',
        login_username=None,
        login_password=None,
        data='all',
        to_folder=None
    ):
        logger.info('CP68 - cp68_mass_download method is triggered')
        method = 'cp68_mass_donwload'
        
        if dt.now().weekday() < 5 and dt.now().time() >= t(8)\
            and dt.now().time() <= t(15):
            logger.warning('CP68 - The website could disapprove data download during trading hours!')
        
        if not login_username or not login_password:
            logger.error('Login username and password is not provided')
            raise ValueError
        
        if data not in (
            'mt4', 'xls', 'fin_report',
            'fin_index', 'events', 'all'
        ):
            logger.error('CP68 - the selected data must be'
                        'one of the following: "mt4", "xls", '
                        '"fin_report", "fin_index", "events", "all"')
            raise ValueError
        
        if not to_folder:
            logger.error('Folder directory is not provided')
            raise ValueError
        
        if not isinstance(symbols, list):
            symbols = [symbols]      

        if not isinstance(data, list):
            data = [data]
                
       # Open page and login
        page_prop = self._get_attribute(method, 'properties')
        page_prop['login_form']['fields']['username'] = login_username
        page_prop['login_form']['fields']['tpassword'] = login_password
        
        browser = Bs4Browsing()
        browser.input_page_prop(page_prop)
        browser.load_page()
        
        logger.info('Start to download the data')

        text_data, link_data = browser.get_text_link_data()
        
        if not self._multi_workers_in_use:
            if symbols == ['all']:
                symbols = [text[0] for text in text_data]
            
            self._update_class_symbols(method, symbols, False)
            self._clear_unloadables(method)

        try:
            for datum in data:
                self._load_file_data(
                    method=method,
                    browser=browser,
                    text_data=text_data,
                    link_data=link_data,
                    data=datum,
                    to_folder=to_folder
                )
        
        except KeyboardInterrupt:
            print('Keyboard Interrupted')
        
        finally:
            browser.close()

        logger.info('CP68 - Download completed')

        return
    

    def _load_html_data(self, method, browser, records=None):
        symbols_array = self._get_attribute(method, 'symbols')
        unloadables = self._get_attribute(method, 'unloadables')
        html_reader = self._get_attribute(method, 'html_reader')

        try:
            for symbol_data in symbols_array:
                if symbol_data[3] in ('in_progress', 'checked'):
                    continue

                self._update_status(
                    symbols_array,
                    symbol_data,
                    'in_progress'
                )
                
                # Scrape the website
                browser.scrape_data(
                    method=html_reader,
                    symbol_data=symbol_data,
                    records=records
                )

                self._update_unloadables(
                    unloadables,
                    symbol_data,
                    browser.load_status
                )
                
                self._update_status(
                    symbols_array,
                    symbol_data,
                    'checked'
                )
                
                self._announce_progress(
                    symbols_array,
                    unloadables
                )
            
        except:
            pass
        
        return browser.consolidated_df
    

    def _load_file_data(
        self,
        method,
        browser,
        text_data,
        link_data,
        data,
        records=None,
        to_folder=None
    ):
        symbols_array = self._get_attribute(method, 'symbols')
        unloadables = self._get_attribute(method, 'unloadables')
        page_prop = self._get_attribute(method, 'properties')
        data_idx = page_prop[data]['idx']

        try:
            symbol_col = [row[0] for row in text_data]
            
            for symbol_data in symbols_array:
                if symbol_data[1] in ('in_progress', 'checked'):
                    continue
                
                symbol = symbol_data[0]
                
                logger.info(f'{symbol} - Start reading data')

                self._update_status(
                    symbols_array,
                    symbol_data,
                    'in_progress',
                    1
                )
                
                try:
                    idx = symbol_col.index(symbol)

                    link = link_data[idx][data_idx]
                
                    browser.cp68_extract_data(
                        symbol,
                        link,
                        data,
                        records,
                        to_folder
                    )

                    self._update_unloadables(
                        self.cp68_unloadables,
                        symbol_data,
                        browser.load_status
                    )
                
                except:
                    self._update_unloadables(
                        self.cp68_unloadables,
                        symbol_data,
                        False
                    )

                finally:    
                    self._update_status(
                        symbols_array,
                        symbol_data,
                        'checked',
                        1
                    )
                
                    self._announce_progress(
                        symbols_array,
                        unloadables,
                        1
                    )

                    random.seed()
                    sleep(random.random())
        
        except:
            pass
        
        if to_folder:
            return
        else:
            return browser.consolidated_df
    

    def _queue_function(self, queue, function, *args, **kwargs):
        temp_df = function(*args, **kwargs)
        queue.put(temp_df)
        
        return temp_df
    

    def _announce_progress(
        self,
        symbols_array,
        unloadables,
        col=3
    ):
        checked_symbols = [
            sym_data for sym_data in symbols_array
            if sym_data[col] == 'checked'
        ]
        
        total_count = len(symbols_array)
        checked_count = len(checked_symbols)
        unloaded_count = len(unloadables)
        
        progress = ''.join([str(round((checked_count)*100/total_count, 2)), '%'])
        
        logger.info(f'{checked_count}/{total_count} symbol(s) checked. '
                    f'Progress: {progress}')
        logger.info(f'{unloaded_count} symbol(s) not fully loaded.')

        return
    

    def _update_status(self, symbols_array, symbol_data, status, col=3):
        try:
            idx = symbols_array.index(symbol_data)
            symbols_array[idx][col] = status
        except:
            logger.warning(f'{symbol_data[0]} - Cannot update '
                           'symbol status', exc_info=True)

        return
    

    def _update_class_df(self, method, additive_df, df_name=None):
        if not additive_df.empty or len(additive_df) > 0:
            if method == 'vnd':
                self.vnd_df = qfutils.merge_dedup(self.vnd_df, additive_df, 'concat')
            elif method == 'cafef':
                self.cafef_df = qfutils.merge_dedup(self.cafef_df, additive_df, 'concat')
            elif method == 'vcsc':
                self.vcsc_df = qfutils.merge_dedup(self.vcsc_df, additive_df, 'concat')
            elif method == 'sb':
                self.sb_df = qfutils.merge_dedup(self.sb_df, additive_df, 'concat')
            elif method in ('cp68', 'cp68_mass_download'):
                if df_name == 'df_mt4':
                    self.cp68_df_mt4 = qfutils.merge_dedup(
                        self.cp68_df_mt4, additive_df, 'concat'
                    )
                elif df_name == 'df_xls':
                    self.cp68_df_xls = qfutils.merge_dedup(
                        self.cp68_df_xls, additive_df, 'concat'
                    )
                else:
                    logger.error(f'There is no dataframe named {df_name}')
            else:
                logger.error(f'Method {method} is not recognized - Only 5 methods '
                            'are eligible: vnd, cafef, vcsc, sb, and cp68')

        return
    

    def _update_class_symbols(self, method, symbols_array, date_format):
        symbols_array = qfutils.validate_symbols(symbols_array, date_format)
        class_symbols_array = self._get_attribute(method, 'symbols')
        class_symbols_array.clear()
        class_symbols_array.extend(symbols_array)
        
        return
    

    def _clear_unloadables(self, method):
        unloadables = self._get_attribute(method, 'unloadables')
        unloadables.clear()
        
        return
    
    
    def _update_unloadables(
        self,
        original_list,
        symbol_data,
        load_status
    ):
        if load_status:
            if symbol_data in original_list:
                original_list.remove(symbol_data)
                logger.info(f'{symbol_data[0]} '
                             '- Data are successfully loaded. '
                             'To be removed from unloadables list')
        else:
            if symbol_data not in original_list:
                original_list.append(symbol_data)
                logger.warning(f'{symbol_data[0]} '
                                '- Data are NOT loaded completely')

        return
    

    def _get_attribute(self, method, attribute):
        if method == 'vnd':
            if attribute == 'df':
                return self.vnd_df
            elif attribute == 'symbols':
                return self.vnd_symbols_array
            elif attribute == 'unloadables':
                return self.vnd_unloadables
            elif attribute == 'properties':
                return self.vnd_prop
            elif attribute == 'function':
                return self.vnd_get_data
            elif attribute == 'html_reader':
                return 'bs4'
            else:
                logger.error(f'Attribute {attribute} '
                             'is not recogizned')
                raise ValueError
        
        elif method == 'cafef':
            if attribute == 'df':
                return self.cafef_df
            elif attribute == 'symbols':
                return self.cafef_symbols_array
            elif attribute == 'unloadables':
                return self.cafef_unloadables
            elif attribute == 'properties':
                return self.cafef_prop
            elif attribute == 'function':
                return self.cafef_get_data
            elif attribute == 'html_reader':
                return 'pandas'
            else:
                logger.error(f'Attribute {attribute} '
                             'is not recogizned')
                raise ValueError
        
        elif method == 'vcsc':
            if attribute == 'df':
                return self.vcsc_df
            elif attribute == 'symbols':
                return self.vcsc_symbols_array
            elif attribute == 'unloadables':
                return self.vcsc_unloadables
            elif attribute == 'properties':
                return self.vcsc_prop
            elif attribute == 'function':
                return self.vcsc_get_data
            elif attribute == 'html_reader':
                return 'pandas'
            else:
                logger.error(f'Attribute {attribute} '
                             'is not recogizned')
                raise ValueError
        
        elif method == 'sb':
            if attribute == 'df':
                return self.sb_df
            elif attribute == 'symbols':
                return self.sb_symbols_array
            elif attribute == 'unloadables':
                return self.sb_unloadables
            elif attribute == 'properties':
                return self.sb_prop
            elif attribute == 'function':
                return self.sb_get_data
            elif attribute == 'html_reader':
                return 'pandas'
            else:
                logger.error(f'Attribute {attribute} '
                             'is not recogizned')
                raise ValueError

        elif method == 'cp68':
            if attribute == 'df_mt4':
                return self.cp68_df_mt4
            elif attribute == 'df_xls':
                return self.cp68_df_xls
            elif attribute == 'symbols':
                return self.cp68_symbols_array
            elif attribute == 'unloadables':
                return self.cp68_unloadables
            elif attribute == 'properties':
                return self.cp68_prop
            elif attribute == 'function':
                return self.cp68_get_data
            elif attribute == 'html_reader':
                return 'bs4'
            else:
                logger.error(f'Attribute {attribute} '
                             'is not recogizned')
                raise ValueError
        
        elif method == 'cp68_mass_download'\
            and not self._dump_data_to_var:
            if attribute == 'df_mt4':
                return self.cp68_df_mt4
            elif attribute == 'df_xls':
                return self.cp68_df_xls
            elif attribute == 'symbols':
                return self.cp68_symbols_array
            elif attribute == 'unloadables':
                return self.cp68_unloadables
            elif attribute == 'properties':
                return self.cp68_prop
            elif attribute == 'function':
                return self.cp68_mass_download
            elif attribute == 'html_reader':
                return 'bs4'
            else:
                logger.error(f'Attribute {attribute} '
                             'is not recogizned')
                raise ValueError
        
        else:
            if self._dump_data_to_var:
                msg = 'vnd, cafef, vcsc, sb and cp68'
                count = 5
                full_msg_1 = f'Method {method} is not recognized.'
                full_msg_2 = f'Only {count} methods are eligible: {msg}'
            else:
                msg = 'vnd, cafef, vcsc, sb, cp68 and cp68_mass_download'
                count = 6
                full_msg_1 = f'Method {method} is not recognized.'
                full_msg_2 = f'Only {count} methods are eligible: {msg}'
            
            logger.error(' '.join([full_msg_1, full_msg_2]))
            raise ValueError


class Portfolio(DataFeeder):

    def __init__(self, data, symbol_col, date_col, driver_exe, driver='chrome'):
        DataFeeder.__init__(self)
        self.data = self._initiate_data(data, symbol_col, date_col)
        self.symbol_index = symbol_col
        self.date_index = date_col
        self.data['issues_detected'] = False
        self.driver_exe = driver_exe
        self.driver = driver
    

    def _initiate_data(self, data, symbol_col, date_col):
        if not data or data.empty:
            data = pd.DataFrame()
        else:
            data = qfutils.validate_index(
                qfutils.validate_dataframe(data),
                symbol_col,
                date_col,
            )

        return data
    

    def _get_symbols(self):
        if self.data.empty:
            symbols_array = []
        
        else:
            try:
                symbols_array = list(
                    self.data.index.get_level_values(self.symbol_index).unique()
                )
                # symnols = self.data.index.levels[0].tolist()
            except:
                logger.error('Cannot retrieve list of symbols '
                             f'from column {self.symbol_index}', exc_info=True)
                raise
        
        return symbols_array


    def update_data(
        self,
        symbols_array,
        method='vnd',
        date_format=None,
        records=None,
        cp68_username=None,
        cp68_password=None,
        cp68_data_source=None,
        workers=8
    ):
        self._dump_data_to_var = True

        if symbols_array == 'all':
            symbols_array = self._get_symbols()
        
        try:
            try:
                if len(symbols_array) < workers:
                    workers = len(symbols_array)
            
                additive_df = self.multithreading(
                    methods=method,
                    symbols_array=symbols_array,
                    date_format=date_format,
                    records=records,
                    cp68_username=cp68_username,
                    cp68_password=cp68_password,
                    cp68_data_source=cp68_data_source,
                    cp68_to_folder=None,
                    workers=workers
                )
                additive_df = additive_df[0]
            except:
                logger.error('Cannot extract data from web with '
                             f'assigned method {method}', exc_info=True)
                raise
            
            try:
                # self.data = self.data.join(additive_df, how='outer')
                # self.data = self.data.groupby(level=self.data.index.names).first()
                self.data = qfutils.merge_dedup(self.data, additive_df, 'join')
            except:
                logger.error('Cannot append web-extracted data to '
                             'original dataframe')
                raise
        
        except:
            raise
        
        finally:
            self._dump_data_to_var = False
        
        return
    

    def interpolate(self, column, method='linear'):
        self._dump_data_to_var = True

        try:
            symbols = self._get_symbols()
            for symbol in symbols:
                values = self.data.loc[symbol, column].interpolate(method)
                self.data.loc[symbol, column] = values.values

        except:
            logger.error(f'Cannot run {method} interpolate method '
                         'on the provided dataframe')
            raise
        
        finally:
            self._dump_data_to_var = False
        
        return
    

    def fill_na(self, na_column, value, inplace=False):
        self._dump_data_to_var = True

        try:
            self.data.loc[:, na_column].fillna(value, inplace=inplace)

        except:
            logger.error(f'Cannot fill missing values in {na_column} '
                         f'with values from {value}', exc_info=True)
            raise
        
        finally:
            self._dump_data_to_var = False
        
        return
    

    def find_missing_dates(self, holidays=[]):
        logger.info('Running find_missing_dates method...')

        final_index = []
        symbols = self._get_symbols()
        for symbol in symbols:

            old_date_index = self.data.loc[symbol, :].index

            min_date = old_date_index.min()
            max_date = old_date_index.max()
            
            logger.info(f'{symbol} - Checking dates from '
                        f'{str(min_date)[:10]} to {str(max_date)[:10]}...')

            new_date_index = pd.bdate_range(
                min_date, max_date, freq='C', holidays=holidays
            )
            new_date_index = new_date_index.union(old_date_index)
            new_index = [
                item for item in itertools.product([symbol], new_date_index)
            ]
            
            final_index.extend(new_index)
            
            logger.info(f'{symbol} - There is/are '
                        f'{len(new_date_index)-len(old_date_index)} '
                        'missing date(s).')
        
        final_index = pd.MultiIndex.from_tuples(final_index)
        
        # Create a dummy DF with the new index
        dummy_df = pd.DataFrame(columns=['dummy'], index=final_index)
        dummy_df.index.names = [self.symbol_index, self.date_index]
        
        logger.info(f'Original DataFrame: {len(self.data)} rows '
                    f'x {len(self.data.columns)} columns')

        # Full join the dummy DF with original DF to add new indices
        self.data = self.data.join(dummy_df, how='outer')
        self.data.drop(columns='dummy', inplace=True)
        self.data.sort_index(
            level=[self.symbol_index, self.date_index],
            inplace=True
        )

        logger.info(f'New DataFrame: {len(self.data)} rows '
                    f'x {len(self.data.columns)} columns')
        
        return
    

    def find_na(self, columns=[]):
        if not columns:
            columns = list(self.data.columns)

        if not isinstance(columns, list) and not isinstance(columns, tuple):
            columns = [columns]

        logger.info('Running find_na method...')
        
        self.data['is_na'] = False
        self.data['na_fields'] = [[]] * len(self.data)
        
        for column in columns:
            try:
                self.data['is_na'] = self.data[column].isna() | self.data['is_na']
                self.data['na_fields'] = self.data['na_fields'].add(
                    self.data['is_na'].apply(
                        lambda is_na: [column] if is_na else []
                    )
                )
            except:
                logger.error(f'Encounter error with column {column}!', exc_info=True)
        
        logger.info('Validation done')
        logger.info('Two new columns are added: [\'is_na\'] and [\'na_fields\']')
        
        self.data['issues_detected'] = self.data['is_na'] | self.data['issues_detected']

        return
    

    def find_outliers(self, columns, lookback_period=20, significance=0.2):
        if not isinstance(columns, list) and not isinstance(columns, tuple):
            columns = [columns]
        
        logger.info('Running find_outliers method...')

        self.data['outliers_detected'] = False
        self.data['outlier_fields'] = [[]] * len(self.data)

        for column in columns:
            try:
                self.data['moving_avg'] = self.data[column].rolling(lookback_period).mean()
                validation = abs(self.data[column]/self.data['moving_avg'] - 1) >= significance
                self.data['outliers_detected'] = validation | self.data['outliers_detected']
                self.data['outlier_fields'] = self.data['outlier_fields'].add(
                    self.data['outliers_detected'].apply(
                        lambda outliers_detected: [column] if outliers_detected else []
                    )
                )
            except:
                logger.error(f'Encounter error with column {column}!', exc_info=True)
        
        self.data.drop(columns='moving_avg', inplace=True)
        
        logger.info('Validation done')
        logger.info('Two new columns are added: [\'outliers_detected\'] and [\'outlier_fields\']')
        
        self.data['issues_detected'] = self.data['outliers_detected'] | self.data['issues_detected']
        
        return


    def find_differences(self, base_columns, ref_columns):
        if len(base_columns) != len(ref_columns):
            logger.error('Length of base columns and ref columns are not the same!')
            raise ValueError

        logger.info('Running find_differences method...')
        
        self.data['base_differs_ref'] = False
        self.data['differ_fields'] = [[]] * len(self.data)
        
        for base_col, ref_col in zip(base_columns, ref_columns):
            try:
                validation = self.data[base_col] != self.data[ref_col]
                self.data['base_differs_ref'] = validation | self.data['base_differs_ref']
                self.data['differ_fields'] = self.data['differ_fields'].add(
                    self.data['base_differs_ref'].apply(
                        lambda base_diff_ref: ['-'.join([base_col, ref_col])] if base_diff_ref else []
                    )
                )
            except:
                logger.error(f'Encounter error with column {base_col}-{ref_col}!', exc_info=True)
        
        logger.info('Validation done')
        logger.info('Two new columns are added: [\'base_differs_ref\'] and [\'differ_fields\']')
        
        self.data['issues_detected'] = self.data['base_differs_ref'] | self.data['issues_detected']
        
        return
