# -*- coding: utf-8 -*-

# Import standard libraries
import json
import locale
import string
import logging
import logging.config
from collections import defaultdict
from datetime import date as date, datetime as dt
from pathlib import Path
from urllib.parse import urlparse

# Import third-party libraries
import numpy as np
import pandas as pd

# Import local modules
from quantfin.logconfig import logging_config


locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


config = logging_config()
logging.config.dictConfig(config)
logger = logging.getLogger(__name__)
 
def validate_dataframe(df):
    
    if not isinstance(df, pd.DataFrame):
        logger.warning('The input Data are not a pandas.DataFrame object. '
                        'It will be converted to pandas.DataFrame object. '
                        'There could be some unexpected erros arising thereon.')
    
        df = pd.DataFrame(df)
    
    return df


def validate_index(df, symbol_index, date_index, sort_index=True):

    if symbol_index not in df.index.names:
        if symbol_index not in df.columns:
            logger.error('There is no column indicating the symbol column')
            raise ValueError
        else:
            logger.warning('Symbol field is not an index. '
                           'It will be made an index')
    
    if date_index not in df.index.names:
        if date_index not in df.columns:
            logger.error('There is no column indicating the trading dates')
            raise ValueError
        else:
            logger.warning('Date field is not an index. '
                           'It will be made an index')
    
    df = _customized_set_index(df, symbol_index, date_index, sort_index)

    return df


def validate_symbols(symbols_array, date_format):

    logger.info('Start validating symbols array...')
    
    symbols_array = _redim_2d(symbols_array)
    invalid_array = []

    for i, symbol_data in enumerate(symbols_array):
        symbols_array[i], is_valid = _validate_symbol_items(
            symbol_data,
            date_format
        )
        
        if not is_valid:
            invalid_array.append(symbol_data)
    
    if len(invalid_array) > 0:
        symbols_array = [
            symbols_array.remove(symbol_data) for symbol_data in invalid_array
        ]
        logger.warning(f'Invalid symbols: {invalid_array}.\n'
                       'Those symbols will be removed from '
                       'the symbols list')
    
    logger.info('Symbols array validation done')
    
    return symbols_array


def split_array(parrent_array, no_of_sub_array):
    
    if not isinstance(parrent_array, list) and not isinstance(parrent_array, tuple):
        logger.warning('The input argument must be either list or tuple. '
                        'The input argument will be converted to tuple')
        
        parrent_array = tuple(parrent_array)
    
    parrent_array_len = len(parrent_array)
    sub_array_len = round(parrent_array_len/no_of_sub_array)

    sub_arrays = []
    start = 0
    end = sub_array_len
    
    for i in range(no_of_sub_array):
        if i < parrent_array_len - 1:
            sub_arrays.append(parrent_array[start:end])
            start = end
            end += sub_array_len
        else:
            sub_arrays.append(parrent_array[start:])
    
    return sub_arrays


def make_symbols_array(obj):

    if not isinstance(obj, list) and not isinstance(obj, tuple):
        logger.error('The input argument must be either list or tuple')
        logger.error(f'The object type is {type(obj)}')
        raise ValueError
    
    if np.ndim(obj) > 1:
        logger.error('The input argument must be '
                        '1-dimensional list/tupple')
        raise ValueError
    
    obj = [[item, None, None] for item in obj]

    return obj
    

def merge_dedup(original_df, additive_df, method):

    ori_is_df = isinstance(original_df, pd.DataFrame)
    add_is_df = isinstance(additive_df, pd.DataFrame)
    cond_1 = original_df is None or original_df.empty or not ori_is_df
    cond_2 = additive_df is None or additive_df.empty or not add_is_df

    if not cond_1 and cond_2:
        return original_df
    
    elif cond_1 and not cond_2:
        return additive_df
    
    elif cond_1 and cond_2:
        return pd.DataFrame()
    
    else:
        try:
            if method == 'concat':
                original_df = pd.concat([original_df, additive_df], sort=True)
            elif method == 'join':
                original_df = original_df.join(additive_df, how='outer', rsuffix='_add')
            else:
                logger.error('The data joining method is not recognized')
        
        except:
            logger.error('Cannot append the additional dataframe '
                         'to the original one', exc_info=True)
        
        try:
            original_df = original_df.groupby(
                level=original_df.index.names
            ).first()
        except:
            logger.error('Cannot remove duplicated records from dataframe',
                         exc_info=True)

    return original_df


def df_numericalize(
    df,
    columns=[],
    thousands=',',
    decimal='.',
    na=['-', '_', 'na', 'NA', 'n/a', 'N/A']):
    
    df = validate_dataframe(df)

    if thousands == decimal:
        logger.error('Thousands seperators and decimal seperator '
                     'are the same! Check again')
        raise ValueError

    if not columns:
        columns = df.columns
    
    for column in columns:
        if df[column].dtypes != 'object':
            continue
        
        # Remove leading and trailing redundant characters
        try:
            df[column] = df[column].apply(
                lambda val: val.replace(' ', '')
                    if ' ' in val else val
            )
            df[column] = df[column].str.rstrip('.')
        except:
            logger.warning('Failed to remove leading and trailing '
                           f'redundant characters for column [{column}]',
                           exc_info=True)
            pass
        
        # Remove thousands seperators
        try:
            df[column] = df[column].apply(
                lambda val: val.replace(thousands, '')
                    if thousands in val else val
            )
        except:
            logger.warning('Failed to remove thousands sepreators '
                        f'for column [{column}]', exc_info=True)
            pass

        # Standardize decimal seperator
        try:
            if decimal != '.':
                df[column] = df[column].apply(
                    lambda val: val[::-1]
                        if decimal in val else val
                )
                df[column] = df[column].apply(
                    lambda val: val.replace(decimal, '.', 1)
                        if decimal in val else val
                )
                df[column] = df[column].apply(
                    lambda val: val[::-1]
                        if decimal in val else val
                )
        except:
            logger.warning('Failed to standardize decimal seperator '
                        f'for column [{column}]', exc_info=True)
            pass

        # Convert NA values to zero
        try:
            df[column] = df[column].apply(
                lambda val: 0 if val in na else val
            )
        except:
            logger.warning('Failed to convert N/A values to zeroes '
                        f'for column {column}', exc_info=True)
            pass

        # Convert to numbers
        try:
            df[column] = pd.to_numeric(df[column])
        except:
            pass        
    
    return df


def standardize_folder_path(path):

    if not path:
        logger.error('The download folder path is not provided')
        raise ValueError
    
    path.replace('\\', '/')

    if path[-1] == '/':
        path = path[:-1]

    return path


def list_duplicates(sequence):
    
    tally = defaultdict(list)
    
    for i, item in enumerate(sequence):
        tally[item].append(i)
    
    result = ((key,locs) for key, locs in tally.items() if len(locs) > 1)
    
    return result


def _customized_set_index(df, symbol_index, date_index, sort_index):
    
    indexes = df.index.names
    cond_1 = symbol_index in indexes and date_index in indexes
    cond_2 = symbol_index == indexes[0] and date_index == indexes[1]
    cond_3 = len(indexes) == 2

    if not (cond_1 and cond_2 and cond_3):
        if cond_1 and cond_3 and not cond_2:
            df.swaplevel(0, 1)
        
        else:
            row_num = np.arange(len(df))
            idx_nlevels = df.index.nlevels
            
            try:
                for level in range(idx_nlevels):
                    idx_val = df.index.get_level_values(level)
                    is_num_idx = idx_val == row_num
                    is_range_idx = type(idx_val) == pd.RangeIndex
                    
                    if is_num_idx.all() or is_range_idx:
                        if idx_nlevels > 1:
                            df = df.droplevel(level)
                            df.reset_index(inplace=True)
                
                df.set_index([symbol_index, date_index], inplace=True)
            
            except:
                logger.warning('Cannot set dataframe index')
        
    if sort_index:
        try:
            df.sort_index(level=[symbol_index, date_index], inplace=True)
        except:
            logger.warning('Cannot sort index')
    
    return df
    

def _validate_symbol_items(symbol_data, date_format):
    
    symbol_data[0] = symbol_data[0].upper()
    is_valid = False

    if len(symbol_data) == 0:
        return symbol_data, is_valid

    if date_format is False:
        if len(symbol_data) == 1:
            symbol_data.append('to_check')
        else:
            symbol_data = symbol_data[:1]
            symbol_data[1] = 'to_check'
        
        is_valid = True
        
        return symbol_data, is_valid
    
    else:
        if len(symbol_data) == 1:
            symbol_data.append(None)
            symbol_data.append(None)
        
        elif len(symbol_data) == 2:
            symbol_data.append(symbol_data[1])
        
        elif len(symbol_data) > 3:
            symbol_data = symbol_data[:3]
            logger.warning(f'{symbol_data[0]} - The provided data elements '
                        f'are more than required: {symbol_data}. '
                        'It will be reduced to 3 elements.')
        
        symbol_data[1] = _str_to_date(symbol_data[1], date_format)
        symbol_data[2] = _str_to_date(symbol_data[2], date_format)
        symbol_data.append('to_check')
        
        if isinstance(symbol_data[1], date):
            if isinstance(symbol_data[2], date) or not symbol_data[2]:
                is_valid = True
        
        if not symbol_data[1] and not symbol_data[2]:
            is_valid = True
        
        if not is_valid:
            logger.warning(f'{symbol_data[0]} - The date formats are '
                        f'not correct: {symbol_data}')
        
        return symbol_data, is_valid


def _redim_2d(array):

    if not isinstance(array, list) and not isinstance(array, tuple):
        logger.warning('The input argument is not either list or tuple. '
                       'It will be converted to a list object. Unexpected '
                       'errors could occur thereon.')
        array = [array]

    if np.ndim(array) == 1:
        array = [[item] for item in array]
    elif np.ndim(array) == 2:
        array = list(map(list, array))
    else:
        logger.error('The input argument must not be '
                     'more than 2-dimensional list/tupple')
        raise ValueError
    
    return array
    

def _to_numeric(text, thousands=None, decimal=None, na_values=None):

    if na_values and text in na_values:
        return 0
    
    if not thousands and not decimal:
        try:
            text = float(text)
        except:
            pass
        
        return text
    
    if thousands:
        text.replace(thousands, '')
    
    if decimal:
        if decimal in text:
            if decimal != '.':
                text = text[::-1]
                text.replace(decimal, '.', 1)
                text = text[::-1]
            
            try:
                text = locale.atof(text)
            except:
                pass
        
        else:
            try:
                text = locale.atoi(text)
            except:
                pass
    
    return text


def _str_to_date(str_obj, date_format):

    try:
        str_obj = dt.strptime(str_obj, date_format)
    except:
        pass

    return str_obj


def _get_url_filename(url):

    parsed_url = urlparse(url)

    prefix = parsed_url.path
    prefix = prefix[prefix.rindex('/') + 1:prefix.rindex('.')]

    suffix = parsed_url.query[parsed_url.query.index('=') + 1:]
    suffix = suffix.translate(str.maketrans('', '', string.punctuation))
    
    filename = '_'.join([prefix, suffix])
    
    return filename


def _get_url_hostname(url):

    parsed_url = urlparse(url)
    url_hostname = f'{parsed_url.scheme}://{parsed_url.hostname}/'

    return url_hostname
    

def _set_columns(
    df,
    dict_cols,
    selected_cols,
    date_cols,
    result_date_format
):
    if not isinstance(date_cols, list):
        date_cols = [date_cols]
    
    if df.empty or len(df) == 0:
        logger.warning('Empty data')
    
    else:
        df.rename(columns=dict_cols, inplace=True)
        df = df.loc[:, selected_cols]

        for date_col in date_cols:
            df[date_col] = pd.to_datetime(
                df[date_col].astype('str'),
                format=result_date_format
            )

    return df
