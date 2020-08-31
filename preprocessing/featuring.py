import numpy as np
import pandas as pd
import re
import copy
from itertools import combinations, combinations_with_replacement
from sklearn.base import BaseEstimator, TransformerMixin


def train_backtest_split(data, level=0, from_year=None):
    if not from_year:
        raise ValueError('Parameter from_year must be specified')
    
    train_set = data.loc[data.index.get_level_values(level).year < from_year]
    backtest_set = data.loc[data.index.get_level_values(level).year >= from_year]
    
    return train_set, backtest_set


def calculate_target_sharpe(data, groupby, input_field, forward_look=5):
    data['daily_ret'] = data.groupby(groupby)[input_field].pct_change()
    data['exp_ret'] = data.groupby(groupby)['daily_ret'].shift(-1).rolling(forward_look).mean().shift(-forward_look + 1)
    data['exp_ret_std'] = data.groupby(groupby)['daily_ret'].shift(-1).rolling(forward_look).apply(lambda x: np.std(x, ddof=1)).shift(-forward_look + 1)
    temp_exp_ret_std = data['exp_ret_std'].replace(0, 10e-20)
    data['exp_sharpe'] = data['exp_ret'].values/temp_exp_ret_std.values
    
    return data


def calculate_hist_sharpe(data, groupby, input_field, *windows):
    if not windows:
        windows = [10]
    
    # groups = data.groupby(groupby).groups.keys()
    data['daily_ret'] = data.groupby(groupby)[input_field].pct_change()
    
    # for group in groups:
        # cust_query = f'{groupby} == {[group]}'
        # query_eval = data.eval
    for window in windows:
        data[f'daily_ret_avg_{window}'] = data.groupby(groupby)['daily_ret'].shift(0).rolling(window).mean()
        data[f'daily_ret_std_{window}'] = data.groupby(groupby)['daily_ret'].shift(0).rolling(window).apply(lambda x: np.std(x, ddof=1))
        temp_daily_ret_std = data[f'daily_ret_std_{window}'].replace(0, 10e-20)
        data[f'sharpe_{window}'] = data[f'daily_ret_avg_{window}'].values/temp_daily_ret_std.values

    return data


def calculate_hist_volume(data, groupby, input_field, *windows):
    if not windows:
        windows = [10]
    
    for window in windows:
        data[f'vol_avg_{window}'] = data.groupby(groupby)[input_field].shift(0).rolling(window).mean()
        data[f'vol_std_{window}'] = data.groupby(groupby)[input_field].shift(0).rolling(window).apply(lambda x: np.std(x, ddof=1))
    
    return data


def filter_volume(data, date_field, volume_field, symbol_field='symbol', window=None, quantile=0.25, unaffected=None):
    if window and symbol_field:
        data[f'temp_avg_{volume_field}'] = data.groupby(symbol_field)[volume_field].shift(0).rolling(window).mean()
    else:
        data[f'temp_avg_{volume_field}'] = data[volume_field].copy()
    
    data = get_q_threshold(data, date_field, f'temp_avg_{volume_field}', quantile=quantile)

    # Get symbols
    try:
        symbols = data.index.get_level_values(symbol_field)
    except:
        try:
            symbols = data[symbol_field]
        except Exception as e:
            raise ValueError(e)
    
    # Define filter conditions
    unaffected_cond = symbols.isin(unaffected)
    quantile_cond = data[f'temp_avg_{volume_field}'] >= data[f'{volume_field}_q{quantile}_by_{date_field}']
    data = data[quantile_cond | unaffected_cond]
    data.drop(columns=f'temp_avg_{volume_field}', inplace=True)
    
    return data


def get_q_threshold(data, dimension, value_field, symbol_field=None, window=None, quantile=0.25):
    if window and symbol_field:
        avg_vals = data.groupby(symbol_field)[[value_field]].shift(0).rolling(window).mean()
        result_col_suffix = f'avg_{window}_q{quantile}_by_{dimension}'
    else:
        avg_vals = data[[value_field]].copy()
        result_col_suffix = f'q{quantile}_by_{dimension}'
    
    try:
        avg_vals['dimension'] = data.index.get_level_values(dimension)
    except:
        avg_vals['dimension'] = data[dimension]

    avg_vals_quantile = avg_vals.groupby('dimension')[[value_field]].quantile(quantile)
    
    data.drop(columns=f'{value_field}_{result_col_suffix}', inplace=True, errors='ignore')
    data = data.join(avg_vals_quantile, on=dimension, how='left', rsuffix=f'_{result_col_suffix}')

    return data


def get_date_partittion(data, date_field):
    try:
        dates = data[date_field].copy()
    except:
        try:
            dates = pd.Series(data.index.get_level_values(date_field))
        except Exception as e:
            raise ValueError(e)
    
    data['wd'] = dates.dt.dayofweek.values
    data['month'] = dates.dt.month.values

    return data


def classify_sharpe(exp_ret, std, boundaries):
    if len(boundaries) != 2:
        raise ValueError('Argument boundaries only accepts 2 values')
    
    ret_boundary = boundaries[0]
    std_boundary = boundaries[1]
    
    exp_ret_cat = classify_exp_ret(exp_ret, ret_boundary)
    std_cat = classify_std(std, std_boundary)

    return exp_ret_cat * std_cat


def classify_exp_ret(exp_ret, boundary=0):
    cat_1 = (exp_ret <= boundary) * -1
    cat_2 = (exp_ret > boundary) * 1

    return cat_1 + cat_2


def classify_std(std, boundary=0.03):
    cat_1 = (std <= boundary) * 1
    cat_2 = (std > boundary) * 2

    return cat_1 + cat_2


# def enrich_features(data, columns, columns_2=None, methods=None, max_combinations=None, largest_val=3.4028235e38, smallest_val=1.175494e-39, return_all=True):
#     # Define maximum number of combinations
#     if not max_combinations or max_combinations > len(columns):
#         max_combinations = len(columns) + 1
#     else:
#         max_combinations += 1
    
#     args = [data, columns, largest_val, smallest_val]
#     args_w_comb = [data, columns, columns_2, max_combinations, largest_val, smallest_val]

#     # Substraction transformation
#     if not methods or 'substract' in methods:
#         data = _substract_transform(*args_w_comb)
    
#     # Inner multiplication transformation
#     if not methods or 'mult' in methods:
#         data = _substract_transform(*args_w_comb)
    
#     # Reciprocal transformation
#     if not methods or 'reci' in methods:
#         data = _reciprocal_transform(*args)
    
#     # Logarithm transformation
#     if not methods or 'log' in methods:
#         data = _logarit_transform(*args)
    
#     # Exponential transformation
#     if not methods or 'exp' in methods:
#         data = _exp_transform(*args)
    
#     # Square root transformation
#     if not methods or 'sqrt' in methods:
#         data = _sqrt_transform(*args)
    
#     if not return_all:
#         selected_cols = [col for col in data.columns if col not in columns]
#         data = data[selected_cols]
    
#     return data


def remove_low_cardinity(data, dimension='date', smallest=10):
    count_check = data.groupby(dimension).agg(['count']).iloc[:, 0]
    to_include = count_check[count_check >= smallest].index.tolist()
    
    cust_query = f'{dimension} == {to_include}'
    data = data.query(cust_query)
    
    return data


def rescale(data, scaler, columns=None, dimension=None, prefit=False, return_all=True, suffix=''):
    if dimension:
        data = _dimensional_rescale(data.copy(), scaler, columns, dimension, prefit)
    else:
        data = _flat_rescale(data.copy(), scaler, columns, prefit)

    if columns and suffix:
        data = _rename_columns(data, columns, suffix)
    
    if return_all:
        return data
    else:
        return data[columns]


def _dimensional_rescale(data, scaler, columns, dimension, prefit):
    groups = list(data.groupby(dimension).groups.keys())
    for group in groups:
        cust_query = f'{dimension} == {[group]}'

        if isinstance(scaler, dict):
            sub_scaler = scaler[group]
        else:
            sub_scaler = scaler

        if columns:
            data.loc[data.eval(cust_query), columns] = _scaler_fit_transform(data.query(cust_query)[columns], sub_scaler, prefit)
        else:
            data.loc[data.eval(cust_query), :] = _scaler_fit_transform(data.query(cust_query), sub_scaler, prefit)
    
    return data


def _flat_rescale(data, scaler, columns, prefit):
    if isinstance(scaler, list):
        scaler = scaler[0]
    
    if columns:
        data.loc[:, columns] = _scaler_fit_transform(data.loc[:, columns], scaler, prefit)
    else:
        scaler.fit(data)
        data.loc[:, :] = _scaler_fit_transform(data, scaler, prefit)
    
    return data


def _scaler_fit_transform(data, scaler, prefit):
    if prefit == True:
        return scaler.transform(data)
    elif prefit == False:
        return scaler.fit_transform(data)
    else:
        raise ValueError('Argument prefit must be a boolean value!')


def get_scalers(data, scaler, columns=None, dimension=None):
    if dimension:
        return _get_dimensional_scalers(data, scaler, columns, dimension)
    else:
        return _get_flat_scalers(data, scaler, columns)


def _get_dimensional_scalers(data, scaler, columns, dimension):
    scalers = {
        'scaled_columns': columns,
        'scalers': {}
    }
    groups = list(data.groupby(dimension).groups.keys())
    for group in groups:
        cust_query = f'{dimension} == {[group]}'

        if columns:
            scaler.fit(data.query(cust_query)[columns])
        else:
            scaler.fit(data.query(cust_query))
        
        scalers['scalers'][group] = copy.copy(scaler)
    
    return scalers


def _get_flat_scalers(data, scaler, columns):
    if columns:
        scaler.fit(data.loc[:, columns])
    else:
        scaler.fit(data)
    
    return [scaler]


def _rename_columns(data, columns, suffix):
    columns = {k: ''.join([v, suffix]) for (k, v) in zip(columns, columns)}
    data.rename(columns=columns, inplace=True)

    return data



def search_thetas(exp_ret, std, seed_thetas=[0, 0.5], min_nclasses=3, precision=0.05, learning_rate=0.1, lr_reduction=0.5, iterations=1000, excl_null=True, excl_outliers=True):
    if len(seed_thetas) != 2:
        raise ValueError('Argument init_thetas only accepts 2 values')

    if len(exp_ret) != len(std):
        raise ValueError('Arguments exp_ret and std must have the same length')

    if iterations < 2:
        raise ValueError('Iteration must equal or is greater than 2')

    # Convert to array for easy calculation
    init_thetas = np.asarray(seed_thetas)
    steps = np.array([0, learning_rate])
    
    # Get list of chunks
    chunks = _get_chunks(iterations)
    
    for i, chunk in enumerate(chunks):
        sharpe_cat, new_thetas, mae = _gradient_search(
            exp_ret,
            std,
            init_thetas,
            steps,
            min_nclasses,
            chunk)
        
        steps = np.flip(steps)
        # Check if learning steps need to be reduced
        if i%2 == 1:
            steps = _reduce_steps(init_thetas, new_thetas, steps, lr_reduction)
            print('Mean absolute error:', mae)
        
        init_thetas = new_thetas.copy()

        if mae <= precision:
            break
    
    print(f'Complete {sum(chunks[:i+1])} iterations. Lowest mae achieved: {mae}.')
    
    return sharpe_cat, init_thetas


def _drop_na(exp_ret, std):
    sharpe = exp_ret/std
    data = np.column_stack((exp_ret, std, sharpe))
    data = data[~np.isnan(data).any(axis=1)]

    return data[:, 0], data[:, 1]



def _reduce_steps(pre_thetas, new_thetas, steps, reduction_factor=0.5):
    if (pre_thetas == new_thetas).all():
        steps = steps * reduction_factor
    
    return steps



def _gradient_search(exp_ret, std, thetas, steps, min_nclasses, iterations, sequence=3):
    mae_hist = []
    thetas_hist = []
    sharpe_hist = []
    classes_hist = []
    
    while iterations > 0:
        # Classify sharpe ratio
        sharpe_cat = classify_sharpe(exp_ret, std, thetas)
        mae = _mean_absolute_error(sharpe_cat)
        classes = len(np.unique(sharpe_cat))
        
        # Update memories
        thetas_hist.append(thetas)
        sharpe_hist.append(sharpe_cat)
        mae_hist.append(mae)
        classes_hist.append(classes)
        
        # Re-define directional factor
        factor = _redirect(mae_hist, classes_hist, min_nclasses, sequence)
        steps = steps * factor
        
        # Calculate new thetas
        thetas = thetas - (1/2) * steps * mae
        
        iterations -= 1
    
    # Get index of minimum values
    try:
        min_idx = _get_minval_idx(mae_hist, classes_hist, min_nclasses)
    except:
        msg1 = 'Cannot find thetas that satisfy minimum unique classes requirement.'
        msg2 = 'Try to change the initial thetas value.'
        raise ValueError(' '.join([msg1, msg2]))
    
    return sharpe_hist[min_idx], thetas_hist[min_idx], mae_hist[min_idx]


def _get_chunks(total_length):
    if total_length <= 50:
        standard_subsize = int(total_length/2)
    elif total_length <= 200:
        standard_subsize = 50
    else:
        standard_subsize = 100

    # Calcualte temporary number of chunks
    size = int(total_length/standard_subsize)
    excess = int(size%2)
    size = size + excess
    
    # Use the temporry number of chunks
    # to calculate number of elements of one chunk
    subsize = int(total_length/size)
    chunks = [subsize] * size
    # The excess elements will be added to the last chunk
    excess = total_length%size
    chunks[-1] += excess

    return chunks


def _mean_absolute_error(category_array):
    total = len(category_array)
    unique, counts = np.unique(category_array, return_counts=True)
    classes = len(unique)
    benchmark = 1/classes

    mae = np.mean(abs(counts/total - benchmark))
    
    return mae


def _get_minval_idx(mae_hist, classes_hist, min_nclasses):
    mae_hist = np.asarray(mae_hist)
    classes_hist = np.asarray(classes_hist)
    
    mae_hist = mae_hist * (classes_hist >= min_nclasses)
    min_val = np.min(mae_hist[np.nonzero(mae_hist)])
    idx = np.where(mae_hist == min_val)
    
    return idx[0][0]


def _redirect(mae_hist, classes_hist, min_nclasses, sequence, rev_factor=0.05):
    # Check if errors are increasing and number of classes are decreasing
    error_cond = _error_increases(mae_hist, sequence)
    nclasses_cond = _nclasses_decreases(classes_hist, min_nclasses, sequence)

    if error_cond or nclasses_cond:
        # If errors are increasing and number of classes are decreasing, reverse the direction
        # magnitue is a function of sequence and rev_factor
        return -(sequence * (1 - rev_factor))
    else:
        # Else, continue with the same magnitude
        return 1


def _error_increases(mae_hist, sequence):
    error_is_increasing = False
    
    if len(mae_hist) >= sequence:
        latest_items = mae_hist[-(sequence + 1):]
        diff = np.diff(latest_items) >= 0
        error_is_increasing = sum(diff) == sequence
    
    return error_is_increasing


def _nclasses_decreases(classes_hist, min_nclasses, sequence):
    min_nclasses_found = False
    nclasses_is_decreasing = False
    
    if len(classes_hist) >= sequence:
        latest_items = classes_hist[-sequence:]
        min_nclasses_found = sum([c >= min_nclasses for c in classes_hist]) > 0
        nclasses_is_decreasing = sum([c < min_nclasses for c in latest_items]) == sequence

    return min_nclasses_found & nclasses_is_decreasing
