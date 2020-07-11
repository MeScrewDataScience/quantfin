import numpy as np


def calculate_target_sharpe(data, groupby, input_field, forward_look=5):
    data['daily_ret'] = data.groupby(groupby)[input_field].pct_change()
    data['exp_ret'] = data.groupby(groupby)['daily_ret'].shift(-1).rolling(forward_look).mean().shift(-forward_look + 1)
    data['sq_sigma'] = data.groupby(groupby)['daily_ret'].shift(-1).rolling(forward_look).apply(lambda x: np.std(x, ddof=1)).shift(-forward_look + 1)
    data['sq_sigma'].replace(0, 10e-20, inplace=True)
    data['sharpe'] = data['exp_ret']/data['sq_sigma']
    
    return data


def rescale_data(data, scaler, columns=None):
    transformer = scaler.fit_transform
    if not columns:
        columns = data.columns
    
    for col in columns:
        data[col] = data[col].apply(transformer)
    
    return data


def train_backtest_split(data, level=0, from_year=None):
    if not from_year:
        raise ValueError('Parameter from_year must be specified')

    backtest_set = data.loc[data.index.get_level_values(level).year >= from_year]
    remainder_set = data.loc[data.index.get_level_values(level).year < from_year]
    
    return remainder_set, backtest_set
