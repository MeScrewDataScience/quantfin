import pandas as pd
import numpy as np
from itertools import repeat


def create_rolling_subsets(values, window=10):
    as_strided = np.lib.stride_tricks.as_strided
    v = as_strided(values, (len(values) - (window - 1), window), (values.strides * 2))
    
    return v


def standardize_values(values, scaler):
    # If scaling data is not required, return original values
    if not scaler:
        return values
    
    else:
        transformer = scaler.fit_transform
        shape = values.shape
        scaled_values = transformer(values.reshape(shape[0], 1))

        return scaled_values


def standardize_size(elem, standard_shape):
    base = np.zeros(standard_shape)
    base[-len(elem):] += elem

    return base


def combine_features(*features):
    features = np.asarray(features)
    lens = list(map(len, features))
    idx = lens.index(min(lens))
    last_axis = len(np.shape(features[idx])) - 1

    for i, __ in enumerate(features):
        features[i] = np.asarray(features[i])
        
        if i != idx:
            excess_rows = lens[i] - min(lens)
            features[i] = features[i][excess_rows:]
    
    result = np.concatenate(features, axis=last_axis)
    
    return result


def pipeline(values, scaler=None, *windows):
    features = []
    standard_shape = (max(windows), 1)
    for window in windows:
        vectorized_subsets = create_rolling_subsets(values, window)
        scaled_subsets = list(map(standardize_values, vectorized_subsets, repeat(scaler)))
        resized_subsets = list(map(standardize_size, scaled_subsets, repeat(standard_shape)))
        features.append(resized_subsets)
    
    features = combine_features(*features)

    return features


def engineer_features(data, scaler=None, *windows):
    cols = data.columns
    features = [pipeline(data[col].values, scaler, *windows) for col in cols]
    features = combine_features(*features)
    
    return features


def resize_targets(data, max_window):
    targets = data.values
    shape_0 = targets.shape[0]
    shape_1 = len(data.columns)
    targets = targets.reshape(shape_0, shape_1)
    targets = targets[max_window-1:]

    return targets


def transform_timesteps(data, target_cols, scaler=None, *windows):
    symbols = data.index.get_level_values('symbol').unique()
    cols = data.columns.drop(target_cols)
    
    features = [engineer_features(data.loc[symbol, cols], scaler, *windows) for symbol in symbols]
    features = np.concatenate(features, axis=0)

    targets = [resize_targets(data.loc[symbol, target_cols], max(windows)) for symbol in symbols]
    targets = np.concatenate(targets, axis=0)

    return features, targets


def flatten(arr, *steps):
    arr = arr.reshape(len(arr), np.product(arr.shape[1:]))
    max_step = max(steps)
    steps *= int(arr.shape[1]/len(steps))
    result = np.empty((len(arr), 0), float)

    for i, step in enumerate(steps):
        if step < max_step:
            temp = arr[:, max_step*(i+1)-step:max_step*(i+1)]
            result = np.append(result, temp, axis=1)
        else:
            temp = arr[:, max_step*i:max_step*(i+1)]
            result = np.append(result, temp, axis=1)
    
    return result