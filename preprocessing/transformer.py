# Basic data manipulation libraries
import pandas as pd
import numpy as np

# sklearn libraries
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler, MinMaxScaler, Normalizer, KBinsDiscretizer

# Stats library
from statsmodels.tsa.stattools import adfuller

# Other libraries
import copy
import warnings
from itertools import combinations, permutations


class KBinsScaler(BaseEstimator, TransformerMixin):
    def __init__(self, feature_names, n_bins=100, encode='ordinal', strategy='quantile', date_field='date', from_n_day=100):
        self.feature_names = self._to_list(feature_names)
        self.bin_scalers = {}
        self.date_field = date_field
        self.distinct_dates = None
        self.base_scaler = KBinsDiscretizer(n_bins=n_bins, encode=encode, strategy=strategy)
        self.strategy = strategy
        self.from_n_day = from_n_day
    
    
    def fit(self, X, y=None):
        self.distinct_dates = self._get_distinct_dates(X.copy())

        return self.partial_fit(X.copy())
    

    def transform(self, X, y=None):
        X_copy = self._add_columns(X.copy())
        
        for date in self.distinct_dates[self.from_n_day:]:
            scaler = self.bin_scalers[date]
            query = f"{self.date_field} == '{date.strftime('%Y-%m-%d')}'"
            X_copy.loc[X.eval(query), self.new_features] = scaler.transform(X.query(query)[self.feature_names])
        
        return X_copy.drop(columns=self.feature_names)

    def partial_fit(self, X, y=None):
        for date in self.distinct_dates[self.from_n_day:]:
            scaler = copy.copy(self.base_scaler)
            query = f"{self.date_field} <= '{date.strftime('%Y-%m-%d')}'"
            scaler.fit(X.query(query)[self.feature_names])
            self.bin_scalers[date] = copy.copy(scaler)
        
        return self
    

    def _get_distinct_dates(self, X):
        return list(X.groupby(self.date_field).groups.keys())
    

    def _add_columns(self, X):
        self.new_features = ['_'.join([self.strategy, feature]) for feature in self.feature_names]
        new_columns = {feature: np.nan for feature in self.new_features}
        X = X.assign(**new_columns)
        
        return X
    

    def _to_list(self, feature_names):
        if isinstance(feature_names, str):
            return [feature_names]
        else:
            return list(feature_names)


class DimScaler(BaseEstimator, TransformerMixin):
    def __init__(self, base_scaler, feature_names, dimension=None, suffix=''):
        self.base_scaler = base_scaler
        self.feature_names = feature_names
        self.dimension = dimension
        self.suffix = suffix
        self.scalers = {}
    

    def fit(self, X, y=None):
        try:
            self.dims = list(X.groupby(self.dimension).groups.keys())
        except:
            self.dims = None
        
        return self._partial_fit(X, self.dims)
    

    def transform(self, X, y=None):
        X_copy = X.copy()

        if self.dimension:
            dims = list(X.groupby(self.dimension).groups.keys())
            self._validate_dim(X, dims)
            
            for dim in dims:
                scaler = self.scalers[dim]
                query = f'{self.dimension} == {[dim]}'
                X_copy.loc[X.eval(query), self.feature_names] = scaler.transform(X.query(query)[self.feature_names])
        
        else:
            X_copy.loc[:, self.feature_names] = self.scalers['no_dim'].transform(X_copy[self.feature_names])
        
        return self._rename_columns(X_copy)
    

    def _validate_dim(self, X, dims):
        aliens = [item for item in dims if item not in self.dims]
        if aliens:
            self.dims.extend(aliens)
            msg1 = f'There are {len(aliens)} dimensional items do not exist in pre-fitted scaler.'
            msg2 = f'fit() method will be applied for those dimension items before transforming.'
            msg = ' '.join([msg1, msg2])
            warnings.warn(msg)

            try:
                sub_X = X[X[self.dimension].isnin(aliens)]
            except:
                sub_X = X[X.index.get_level_values(self.dimension).isin(aliens)]
            
            self._partial_fit(sub_X, aliens)

        return self
    
    
    def _partial_fit(self, X, dims):
        if dims:
            for dim in dims:
                scaler = copy.copy(self.base_scaler)
                query = f'{self.dimension} == {[dim]}'
                scaler.fit(X.query(query)[self.feature_names])
                self.scalers[dim] = copy.copy(scaler)
        
        else:
            scaler = copy.copy(self.base_scaler)
            scaler.fit(X[self.feature_names])
            self.scalers['no_dim'] = copy.copy(scaler)
        
        return self
    

    def _rename_columns(self, X):
        if self.suffix:
            columns = {col: ''.join([col, self.suffix]) for col in self.feature_names}
            X.rename(columns=columns, inplace=True)

        return X


class DimQuantileCalculator(BaseEstimator, TransformerMixin):
    def __init__(self, feature_names, dimension, quantile, on_collision='remove_old'):
        self.feature_names = list(feature_names)
        self.dimension = dimension
        self.quantile = quantile
        self.on_collision = on_collision
        self.quantiled_X = pd.DataFrame()
    

    def fit(self, X, y=None):
        self.dims = list(X.groupby(self.dimension).groups.keys())
        
        return self._partial_fit(X)
    

    def transform(self, X, y=None):
        dims = list(X.groupby(self.dimension).groups.keys())
        self._validate_dim(X, dims)
        
        return self._append_data(X.copy())
    

    def _validate_dim(self, X, dims):
        aliens = [item for item in dims if item not in self.dims]
        if aliens:
            self.dims.extend(aliens)
            msg1 = f'There are {len(aliens)} dimensional items do not exist in pre-fitted scaler.'
            msg2 = f'fit() method will be applied for those dimension items before transforming.'
            warnings.warn(' '.join([msg1, msg2]))

            try:
                sub_X = X[X[self.dimension].isnin(aliens)]
            except:
                sub_X = X[X.index.get_level_values(self.dimension).isin(aliens)]
            
            self._partial_fit(sub_X)

        return self
    

    def _append_data(self, X):
        if self.on_collision == 'remove_old':
            X.drop(columns=self.quantiled_X.columns, errors='ignore', inplace=True)
        
        X = X.join(self.quantiled_X, on=self.dimension, how='left', rsuffix='_new')

        return X
    
    
    def _partial_fit(self, X):
        quantiled_X = X.groupby(self.dimension)[self.feature_names].quantile(self.quantile)
        quantiled_X = self._rename_columns(quantiled_X)

        if self.quantiled_X.empty:
            self.quantiled_X = quantiled_X
        else:
            self.quantiled_X = pd.concat([self.quantiled_X, quantiled_X], axis=1)
        
        return self


    def _rename_columns(self, X):
        suffix = f'q{self.quantile}_by_{self.dimension}'
        columns = {col: '_'.join([col, suffix]) for col in self.feature_names}
        X.rename(columns=columns, inplace=True)

        return X


class ArithTransformer(BaseEstimator, TransformerMixin):
    def __init__(
        self,
        feature_names=None,
        compare_feature_names=None,
        operations=None,
        get_combinations=True,
        targets=None,
        components=None,
        largest_val=3.4028235e38,
        smallest_val=1.175494e-39
    ):
        self.feature_names = feature_names
        self.compare_feature_names = compare_feature_names
        self.operations = operations
        self.get_combinations = get_combinations
        self.targets = targets
        self.components = components
        self.largest_val = largest_val
        self.smallest_val = smallest_val
        self.ops_dict = dict()
        self.base_operations = ['reci', 'log', 'exp', 'sqrt', 'mult', 'subs']
    

    def fit(self, X, y=None):
        if not self.feature_names:
            msg = 'feature_names must be provided to perform fit() method!'
            raise ValueError(msg)

        self._validate_operations(self.operations)
        # self.ops_dict = dict()
        
        for operation in self.operations:
            if operation in ('reci', 'log', 'exp', 'sqrt'):
                self.ops_dict[operation] = self.feature_names
            else:
                self.ops_dict[operation] = self._combine_features()
        
        return self
    

    def transform(self, X, y=None):
        for operation, cols in self.ops_dict.items():
            try:
                operator = self._get_operator(operation)
                X = operator(X.copy(), cols)
            except:
                pass
        
        return X


    def reverse_fit(self, targets, components):
        self.targets = targets
        self.components = components
        
        # Split the target results to smaller components
        features = np.asarray(list(filter(None, map(self._get_components, self.targets))))
        operations = list(np.unique(features[:, 0]))
        # return components, operations
        # Validate the operations
        self._validate_operations(operations)

        # Create an empty operations dict
        self.ops_dict = {operation: [] for operation in operations}

        # Fill operations dict with extracted components
        for operation, first, second in features:
            if second:
                self.ops_dict[operation].append((first, second))
            else:
                self.ops_dict[operation].append(first)
        
        return self
    

    def _get_components(self, target):
        try:
            features, operation = self._split_suffix(target)
            first, second = self._split_features(features)
            return operation, first, second
        except:
            pass
    

    def _split_suffix(self, target):
        for operation in self.base_operations:
            if target.endswith(f'_{operation}'):
                features = target.rstrip(f'{operation}').rstrip('_')
                return features, operation
    

    def _split_features(self, combined_name):
        for feature in sorted(self.components, key=len, reverse=True):
            if combined_name.startswith(feature):
                first = feature
                break
        
        second = combined_name.replace(first, '', 1).lstrip('_')
        
        return first, second
    

    def _get_operator(self, operation):
        if operation == 'reci':
            return self._reciprocal_transform
        elif operation == 'log':
            return self._logarit_transform
        elif operation == 'exp':
            return self._exp_transform
        elif operation == 'sqrt':
            return self._sqrt_transform
        elif operation == 'mult':
            return self._multiply_transform
        elif operation == 'subs':
            return self._substract_transform


    def _substract_transform(self, data, columns):
        for col1, col2 in columns:
            subs_result = data[col1].values - data[col2].values
            
            if self._validate_xtrm(subs_result):
                col_result = '_'.join([col1, col2]) + '_subs'
                data[col_result] = subs_result
        
        return data


    def _multiply_transform(self, data, columns):
        for col1, col2 in columns:
            mult_result = data[col1].values * data[col2].values
            
            if self._validate_xtrm(mult_result):
                col_result = '_'.join([col1, col2]) + '_mult'
                data[col_result] = mult_result
        
        return data


    def _reciprocal_transform(self, data, columns):
        for col in columns:
            if (data[col].values == 0).any():
                continue

            reci_result = 1/data[col].values
            if self._validate_xtrm(reci_result):
                data[f'{col}_reci'] = reci_result
        
        return data


    def _logarit_transform(self, data, columns):
        for col in columns:
            if (data[col].values <= 0).any():
                continue

            log_result = np.log(data[col].values)
            if self._validate_xtrm(log_result):
                data[f'{col}_log'] = log_result
        
        return data


    def _exp_transform(self, data, columns):
        for col in columns:
            exp_result = np.exp(data[col].values)
            if self._validate_xtrm(exp_result):
                data[f'{col}_exp'] = exp_result
        
        return data


    def _sqrt_transform(self, data, columns):
        for col in columns:
            if (data[col].values < 0).any():
                continue
            
            sqrt_result = np.sqrt(data[col].values)
            if self._validate_xtrm(sqrt_result):
                data[f'{col}_sqrt'] = sqrt_result
        
        return data


    def _validate_xtrm(self, values):
        not_nan = ~(np.isnan(values).all())
        not_exceed_max = ~((abs(values) >= self.largest_val).any())
        not_exceed_min = ~(((abs(values) > 0) & (abs(values) < 1) & (abs(values) < self.smallest_val)).any())

        return not_nan & not_exceed_max & not_exceed_min
    

    def _validate_operations(self, operations):
        for operation in operations:
            if operation not in self.base_operations:
                avail_operations = ', '.join(self.base_operations)
                msg1 = f'Operation {operations} is not recognized.'
                msg2 = f'Available operations: {avail_operations}.'
                msg = ' '.join([msg1, msg2])
                raise ValueError(msg)
        
        return
    
    
    def _combine_features(self):
        if not self.compare_feature_names:
            result = list(combinations(self.feature_names, 2))
        
        else:
            if self.get_combinations:
                result = []
                for col1 in self.feature_names:
                    for col2 in self.compare_feature_names:
                        if col1 != col2 and (col2, col1) not in result:
                            result.append((col1, col2))
            else:
                result = list(zip(self.feature_names, self.compare_feature_names))
        
        return result
    

class RollingStatsCalculator():
    def __init__(self, feature_names, symbol_col, stats_val, hurst_max_range=0.05):
        self.feature_names = self._to_list(feature_names)
        self.symbol_col = symbol_col
        self.stats_val = stats_val
        self.hurst_max_range = hurst_max_range
    

    def transform(self, data, *windows):
        data_copy = data.copy()
        symbols = data_copy.index.get_level_values(self.symbol_col).unique()
        features = [self._engineer_features(data.loc[symbol, self.feature_names], *windows) for symbol in symbols]
        features = np.concatenate(features, axis=0)

        feat_window_combinations = [(feature, window) for feature in self.feature_names for window in windows ]
        for i, (feature, window) in enumerate(feat_window_combinations):
            new_col = f'{feature}_rolling_{self.stats_val}_n{window}'
            data_copy[new_col] = features[:, i]

        return data_copy
    

    def _engineer_features(self, data, *windows):
        cols = data.columns
        features = [self._run_pipeline(data[col].values, *windows) for col in cols]
        
        return self._combine_features(*features)

    
    def _run_pipeline(self, values, *windows):
        features = []
        for window in windows:
            if len(values) <= window:
                vectors = np.zeros((len(values), 1))
                vectors.fill(np.nan)
                features.append(vectors)
            else:
                series = self._get_rolling_subsets(values, window)
                stats_vals = self._get_stats_val(series)
                nan_array = np.empty((window-1, 1))
                nan_array.fill(np.nan)
                features.append(np.concatenate((nan_array, stats_vals.reshape(-1, 1))))
        
        return self._combine_features(*features)
    

    def _get_stats_val(self, series):
        if self.stats_val == 'percentile':
            return self._get_percentile_val(series)
        elif self.stats_val == 'adf_pvalue':
            return self._get_adf_val(series)
        elif self.stats_val == 'hurst':
            return self._get_hurst_val(series)
    

    def _get_percentile_val(self, series):
        return np.nan_to_num((series[:, -1] - series.min(axis=1)) / (series.max(axis=1) - series.min(axis=1)))
    

    def _get_adf_val(self, series):
        return np.apply_along_axis(self.__adf_sub_func, 1, series)
    

    def _get_hurst_val(self, series):
        return np.apply_along_axis(self.__hurst_sub_func, 1, series)
    

    def __adf_sub_func(self, sub_series):
        adf_val = adfuller(sub_series)
        if not adf_val[1]:
            return 1.00
        else:
            return adf_val[1]

    def __hurst_sub_func(self, sub_series):
        lags = range(2, int(len(sub_series)*self.hurst_max_range))
        # Calculate the array of the variances of the lagged differences
        tau = [self.__tau_sub_func(sub_series, lag) for lag in lags]

        # Use a linear fit to estimate the Hurst Exponent
        poly = np.polyfit(np.log(lags), np.log(tau), 1)
        
        return poly[0] * 2
    

    def __tau_sub_func(self, sub_series, lag):
        lags_var = np.sqrt(
            np.std(np.subtract(sub_series[lag:], sub_series[:-lag]))
        )
        if lags_var == 0:
            return 1e-10
        else:
            return lags_var
    

    def _get_rolling_subsets(self, values, window):
        as_strided = np.lib.stride_tricks.as_strided
        v = as_strided(values, (len(values) - (window - 1), window), (values.strides * 2))
        
        return v
    

    def _combine_features(self, *features):
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
    

    def _to_list(self, feature_names):
        if isinstance(feature_names, str):
            return [feature_names]
        else:
            return list(feature_names)
