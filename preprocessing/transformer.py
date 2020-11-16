# Basic data manipulation libraries
import pandas as pd
import numpy as np

# sklearn libraries
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler, MinMaxScaler, Normalizer, KBinsDiscretizer

# Other libraries
import copy
import warnings
from itertools import combinations, permutations


class KBinsScaler(BaseEstimator, TransformerMixin):
    def __init__(self, feature_names, n_bins=100, encode='ordinal', strategy='quantile', date_field='date'):
        self.feature_names = self._to_list(feature_names)
        self.bin_scalers = {}
        self.date_field = date_field
        self.distinct_dates = None
        self.base_scaler = KBinsDiscretizer(n_bins=n_bins, encode=encode, strategy=strategy)
        self.strategy = strategy
    
    
    def fit(self, X, y=None):
        self.distinct_dates = self._get_distinct_dates(X.copy())

        return self.partial_fit(X)
    

    def transform(self, X, y=None):
        X_copy = X.copy()
        
        for date in self.distinct_dates:
            scaler = self.bin_scalers[date]
            query = f"{self.date_field} == '{date.strftime('%Y-%m-%d')}'"
            X_copy.loc[X.eval(query), self.feature_names] = scaler.transform(X.query(query)[self.feature_names])
        
        return self._rename_columns(X_copy)


    def partial_fit(self, X, y=None):
        for date in self.distinct_dates:
            scaler = copy.copy(self.base_scaler)
            query = f"{self.date_field} <= '{date.strftime('%Y-%m-%d')}'"
            scaler.fit(X.query(query)[self.feature_names])
            self.bin_scalers[date] = copy.copy(scaler)
        
        return self
    

    def _get_distinct_dates(self, X):
        return list(X.groupby(self.date_field).groups.keys())
    

    def _rename_columns(self, X):
        columns = {feature: '_'.join([self.strategy, feature]) for feature in self.feature_names}
        
        return X.rename(columns=columns, inplace=True)
    

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


class DimQuantile(BaseEstimator, TransformerMixin):
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
    

    # def _drop_repeated(self, pairs):
