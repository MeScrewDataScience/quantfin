# Basic data manipulation libraries
import pandas as pd
import numpy as np

# sklearn libraries
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler, MinMaxScaler, Normalizer

# Other libraries
import copy
import warnings


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
            warnings.warn(' '.join([msg1, msg2]))

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
        feature_names_2nd=None,
        operations=None,
        get_combinations=False,
        targets=None,
        components=None,
        largest_val=3.4028235e38,
        smallest_val=1.175494e-39
    ):
        self.feature_names = feature_names
        self.feature_names_2nd = feature_names_2nd
        self.operations = operations
        self.get_combinations = get_combinations
        self.targets = self.targets
        self.components = components
        self.largest_val = largest_val
        self.smallest_val = smallest_val
    

    def fit(self, X, y=None):
        if not self.feature_names:
            msg = 'feature_names must be provided to perform fit() method!'
            raise ValueError(msg)

        self._validate_operations(self.operations)
        self.ops_dict = dict()
        
        for operation in self.operations:
            if operation in ('reci', 'log', 'exp', 'sqrt'):
                self.ops_dict[operation] = self.feature_names
            else:
                self.ops_dict[operation] = self._combine_features()
        
        return self
    

    def transform(self, X, y=None):
        for operation, cols in self.ops_dict.items():
            operator = self._get_operator(operation)
            X = operator(X.copy(), cols)
        
        return X


    def reverse_fit(self, X, y=None):
        # Raise error if possible components for target results is not provided
        if not self.components:
            msg = 'Possible components of target results must be provided to perform reverse_fit() method!'
            raise ValueError(msg)
        
        # Split the target results to smaller components
        components = np.asarray(list(map(self._get_components, self.targets)))
        operations = list(np.unique(components[:, 0]))

        # Validate the operations
        self._validate_operations(operations)

        # Create an empty operations dict
        self.ops_dict = {operation:[] for operation in operations}

        # Fill operations dict with extracted components
        for operation, first, second in components:
            if operation in ('reci', 'log', 'exp', 'sqrt'):
                self.ops_dict[operation].extend(first)
            else:
                self.ops_dict[operation].extend((first, second))
        
        return self
    

    def _get_components(self, target):
        features, operation = self._split_suffix(target)
        first, second = self._split_features(features)

        return operation, first, second
    

    def _split_suffix(self, target):
        for operation in self.base_operations:
            if target.endswith(f'_{operation}'):
                features = target.rstrip(f'_{operation}')
                break
        
        return features, operation
    

    def _split_features(self, combined_name):
        for feature in sorted(self.components, key=len):
            if combined_name.startswith(feature):
                first = feature
                break
        
        second = combined_name.lstrip(first).rstrip('_')
        
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
                col_result = '_'.join(columns) + '_subs'
                data[col_result] = subs_result
        
        return data


    def _multiply_transform(self, data, columns):
        for col1, col2 in columns:
            mult_result = data[col1].values * data[col2].values
            
            if self._validate_xtrm(mult_result):
                col_result = '_'.join(columns) + '_mult'
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
            if (data[col].values == 0).any():
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
        for col in self.feature_names:
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

        return (not_nan & not_exceed_max & not_exceed_min).all()
    

    def _validate_operations(self, operations):
        self.base_operations = ['reci', 'log', 'exp', 'sqrt', 'mult', 'subs']
        for operation in operations:
            if operation not in self.base_operations:
                avail_operations = ', '.join(self.base_operations)
                msg1 = f'Operation {operations} is not recognized.'
                msg2 = f'Available operations: {avail_operations}.'
                msg = ' '.join([msg1, msg2])
                raise ValueError(msg)
        
        return
    
    
    def _combine_features(self):
        if not self.feature_names_2nd:
            second_components = self.feature_names.copy()
        else:
            second_components = self.feature_names_2nd
        
        if self.get_combinations:
            combinations = [(col1, col2) for col1 in self.feature_names for col2 in second_components]
        else:
            combinations = list(zip(self.feature_names, second_components))
        
        return combinations
