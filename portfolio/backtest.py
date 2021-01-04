# -*- coding: utf-8 -*-

# Import third-party libraries
import pandas as pd
import numpy as np
import itertools

# Import local module
import quantfin.portfolio.evaluation as eval

class MeanRevert():
    def __init__(self, dataframe):
        self.data = dataframe
        self.result = pd.DataFrame()

class Momentum():
    def __init__(self, dataframe):
        self.data = dataframe
        self.result = pd.DataFrame()

class Backtest():
    def __init__(
        self,
        dataframe,
        symbol_col='symbol',
        date_col='date',
        price_col='adj_close',
        vol_col='vol',
        daily_ret_col='daily_ret',
        pred_col='pred_val',
        pred_proba_col='up_proba',
        buy_strats=[
            # {
            #     'type': 'simple_compare',
            #     'col': 'vol_cv_100',
            #     'operation': '<=',
            #     'threshold': [1, 2, 5],
            # },
            {
                'type': 'simple_compare',
                'col': 'vol_avg_100',
                'operation': '>=',
                'threshold': [30000,]
            },
            {
                'type': 'simple_compare',
                'col': 'pred_val',
                'operation': 'in',
                'threshold': [[1, 2],]
            },
            # {
            #     'type': 'simple_compare',
            #     'col': 'adj_close',
            #     'operation': '>=',
            #     'threshold': [0, 5, 10]
            # },
            # {
            #     'type': 'simple_compare',
            #     'col': 'vol',
            #     'operation': '>=',
            #     'threshold': [30000]
            # },
            # {
            #     'type': 'double_compare',
            #     'col': ['up_proba_lr', 'up_proba_hr'],
            #     'operation': ['>=', '>='],
            #     'threshold': [[0.3, 0.3], ]
            # }
        ],
        sell_strats=[
            {
                'type': 'simple_compare',
                'col': 'pred_val',
                'operation': '<',
                'threshold': [0,]
            },
            {
                'type': 'holding_days',
                'col': None,
                'operation': '>=',
                'threshold': [3]
            },
            {
                'type': 'trailing_stoploss',
                'col': None,
                'operation': '>=',
                'threshold': [2]
            }
        ],
        portfolio_size=[25],
        low_risk_prop=[0.25],
        min_holding_days=3
    ):
        self.data = dataframe
        self.symbol_col = symbol_col
        self.date_col = date_col
        self.price_col = price_col
        self.vol_col = vol_col
        self.daily_ret_col = daily_ret_col
        self.pred_col = pred_col
        self.pred_proba_col = pred_proba_col
        self.buy_strats = buy_strats
        self.sell_strats = sell_strats
        self.portfolio_size = portfolio_size
        self.low_risk_prop = low_risk_prop
        self.min_holding_days = min_holding_days
        self._construct_pivoted_df()
        self._construct_backtest_results()
    

    def _construct_pivoted_df(self):
        self.data['daily_ret'] = self.data.groupby(self.symbol_col)[self.price_col].pct_change()
        self.pivoted_data = pd.pivot_table(self.data, values=self.data.columns, index=self.date_col, columns=[self.symbol_col])

        return self
    

    def _construct_backtest_results(self):
        # Define columns
        buy_strats_cols = ['_'.join([str(strat['col']), str(strat['operation'])]) for strat in self.buy_strats]
        sell_strats_cols = ['_'.join([str(strat['col']), str(strat['operation'])]) for strat in self.sell_strats if strat['type'] not in ['holding_days', 'trailing_stoploss']]
        sell_strats_cols_2 = ['_'.join([str(strat['type']), str(strat['operation'])]) for strat in self.sell_strats if strat['type'] in ['holding_days', 'trailing_stoploss']]
        sell_strats_cols.extend(sell_strats_cols_2)
        extra_cols = ['cumm_return', 'sharpe_ratio', 'trade_count', 'portfolio_size', 'low_risk_prop']
        all_cols = extra_cols + buy_strats_cols + sell_strats_cols

        # Create dataframe
        self.backtest_results = pd.DataFrame(columns=all_cols)

        return self

    
    def run(self):
        pd.set_option('display.width', 1000)
        
        symbols = self.data.index.get_level_values(self.symbol_col).unique()
        symbols_len = len(symbols)
        df_shape = self.pivoted_data[self.price_col].shape
        df_index = self.pivoted_data.index
        df_len = len(self.pivoted_data)
        idx_matrices = self._get_index_matrices()
        
        print(f'There are total {len(idx_matrices)} options')

        for option_num, idx_matrix in enumerate(idx_matrices):
            trading_logs = pd.DataFrame(data=np.zeros(df_shape), columns=symbols, index=df_index, dtype=np.int8)
            holding_days = pd.Series(data=np.zeros(symbols_len), index=symbols, dtype=np.int8)
            trailing_pnl = pd.Series(data=np.ones(symbols_len), index=symbols, dtype=np.int8)
            
            # Trading slots
            # portfolio_size = self.portfolio_size[idx_matrix[-2]]
            # low_risk_prop = self.low_risk_prop[idx_matrix[-1]]
            # low_risk_slots = round(portfolio_size * low_risk_prop)
            # high_risk_slots = portfolio_size - low_risk_slots

            for date_idx in range(self.min_holding_days, df_len):
                # Initiate empty signals
                buy_signals = pd.Series(data=np.ones(symbols_len), index=symbols, dtype=np.int8)
                sell_signals = pd.Series(data=np.zeros(symbols_len), index=symbols, dtype=np.int8)

                # Get current positions and daily return
                current_positions = trading_logs.iloc[date_idx-1]
                daily_ret = (self.pivoted_data.iloc[date_idx-1][self.daily_ret_col] * (current_positions > 0)) + 1
                
                # Get holding days
                holding_days += (current_positions > 0) * 1
                holding_days *= (current_positions > 0)
                
                # Get trailing P&L
                trailing_pnl = (trailing_pnl * (current_positions > 0)).replace(0, 1)
                trailing_pnl *= daily_ret

                # Break line seperating buy and sell strategies
                break_line = len(self.buy_strats)
                
                # Buy signals
                for strat, option_idx in zip(self.buy_strats, idx_matrix[:break_line]):
                    signal = self._generate_signals(strat, option_idx, date_idx)
                    buy_signals *= signal
                
                # Sell signals
                for strat, option_idx in zip(self.sell_strats, idx_matrix[break_line:-2]):
                    if strat['type'] == 'holding_days':
                        signal = self._generate_signals(strat, option_idx, None, holding_days)
                    elif strat['type'] == 'trailing_stoploss':
                        signal = self._generate_signals(strat, option_idx, None, trailing_pnl)
                    else:
                        signal = self._generate_signals(strat, option_idx, date_idx)
                    
                    sell_signals = ((sell_signals + signal) > 0) * 1
                    sell_signals *= (holding_days >= self.min_holding_days)
                
                # Finalize new trades
                new_trades = self._get_new_trades(current_positions, buy_signals, sell_signals, idx_matrix, date_idx)
                
                trading_logs.iloc[date_idx] = (current_positions * (sell_signals == 0)) + new_trades

            # Consolidate with all backtest results
            self._consolidate_returns(trading_logs, idx_matrix)
            
            # Display new result
            trade_count = self.backtest_results.iloc[-1]['trade_count']
            cumm_return = round(self.backtest_results.iloc[-1]['cumm_return'], 2)
            sharpe_ratio = round(self.backtest_results.iloc[-1]['sharpe_ratio'], 2)
            print(f'Option {option_num + 1} - ', end='')
            print(f'Trade count: {trade_count} - Cumm Ret: {cumm_return} - Sharpe: {sharpe_ratio}')
        
        return self.backtest_results
    

    def _consolidate_returns(self, trading_logs, idx_matrix):
        # Get strategies details
        break_line = len(self.buy_strats)
        buy_strats = [strat['threshold'][i] for strat, i in zip(self.buy_strats, idx_matrix[:break_line])]
        sell_strats = [strat['threshold'][i] for strat, i in zip(self.sell_strats, idx_matrix[break_line:-2])]
        portfolio_size = self.portfolio_size[idx_matrix[-2]]
        low_risk_prop = self.low_risk_prop[idx_matrix[-1]]
        full_strats = [portfolio_size] + [low_risk_prop] + buy_strats + sell_strats

        # Calculate trading results
        trade_count = (trading_logs > 0).sum().sum()
        sum_return = (self.pivoted_data[self.daily_ret_col] * (trading_logs > 0) * (1/portfolio_size)).sum(axis=1)
        cumm_return = np.prod(sum_return + 1)
        mean_return = np.mean(list(filter(lambda val: (val != 0), np.ravel(sum_return))))
        std_return = np.std(list(filter(lambda val: (val != 0), np.ravel(sum_return))))
        sharpe_ratio = mean_return/std_return

        # Consolidate results
        consolidated_results = [cumm_return] + [sharpe_ratio] + [trade_count] + full_strats
        consolidated_results = pd.DataFrame([consolidated_results,], columns=self.backtest_results.columns)
        
        self.backtest_results = pd.concat([self.backtest_results, consolidated_results])

        return self
    

    def _get_new_trades(self, current_positions, buy_signals, sell_signals, idx_matrix, date_idx):
        # Get predictions and volume data
        pred_vals = self.pivoted_data.iloc[date_idx-2][self.pred_col].fillna(0)
        pred_proba = self.pivoted_data.iloc[date_idx-2][self.pred_proba_col]
        vol = self.pivoted_data.iloc[date_idx-2][self.vol_col]

        # Update positions with sell signals
        updated_positions = current_positions * (sell_signals == 0)
        
        # Get portfolio size and low risk proportion
        portfolio_size = self.portfolio_size[idx_matrix[-2]]
        low_risk_prop = self.low_risk_prop[idx_matrix[-1]]

        if low_risk_prop is None:
            # Get available slots
            avail_slots = portfolio_size - sum((updated_positions > 0) * 1)

            # Rank stock's predictions probability and volume
            stock_pred_proba = buy_signals * pred_proba
            stock_vol = vol * (stock_pred_proba.rank(ascending=False) <= avail_slots)
            stock_ranking = stock_vol.rank(ascending=False)

            # New trading signals
            new_trades = (stock_ranking <= avail_slots) * pred_vals

        else:
            # Get available low risk slots
            low_risk_slots = round(portfolio_size * low_risk_prop)
            high_risk_slots = portfolio_size - low_risk_slots
            low_risk_avail = low_risk_slots - sum((updated_positions == 1) * 1)
            high_risk_avail = high_risk_slots - sum((updated_positions == 2) * 1)
            
            # Selected stock's risk probability
            low_risk_pred_proba = buy_signals * (pred_vals == 1) * pred_proba
            high_risk_pred_proba = buy_signals * (pred_vals == 2) * pred_proba

            # Rank stock's predictions probability and volume
            low_risk_vol = vol * (low_risk_pred_proba.rank(ascending=False) <= low_risk_avail)
            high_risk_vol = vol * (high_risk_pred_proba.rank(ascending=False) <= high_risk_avail)
            low_risk_ranking = low_risk_vol.rank(ascending=False)
            high_risk_ranking = high_risk_vol.rank(ascending=False)

            # New trading signals
            low_risk_new_trades = (low_risk_ranking <= low_risk_avail) * pred_vals
            high_risk_new_trades = (high_risk_ranking <= high_risk_avail) * pred_vals
            new_trades = low_risk_new_trades + high_risk_new_trades
        
        return new_trades
    

    def _get_index_matrices(self):
        strats = self.buy_strats + self.sell_strats
        strats = list(itertools.product(*[range(len(strat['threshold'])) for strat in strats]))
        strats = [list(strat) + [i] for i, _ in enumerate(self.portfolio_size) for strat in strats]
        strats = [list(strat) + [i] for i, _ in enumerate(self.low_risk_prop) for strat in strats]
        
        return strats
    

    def _generate_signals(self, strat, threshold_idx, date_idx=None, value_array=None):
        if strat['type'] == 'simple_compare':
            val = self.pivoted_data.iloc[date_idx-2][strat['col']]
            threshold = strat['threshold'][threshold_idx]
            compare_result = self._compare(val, threshold, strat['operation'])
        
        elif strat['type'] == 'double_compare':
            val1 = self.pivoted_data.iloc[date_idx-2][strat['col'][0]]
            val2 = self.pivoted_data.iloc[date_idx-2][strat['col'][1]]
            threshold1 = strat['threshold'][threshold_idx][0]
            threshold2 = strat['threshold'][threshold_idx][1]
            compare_result1 = self._compare(val1, threshold1, strat['operation'][0])
            compare_result2 = self._compare(val2, threshold2, strat['operation'][1])
            compare_result = compare_result1 | compare_result2
        
        elif strat['type'] == 'columns_compare':
            val = self.pivoted_data.iloc[date_idx-2][strat['col']]
            ref_col = strat['threshold'][threshold_idx]
            threshold = self.pivoted_data.iloc[date_idx-2][ref_col]
            compare_result = self._compare(val, threshold, strat['operation'])
        
        elif strat['type'] == 'holding_days':
            threshold = strat['threshold'][threshold_idx]
            compare_result = self._compare(value_array, threshold, strat['operation'])
        
        elif strat['type'] == 'trailing_stoploss':
            threshold = 1/(1 - strat['threshold'][threshold_idx])
            compare_result = self._compare(1/value_array, threshold, strat['operation'])
                
        return compare_result * 1
    

    def _compare(self, val, threshold, operation):
        if operation == '>':
            return val > threshold
        elif operation == '>=':
            return val >= threshold
        elif operation == '<':
            return val < threshold
        elif operation == '<=':
            return val <= threshold
        elif operation == '==':
            return val == threshold
        elif operation == 'in':
            return val.isin(threshold)
        elif operation == 'not in':
            return ~val.isin(threshold)
