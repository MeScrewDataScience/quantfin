import pandas as pd
import numpy as np


class Watchlist():

    def __init__(self, current_watchlist=None, holidays=[]):
        self.symbol_field = 'Symbol'
        self.start_date = 'Start Date'
        self.review_date = 'Review Date'
        self.nomination_field = 'Nomination'
        self.up_proba_low_risk = 'Up Proba - LR'
        self.up_proba_high_risk = 'Up Proba - HR'
        self.up_proba = 'Up Proba'
        self.risk_profile = 'Volat (Risk)'
        self.previous_action = 'Previous Action'
        self.new_action = 'New Action'
        self.t0_price = 'T0 Price'
        self.curr_price = 'Curr. Price'
        self.curr_vol = 'Curr. Vol'
        self.est_return = 'Est. Return'
        self.rev_suffix = ' Rev.'
        self.nomination_lvl1 = 'Recommend'
        self.nomination_lvl2 = 'Hold'
        self.nomination_lvl3 = 'Sell'
        self.low_risk = 'Low'
        self.high_risk = 'High'
        self.action_buy = 'Buy & Hold'
        self.action_sell = 'Sell & Close'
        self.holidays = holidays
        # self.action_obs = 'Observe'
        # self.action_drop = 'Drop'
        if not current_watchlist:
            self.current_watchlist = pd.DataFrame(columns=[
                self.symbol_field,
                self.nomination_field,
                self.start_date,
                self.up_proba_low_risk,
                self.up_proba_high_risk,
                self.up_proba,
                self.risk_profile,
                self.review_date,
                self.up_proba_low_risk + self.rev_suffix,
                self.up_proba_high_risk + self.rev_suffix,
                self.up_proba + self.rev_suffix,
                self.risk_profile + self.rev_suffix,
                self.previous_action,
                self.new_action,
                self.t0_price,
                self.curr_price,
                self.curr_vol,
                self.est_return,
            ])


    def load_csv(self, directory, parse_dates, infer_datetime_format=True, index_col=0):
        self.current_watchlist = pd.read_csv(
            directory,
            parse_dates=parse_dates,
            infer_datetime_format=infer_datetime_format,
            index_col=index_col
        )

        return
    

    def build_new_watchlist(
        self,
        pred_result,
        origin_df,
        symbol_field='symbol',
        date_field='date',
        price_field='adj_close',
        vol_field='vol',
        proba_ubound=0.0,
        proba_lbound=0.0,
        low_risk_proba_lbound=0.5,
        high_risk_proba_lbound=0.3,
        min_price=10,
        min_vol=30000,
        risk_appetite=[1, 2],
        stoploss=0.1,
        max_holding_days=10,
        portfolio_size=10,
        low_risk_prop=0.7,
    ):
        # Create baseline for watchlist
        base = origin_df.groupby([symbol_field, date_field, price_field, vol_field]).groups.keys()
        base = np.array(list(base))
        symbols = base[:, 0]
        dates = pd.to_datetime(base[:, 1])
        price = base[:, 2]
        vol = base[:, 3]
        
        # Sum Down & Up probabilities
        up_proba_lr, up_proba_hr, up_proba = self._get_agg_proba(pred_result)
        
        # Define riskiness
        risk = self._get_risk_profile(pred_result)
        
        # Define nominations
        nominations = self._get_nomination(
            price,
            vol,
            pred_result,
            proba_ubound,
            proba_lbound,
            low_risk_proba_lbound,
            high_risk_proba_lbound,
            min_price,
            min_vol,
            risk_appetite,
        )
        
        # Action
        action = self._get_action(nominations)

        # Create full list
        full_obs = pd.DataFrame(symbols, columns=[self.symbol_field])
        full_obs[self.nomination_field] = nominations
        full_obs[self.start_date] = dates
        full_obs[self.up_proba_low_risk] = up_proba_lr
        full_obs[self.up_proba_high_risk] = up_proba_hr
        full_obs[self.up_proba] = up_proba
        full_obs[self.risk_profile] = risk
        full_obs[self.new_action] = action
        full_obs[self.t0_price] = price
        full_obs[self.curr_vol] = vol

        # Filter new watchlist
        new_obs = self._get_new_obs(full_obs)
        new_obs.reset_index(drop=True, inplace=True)
        
        return self._update_watchlist(new_obs, portfolio_size, low_risk_prop, stoploss, max_holding_days)
    

    def _update_watchlist(self, new_obs, portfolio_size, low_risk_prop, stoploss, max_holding_days):
        original_obs = self._get_original_obs()
        new_comers = self._get_new_comers(new_obs)
        original_obs = original_obs.append(new_comers)

        full_obs = self._join_obs(original_obs, new_obs)
        full_obs = self._realign_col_names(full_obs)
        full_obs = self._realign_col_values(full_obs, stoploss, max_holding_days)
        full_obs = self._rearrange_columns(full_obs)
        selected_obs = self._filter_obs(full_obs, portfolio_size, low_risk_prop)
        
        inactive_wl = self.get_inactive_watchlist()
        final_wl = pd.concat([self.sort_watchlist(selected_obs), self.sort_watchlist(inactive_wl)])
        
        self.final_watchlist = final_wl.reset_index(drop=True)

        return self.final_watchlist
    

    def _filter_obs(self, obs, portfolio_size, low_risk_prop):
        # Proba columns
        risk_profile_column = self.risk_profile + self.rev_suffix
        proba_lr_column = self.up_proba_low_risk + self.rev_suffix
        proba_hr_column = self.up_proba_high_risk + self.rev_suffix
        
        # Initiate conditions
        is_new = obs[self.start_date] == obs[self.review_date]
        is_nominated = obs[self.nomination_field] == self.nomination_lvl1
        is_low_risk = obs[risk_profile_column] == self.low_risk
        is_high_risk = obs[risk_profile_column] == self.high_risk
        
        # Get current positions
        holding_days = obs[[self.start_date, self.review_date]].apply(self._biz_days_count, args=('C', self.holidays), axis=1)
        low_risk_slots = round(portfolio_size * low_risk_prop)
        high_risk_slots = portfolio_size - low_risk_slots
        curr_low_risk_positions = len(obs[(~is_new) & is_nominated & is_low_risk])
        curr_high_risk_positions = len(obs[(~is_new) & is_nominated & is_high_risk])

        # Calculate available slots
        if curr_low_risk_positions > low_risk_slots:
            excess_slots = curr_low_risk_positions - low_risk_slots
            curr_low_risk = (obs[proba_lr_column] * (~is_new & is_nominated & is_low_risk & (holding_days >= 3)))
            curr_low_risk_ranking = curr_low_risk.replace(0, np.nan).rank()
            obs.loc[curr_low_risk_ranking <= excess_slots, [self.nomination_field, self.new_action]] = [self.nomination_lvl3, self.action_sell]
            avail_low_risk_postions = 0
        else: 
            avail_low_risk_postions = low_risk_slots - curr_low_risk_positions
        
        if curr_high_risk_positions > high_risk_slots:
            excess_slots = curr_high_risk_positions - high_risk_slots
            curr_high_risk = (obs[proba_hr_column] * (~is_new & is_nominated & is_high_risk & (holding_days >= 3)))
            curr_high_risk_ranking = curr_high_risk.replace(0, np.nan).rank()
            obs.loc[curr_high_risk_ranking <= excess_slots, [self.nomination_field, self.new_action]] = [self.nomination_lvl3, self.action_sell]
            avail_high_risk_postions = 0
        else:
            avail_high_risk_postions = high_risk_slots - curr_high_risk_positions
        
        # Rank prediction proba
        low_risk_ranking = (obs[proba_lr_column] * (is_new & is_nominated & is_low_risk)).rank(ascending=False)
        high_risk_ranking = (obs[proba_hr_column] * (is_new & is_nominated & is_high_risk)).rank(ascending=False)
        
        # Select stocks
        selected_low_risk = obs[low_risk_ranking <= avail_low_risk_postions]
        selected_high_risk = obs[high_risk_ranking <= avail_high_risk_postions]
        
        # Return selected observations
        return pd.concat([selected_low_risk, selected_high_risk, obs[~is_new]])
    

    def _get_new_obs(self, full_obs):
        # Define conditions
        is_being_watched = full_obs[self.symbol_field].isin(self.get_active_symbols())
        # is_potential = full_obs[self.nomination_field].isin([self.nomination_lvl1, self.nomination_lvl2])
        is_nominated = full_obs[self.nomination_field].isin([self.nomination_lvl1])
        
        return full_obs[is_being_watched | is_nominated]


    def get_active_symbols(self):
        active_wl = self.get_active_watchlist()
        active_symbols = active_wl[self.symbol_field].tolist()

        return active_symbols
    

    def get_active_watchlist(self):
        is_bought = self.current_watchlist[self.new_action] == self.action_buy
        # is_observed = self.current_watchlist[self.new_action] == self.action_obs
        # active_wl = self.current_watchlist[is_bought | is_observed].copy()
        active_wl = self.current_watchlist[is_bought].copy()

        return active_wl
    

    def get_inactive_watchlist(self):
        is_closed = self.current_watchlist[self.new_action] == self.action_sell
        # is_dropped = self.current_watchlist[self.new_action] == self.action_drop
        # inactive_wl = self.current_watchlist[is_closed | is_dropped].copy()
        inactive_wl = self.current_watchlist[is_closed].copy()

        return inactive_wl
    

    def sort_watchlist(self, watchlist=None):
        if watchlist is None:
            watchlist = self.final_watchlist
        
        # Customize order of nominations
        # watchlist[self.nomination_field] = pd.Categorical(
        #     watchlist[self.nomination_field],
        #     [self.nomination_lvl1, self.nomination_lvl2, self.nomination_lvl3]
        # )
        watchlist[self.nomination_field] = pd.Categorical(
            watchlist[self.nomination_field],
            [self.nomination_lvl1, self.nomination_lvl3]
        )

        # Customize order of new actions
        # watchlist[self.new_action] = pd.Categorical(
        #     watchlist[self.new_action],
        #     [self.action_buy, self.action_sell, self.action_obs, self.action_drop]
        # )
        watchlist[self.new_action] = pd.Categorical(
            watchlist[self.new_action],
            [self.action_buy, self.action_sell]
        )

        # Sort watchlist
        sort_columns = [self.new_action, self.nomination_field, self.start_date, self.up_proba + self.rev_suffix, self.curr_vol]
        asc = [True, True, False, False, False]
        watchlist.sort_values(by=sort_columns, ascending=asc, inplace=True)

        return watchlist
    

    def _rearrange_columns(self, observations):
        columns = [
            self.symbol_field,
            self.nomination_field,
            self.start_date,
            self.up_proba_low_risk,
            self.up_proba_high_risk,
            self.up_proba,
            self.risk_profile,
            self.review_date,
            self.up_proba_low_risk + self.rev_suffix,
            self.up_proba_high_risk + self.rev_suffix,
            self.up_proba + self.rev_suffix,
            self.risk_profile + self.rev_suffix,
            self.previous_action,
            self.new_action,
            self.t0_price,
            self.curr_price,
            self.curr_vol,
            self.est_return,
        ]

        observations = observations[columns]

        return observations
    

    def _join_obs(self, original_obs, new_obs):
        full_obs = original_obs.merge(
            new_obs,
            how='left',
            on=self.symbol_field,
            suffixes=('', self.rev_suffix)
        )

        return full_obs
    

    def _get_original_obs(self):
        columns = [
            self.symbol_field,
            self.nomination_field,
            self.start_date,
            self.up_proba_low_risk,
            self.up_proba_high_risk,
            self.up_proba,
            self.risk_profile,
            self.new_action,
            self.t0_price
        ]

        original_obs = self.get_active_watchlist()

        return original_obs[columns]
    

    def _get_new_comers(self, new_obs):
        active_symbols = self.get_active_symbols()
        is_current = new_obs[self.symbol_field].isin(active_symbols)
        
        return new_obs[~is_current]
    

    def _realign_col_names(self, final_wl):
        final_wl.drop(columns=self.curr_vol, inplace=True)
        final_wl.rename(
            columns={
                self.start_date + self.rev_suffix: self.review_date,
                self.new_action: self.previous_action,
                self.new_action + self.rev_suffix: self.new_action,
                self.t0_price + self.rev_suffix: self.curr_price,
                self.curr_vol + self.rev_suffix: self.curr_vol
            },
            inplace=True
        )

        return final_wl
    

    def _realign_col_values(self, final_wl, stoploss, max_holding_days):
        # Update estimated return
        final_wl[self.est_return] = final_wl[self.curr_price]/final_wl[self.t0_price] - 1

        # Update nomination
        columns = [
            self.nomination_field,
            self.nomination_field + self.rev_suffix,
            self.start_date,
            self.review_date,
            self.est_return,
        ]
        final_wl[columns] = final_wl[columns].apply(
            self._update_nomination, args=(stoploss, max_holding_days), axis=1
        )

        # Update action
        columns = [
            self.nomination_field + self.rev_suffix,
            self.new_action,
        ]
        final_wl[columns] = final_wl[columns].apply(self._update_action, axis=1)

        return final_wl
    

    def _update_action(self, columns):
        if columns[0] == self.nomination_lvl3:
            columns[1] = self.action_sell
        
        return columns
    

    def _update_nomination(self, columns, stoploss, max_holding_days):
        former_buy = columns[0] == self.nomination_lvl1
        later_sell = columns[1] == self.nomination_lvl3
        holding_days = self._biz_days_count(columns[2:4], freq='C', holidays=self.holidays)
        if former_buy and later_sell and holding_days >= 3:
            columns[0] = self.nomination_lvl3
        
        if holding_days >= max_holding_days:
            columns[0] = self.nomination_lvl3
            columns[1] = self.nomination_lvl3
        
        if columns[4] <= -stoploss:
            columns[0] = self.nomination_lvl3
            columns[1] = self.nomination_lvl3

        return columns
    

    # def _higher_nomination_lvl(self, columns):
    #     former_lvl2 = columns[0] == self.nomination_lvl2
    #     # former_lvl3 = columns[0] == self.nomination_lvl3
    #     later_lvl1 = columns[1] == self.nomination_lvl1
    #     # later_lvl2 = columns[1] == self.nomination_lvl2

    #     # is_higer = (former_lvl2 | former_lvl3) & later_lvl1
    #     # is_higer = is_higer | (former_lvl3 & later_lvl2)
        
    #     return former_lvl2 & later_lvl1
    

    # def _lower_nomination_lvl(self, columns):
    #     former_lvl1 = columns[0] == self.nomination_lvl1
    #     later_lvl2 = columns[1] == self.nomination_lvl2
    #     # former_lvl2 = columns[0] == self.nomination_lvl2
    #     later_lvl3 = columns[1] == self.nomination_lvl3

    #     return former_lvl1 & (later_lvl2 | later_lvl3)
    

    def _get_agg_proba(self, pred_result):
        up_proba_lr = pred_result[:, 2]
        up_proba_hr = pred_result[:, 3]
        up_proba = pred_result[:, 2] + pred_result[:, 3]

        return up_proba_lr, up_proba_hr, up_proba
    

    def _get_risk_profile(self, pred_result):
        risk = pred_result[:, 2] > pred_result[:, 3]
        risk = map(lambda x: self.low_risk if x else self.high_risk, risk)

        return list(risk)
    

    def _get_nomination(
        self,
        price,
        vol,
        pred_result,
        proba_ubound,
        proba_lbound,
        low_risk_proba_lbound,
        high_risk_proba_lbound,
        min_price,
        min_vol,
        risk_appetite,
    ):
        # Get prediction values
        pred_val = np.multiply([-2, -1, 1, 2], np.equal(pred_result, pred_result.max(axis=1).reshape(-1, 1))).sum(axis=1)
        
        # Define buy conditions
        pred_val_cond = np.isin(pred_val, risk_appetite)
        pred_proba_cond = (pred_result[:, 2] + pred_result[:, 3]) >= proba_ubound
        elem_cond = (pred_result[:, 2] >= low_risk_proba_lbound) | (pred_result[:, 3] >= high_risk_proba_lbound)
        price_cond = price >= min_price
        vol_cond = vol >= min_vol
        hold_cond = (pred_result[:, 2] + pred_result[:, 3]) >= proba_lbound
        
        # Define sell conditions
        sell_pred_val_cond = pred_val < 0
        sell_pred_proba_cond = ~hold_cond
        
        # Categorize nominations
        buy = (pred_val_cond & pred_proba_cond & elem_cond & price_cond & vol_cond) * pred_val
        hold = hold_cond * 0
        sell = (sell_pred_val_cond & sell_pred_proba_cond) * (-1)
        category = buy + hold + sell
        category = map(
            lambda x: self.nomination_lvl1 if x > 0 else self.nomination_lvl2 if x == 0 else self.nomination_lvl3,
            category
        )
        category = list(category)

        return category
    

    def _get_action(self, nomination):
        action = map(
            lambda x: self.action_buy if x in [self.nomination_lvl1, self.nomination_lvl2] else self.action_sell,
            nomination
        )

        return list(action)
    

    def _biz_days_count(self, columns, freq='C', holidays=[]):
        return len(pd.bdate_range(columns[0], columns[1], freq='C', holidays=holidays))
