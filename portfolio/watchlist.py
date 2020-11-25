import pandas as pd
import numpy as np


class Watchlist():

    def __init__(self, current_watchlist=None, new_watchlist=None):
        self.symbol_field = 'Symbol'
        self.start_date = 'Start Date'
        self.review_date = 'Review Date'
        self.nomination_field = 'Nomination'
        self.down_proba = 'Down Proba'
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
        # self.action_obs = 'Observe'
        # self.action_drop = 'Drop'
        self.new_watchlist = new_watchlist
        if not current_watchlist:
            self.current_watchlist = pd.DataFrame(columns=[
                self.symbol_field,
                self.nomination_field,
                self.start_date,
                self.down_proba,
                self.up_proba,
                self.risk_profile,
                self.review_date,
                self.down_proba + self.rev_suffix,
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
        ubound=0.55,
        lbound=0.5,
        elem_bound=0.3,
        risk_appetite=[2,],
        min_price=10,
        min_vol=30000,
        portfolio_size=10,
        low_risk_prop=0.5
    ):
        # Create baseline for watchlist
        base = origin_df.groupby([symbol_field, date_field, price_field, vol_field]).groups.keys()
        base = np.array(list(base))
        symbols = base[:, 0]
        dates = pd.to_datetime(base[:, 1]).strftime('%Y-%m-%d')
        price = base[:, 2]
        vol = base[:, 3]
        
        # Sum Down & Up probabilities
        down_proba, up_proba = self._get_agg_proba(pred_result)
        
        # Define riskiness
        risk = self._get_risk_profile(pred_result)
        
        # Define suggestion category: Recommend or Potential
        category = self._get_nomination(price, vol, pred_result, ubound, lbound, elem_bound, risk_appetite, min_price, min_vol)
        
        # Action
        action = self._get_action(category)

        # Create full list
        full_list = pd.DataFrame(symbols, columns=[self.symbol_field])
        full_list[self.nomination_field] = category
        full_list[self.start_date] = dates
        full_list[self.down_proba] = down_proba
        full_list[self.up_proba] = up_proba
        full_list[self.risk_profile] = risk
        full_list[self.new_action] = action
        full_list[self.t0_price] = price
        full_list[self.curr_vol] = vol

        # Filter new watchlist
        self.new_watchlist = self._get_new_watchlist(full_list)
        self.new_watchlist.reset_index(drop=True, inplace=True)
        
        return self.new_watchlist
    

    def integrate_watchlists(self):
        final_wl = self._get_original_obs()
        new_comers = self._get_new_comers()
        final_wl = final_wl.append(new_comers)

        final_wl = self._update_review_obs(final_wl)
        final_wl = self._realign_col_names(final_wl)
        final_wl = self._realign_col_values(final_wl)
        final_wl = self._rearrange_columns(final_wl)
        inactive_wl = self.get_inactive_watchlist()
        final_wl = pd.concat([self.sort_watchlist(final_wl), self.sort_watchlist(inactive_wl)])
        
        self.final_watchlist = final_wl.reset_index(drop=True)

        return self.final_watchlist
    

    def _get_new_watchlist(self, full_list):
        # Define conditions
        is_being_watched = full_list[self.symbol_field].isin(self.get_active_symbols())
        # is_potential = full_list[self.nomination_field].isin([self.nomination_lvl1, self.nomination_lvl2])
        is_potential = full_list[self.nomination_field].isin([self.nomination_lvl1])
        
        return full_list[is_being_watched | is_potential]


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
    

    def _rearrange_columns(self, final_wl):
        columns = [
            self.symbol_field,
            self.nomination_field,
            self.start_date,
            self.down_proba,
            self.up_proba,
            self.risk_profile,
            self.review_date,
            self.down_proba + self.rev_suffix,
            self.up_proba + self.rev_suffix,
            self.risk_profile + self.rev_suffix,
            self.previous_action,
            self.new_action,
            self.t0_price,
            self.curr_price,
            self.curr_vol,
            self.est_return,
        ]

        final_wl = final_wl[columns]

        return final_wl
    

    def _update_review_obs(self, final_wl):
        final_wl = final_wl.merge(
            self.new_watchlist,
            how='left',
            on=self.symbol_field,
            suffixes=('', self.rev_suffix)
        )

        return final_wl
    

    def _get_original_obs(self):
        columns = [
            self.symbol_field,
            self.nomination_field,
            self.start_date,
            self.down_proba,
            self.up_proba,
            self.risk_profile,
            self.new_action,
            self.t0_price
        ]

        original_obs = self.get_active_watchlist()

        return original_obs[columns]
    

    def _get_new_comers(self):
        active_symbols = self.get_active_symbols()
        is_current = self.new_watchlist[self.symbol_field].isin(active_symbols)
        
        return self.new_watchlist[~is_current]
    

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
    

    def _realign_col_values(self, final_wl):
        # Update nomination
        origin_nom = self.nomination_field
        new_nom = self.nomination_field + self.rev_suffix
        final_wl[[origin_nom, new_nom]] = final_wl[[origin_nom, new_nom]].apply(self._update_nomination, axis=1)

        # # Update action
        # previous_act = self.previous_action
        # new_act = self.new_action
        # final_wl[[previous_act, new_act]] = final_wl[[previous_act, new_act]].apply(self._update_action, axis=1)

        # Update estimated return
        final_wl[self.est_return] = final_wl[self.curr_price]/final_wl[self.t0_price] - 1

        return final_wl
    

    def _update_action(self, columns):
        # former_buy = columns[0] == self.action_buy
        # former_obs = columns[0] == self.action_obs
        # later_obs = columns[1] == self.action_obs
        # later_sell = columns[1] == self.action_sell

        # if former_buy and later_obs:
        #     columns[1] = self.action_buy
        # elif former_obs and later_sell:
        #     columns[1] = self.action_drop
        
        return columns
    

    def _update_nomination(self, columns):
        former_buy = columns[0] == self.nomination_lvl1
        later_sell = columns[1] == self.nomination_lvl3
        if former_buy and later_sell:
            columns[0] = self.nomination_lvl3

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
        down_prob = pred_result[:, 0] + pred_result[:, 1]
        up_prob = pred_result[:, 2] + pred_result[:, 3]

        return down_prob, up_prob
    

    def _get_risk_profile(self, pred_result):
        risk = pred_result[:, 2] > pred_result[:, 3]
        risk = map(lambda x: self.low_risk if x else self.high_risk, risk)

        return list(risk)
    

    def _get_nomination(self, price, vol, pred_result, ubound, lbound, elem_bound, risk_appetite, min_price, min_vol):
        # Get prediction values
        pred_val = np.multiply([-2, -1, 1, 2], np.equal(pred_result, pred_result.max(axis=1).reshape(-1, 1))).sum(axis=1)
        # Define conditions
        buy_cond_1 = np.isin(pred_val, risk_appetite)
        buy_cond_2 = (pred_result[:, 2] + pred_result[:, 3]) >= ubound
        elem_cond = (pred_result[:, 2] >= elem_bound) | (pred_result[:, 3] >= elem_bound)
        price_cond = price >= min_price
        vol_cond = vol >= min_vol
        hold_cond = (pred_result[:, 2] + pred_result[:, 3]) >= lbound
        sell_cond_1 = pred_val < 0
        sell_cond_2 = ~hold_cond
        
        # Categorize nominations
        buy = (buy_cond_1 & buy_cond_2 & elem_cond & price_cond & vol_cond) * 1
        hold = hold_cond * 0
        sell = (sell_cond_1 & sell_cond_2) * (-1)
        category = buy + hold + sell
        category = map(
            lambda x: self.nomination_lvl1 if x == 1 else self.nomination_lvl2 if x == 0 else self.nomination_lvl3,
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
