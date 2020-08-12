import pandas as pd
import numpy as np


class Watchlist():

    def __init__(self, current_watchlist=None, new_watchlist=None):
        self.current_watchlist = current_watchlist
        self.new_watchlist = new_watchlist
        self.symbol_field = 'Symbol'
        self.start_date = 'Start Date'
        self.review_date = 'Review Date'
        self.nomination_field = 'Nomination'
        self.down_proba = 'Down Proba'
        self.up_proba = 'Up Proba'
        self.volat = 'Volat (Risk)'
        self.previous_action = 'Previous Action'
        self.new_action = 'New Action'
        self.t0_price = 'T0 Price'
        self.curr_price = 'Curr. Price'
        self.est_return = 'Est. Return'
        self.rev_suffix = ' Rev.'
        self.nomination_lvl1 = 'Recommended'
        self.nomination_lvl2 = 'Potential'
        self.nomination_lvl3 = 'Normal'
        self.low_risk = 'Low'
        self.high_risk = 'High'
        self.action_buy = 'Buy & Hold'
        self.action_sell = 'Sell & Close'
        self.action_obs = 'Observe'
        self.action_drop = 'Drop'

        return


    def load_watchlist_csv(self, directory, parse_dates, infer_datetime_format=True, index_col=0):
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
        ubound=0.7,
        lbound=0.6,
        elem_bound=0.5
    ):
        # Create baseline for watchlist
        base = origin_df.groupby([symbol_field, date_field, price_field]).groups.keys()
        base = np.array(list(base))
        
        # Sum Down & Up probabilities
        down_proba, up_proba = self._get_agg_proba(pred_result)
        
        # Define riskiness
        risk = self._get_risk_profile(pred_result)
        
        # Define suggestion category: Recommended or Potential
        category = self._get_nomination(pred_result, ubound, lbound, elem_bound)
        
        # Action
        action = self._get_action(category)

        # Create watchlist
        self.full_watchlist = pd.DataFrame(base[:, 0], columns=[self.symbol_field])
        self.full_watchlist[self.nomination_field] = category
        self.full_watchlist[self.start_date] = base[:, 1]
        self.full_watchlist[self.down_proba] = down_proba
        self.full_watchlist[self.up_proba] = up_proba
        self.full_watchlist[self.volat] = risk
        self.full_watchlist[self.new_action] = action
        self.full_watchlist[self.t0_price] = base[:, 2]

        # Filter new watchlist
        self.new_watchlist = self.get_new_watchlist()
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
        
        self.final_watchlist = final_wl

        return self.final_watchlist
    

    def get_new_watchlist(self):
        # Define conditions
        is_being_watched = self.full_watchlist[self.symbol_field].isin(self.get_active_symbols())
        is_potential = self.full_watchlist[self.nomination_field].isin([self.nomination_lvl1, self.nomination_lvl2])
        
        return self.full_watchlist[is_being_watched | is_potential]


    def get_active_symbols(self):
        active_wl = self.get_active_watchlist()
        active_symbols = active_wl[self.symbol_field].tolist()

        return active_symbols
    

    def get_active_watchlist(self):
        is_closed = self.current_watchlist[self.new_action] == self.action_sell
        is_dropped = self.current_watchlist[self.new_action] == self.action_drop
        active_wl = self.current_watchlist[~(is_closed | is_dropped)].copy()

        return active_wl
    

    def get_inactive_watchlist(self):
        is_closed = self.current_watchlist[self.new_action] == self.action_sell
        is_dropped = self.current_watchlist[self.new_action] == self.action_drop
        inactive_wl = self.current_watchlist[is_closed | is_dropped].copy()

        return inactive_wl
    

    def sort_watchlist(self, watchlist=None):
        if watchlist is None:
            watchlist = self.final_watchlist
        
        # Customize order of nominations
        watchlist[self.nomination_field] = pd.Categorial(
            watchlist[self.nomination_field],
            [self.nomination_lvl1, self.nomination_lvl2, self.nomination_lvl3]
        )

        # Customize order of new actions
        watchlist[self.new_action] = pd.Categorial(
            watchlist[self.new_action],
            [self.action_buy, self.action_sell, self.action_obs, self.action_drop]
        )

        # Sort watchlist
        sort_columns = [self.nomination_field, self.start_date, self.new_action, self.up_proba]
        asc = [True, False, True, False]
        watchlist.sort_values(by=sort_columns, ascending=asc, inplace=True)

        return watchlist
    

    def _rearrange_columns(self, final_wl):
        columns = [
            self.symbol_field,
            self.nomination_field,
            self.start_date,
            self.down_proba,
            self.up_proba,
            self.volat,
            self.review_date,
            self.down_proba + self.rev_suffix,
            self.up_proba + self.rev_suffix,
            self.volat + self.rev_suffix,
            self.previous_action,
            self.new_action,
            self.t0_price,
            self.curr_price,
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
            self.volat,
            self.new_action,
            self.t0_price,
        ]

        original_obs = self.get_active_watchlist()

        return original_obs[columns]
    

    def _get_new_comers(self):
        active_symbols = self.get_active_symbols()
        is_current = self.new_watchlist[self.symbol_field].isin(active_symbols)
        
        return self.new_watchlist[~is_current]
    

    def _realign_col_names(self, final_wl):
        final_wl.rename(
            columns={
                self.start_date + self.rev_suffix: self.review_date,
                self.new_action: self.previous_action,
                self.new_action + self.rev_suffix: self.new_action,
                self.t0_price + self.rev_suffix: self.curr_price,
            },
            inplace=True
        )

        return final_wl
    

    def _realign_col_values(self, final_wl):
        # Update nomination
        origin_nom = self.nomination_field
        new_nom = self.nomination_field + self.rev_suffix
        final_wl[[origin_nom, new_nom]] = final_wl[[origin_nom, new_nom]].apply(self._update_nomination, axis=1)

        # Update action
        origin_act = self.previous_action
        new_act = self.new_action
        final_wl[[origin_act, new_act]] = final_wl[[origin_act, new_act]].apply(self._recorrect_action, axis=1)

        # Update estimated return
        final_wl[self.est_return] = final_wl[self.curr_price]/final_wl[self.t0_price] - 1

        return final_wl
    

    def _recorrect_action(self, columns):
        former_buy = columns[0] == self.action_buy
        former_obs = columns[0] == self.action_obs
        later_obs = columns[1] == self.action_obs
        later_sell = columns[1] == self.action_sell

        if former_buy and later_obs:
            columns[1] = self.action_buy
        elif former_obs and later_sell:
            columns[1] = self.action_drop
        
        return columns
    

    def _update_nomination(self, columns):
        if self._higher_nomination_lvl(columns) | self._lower_nomination_lvl(columns):
            columns[0] = columns[1]

        return columns
    

    def _higher_nomination_lvl(self, columns):
        former_lvl2 = columns[0] == self.nomination_lvl2
        former_lvl3 = columns[0] == self.nomination_lvl3
        later_lvl1 = columns[1] == self.nomination_lvl1
        later_lvl2 = columns[1] == self.nomination_lvl2

        is_higer = (former_lvl2 | former_lvl3) & later_lvl1
        is_higer = is_higer | (former_lvl3 & later_lvl2)
        
        return is_higer
    

    def _lower_nomination_lvl(self, columns):
        former_lvl2 = columns[0] == self.nomination_lvl2
        later_lvl3 = columns[1] == self.nomination_lvl3

        return former_lvl2 & later_lvl3
    

    def _get_agg_proba(self, pred_result):
        down_prob = pred_result[:, 0] + pred_result[:, 1]
        up_prob = pred_result[:, 2] + pred_result[:, 3]

        return down_prob, up_prob
    

    def _get_risk_profile(self, pred_result):
        risk = pred_result[:, 2] > pred_result[:, 3]
        risk = map(lambda x: self.low_risk if x else self.high_risk, risk)

        return list(risk)
    

    def _get_nomination(self, pred_result, ubound, lbound, elem_bound):
        # Define conditions
        nominated_cond = (pred_result[:, 2] + pred_result[:, 3]) >= ubound
        potential_cond_lb = (pred_result[:, 2] + pred_result[:, 3]) >= lbound
        potential_cond_ub = (pred_result[:, 2] + pred_result[:, 3]) < ubound
        elem_cond = (pred_result[:, 2] >= elem_bound) | (pred_result[:, 3] >= elem_bound)
        
        # Categorize nominations
        recommended = (nominated_cond & elem_cond) * 2
        potential = (potential_cond_lb & potential_cond_ub & elem_cond) * 1
        category = recommended + potential
        category = map(
            lambda x: self.nomination_lvl1 if x == 2 else self.nomination_lvl2 if x == 1 else self.nomination_lvl3,
            category
        )
        category = list(category)

        return category
    

    def _get_action(self, nomination):
        action = map(
            lambda x: self.action_buy if x == self.nomination_lvl1 else self.action_obs if x == self.nomination_lvl2 else self.action_sell,
            nomination
        )

        return list(action)
