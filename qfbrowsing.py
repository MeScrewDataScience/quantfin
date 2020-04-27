# -*- coding: utf-8 -*-

import datetime as dt
import json
import mechanicalsoup
import logging
import logging.config
import pandas as pd
import qfutils
from bs4 import BeautifulSoup
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as chOptions
from selenium.webdriver.firefox.options import Options as ffOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from time import sleep


with open('logging_config.json') as jsonfile:
    logging_config = json.load(jsonfile)

logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)


class SoupReader():

    def __init__(self, soup):
        self.soup = soup

    def read_html_tabular_text(
        self,
        tag_attr={},
        skiprows=[],
        skipcols=[],
        thousands=None,
        decimal=None,
        na_values=None
    ):
        
        if len(tag_attr['row_col_pair']) != 2:
            logger.error('The tabular soup data should be 2 dimensional')
            raise ValueError

        table_soup = self._read_soup(tag_attr)
        rows = table_soup.find_all(tag_attr['row_col_pair'][0])
        text_data = []

        # When the tabular Data are 2-dimensional,
        # loop through all rows and columns
        for rows_idx, row_soup in enumerate(rows):
            if skiprows and rows_idx in skiprows:
                continue
            
            text_values = []
            cols = row_soup.find_all(tag_attr['row_col_pair'][1])

            for col_idx, col_soup in enumerate(cols):
                if skipcols and col_idx in skipcols:
                    continue
                
                # Remove redundant spaces
                text = col_soup.text.strip()
                text = ''.join(text.split())
                # Convert to numeric if possible
                text = qfutils._to_numeric(text, thousands, decimal, na_values)

                text_values.append(text)
        
            text_data.append(text_values)
        
        return text_data
    

    def read_html_tabular_link(self, tag_attr, skiprows=[], skipcols=[]):
        
        if len(tag_attr['row_col_pair']) != 2:
            logger.error('The tabular soup data should be 2 dimensional. '
                        'Check \'row_col_pair\' config!')
            raise ValueError

        table_soup = self._read_soup(tag_attr)
        rows = table_soup.find_all(tag_attr['row_col_pair'][0])

        link_data = []

        # When the tabular Data are 2-dimensional,
        # loop through all rows and columns
        for rows_idx, row_soup in enumerate(rows):
            if skiprows and rows_idx in skiprows:
                continue
            
            link_values = []
            cols = row_soup.find_all(tag_attr['row_col_pair'][1])

            for col_idx, col_soup in enumerate(cols):
                if skipcols and col_idx in skipcols:
                    continue
                
                link = col_soup.find('a')

                if not link:
                    link = None
                else:
                    link = link.get('href')

                link_values.append(link)
        
            link_data.append(link_values)
        
        return link_data
    

    def _read_soup(self, tag_attr):

        if not self.soup and isinstance(self.soup, BeautifulSoup):
            logger.error('The provided object is not '
                        'a bs4.BeautifulSoup object!')
            raise ValueError

        if tag_attr['attr'] and not tag_attr['attr_val']:
            logger.error(f"Tag\'s attribute value is missing! "
                             f"{tag_attr['attr']}, {tag_attr['attr_val']}")
            raise ValueError

        if not tag_attr['attr'] and tag_attr['attr_val']:
            logger.error('Tag\'s attribute name is missing!')
            raise ValueError

        if tag_attr['lookup_type'] == 'find_all':
            soup = self.soup.find_all(
                tag_attr['tag_name'],
                {tag_attr['attr']: tag_attr['attr_val']}
            )
            soup = soup[tag_attr['idx']]

        elif tag_attr['lookup_type'] == 'find':
            soup = self.soup.find(
                tag_attr['tag_name'],
                {tag_attr['attr']: tag_attr['attr_val']}
            )

        else:
            logger.error('The internal get_soup function only support either'
                        ' \'find\' or \'find_all\' method!')
            raise ValueError

        return soup


class SelBrowsing(webdriver.Chrome, webdriver.Firefox, webdriver.PhantomJS):
    
    def __init__(self, driver, driver_exe, headless=True):
        
        self.consolidated_df = pd.DataFrame()
        self.load_status = False
        self.headless = headless

        if driver.lower() == 'chrome':
            if self.headless:
                options = chOptions()
                options.add_argument('--headless')
                options.add_argument('--disable-gpu')
                options.add_argument("--log-level=3")
            
                try:
                    webdriver.Chrome.__init__(self, driver_exe, chrome_options=options)
                except:
                    logger.warning(
                        'Can\'t launch web driver with the given exe file',
                        exc_info=True
                    )
                    try:
                        webdriver.Chrome.__init__(self, chrome_options=options)
                    except Exception as e:
                        raise ValueError(e)
            
            else:
                try:
                    webdriver.Chrome.__init__(self, driver_exe)
                except:
                    logger.warning(
                        'Can\'t launch web driver with the given exe file',
                        exc_info=True
                    )
                    try:
                        webdriver.Chrome.__init__(self)
                    except Exception as e:
                        raise ValueError(e)
        
        elif driver.lower() == 'firefox':
            if self.headless:
                options = ffOptions()
                options.add_argument('--headless')
                options.add_argument('--disable-gpu')
                options.add_argument("--log-level=3")
            
                try:
                    webdriver.Firefox.__init__(self, driver_exe, options=options)
                except:
                    logger.warning(
                        'Can\'t launch web driver with the given exe file',
                        exc_info=True
                    )
                    try:
                        webdriver.Firefox.__init__(self)
                    except Exception as e2:
                        raise ValueError(e2)
            
            else:
                try:
                    webdriver.Firefox.__init__(self, driver_exe)
                except:
                    logger.warning(
                        'Can\'t launch web driver with the given exe file',
                        exc_info=True
                    )
                    try:
                        webdriver.Firefox.__init__(self)
                    except Exception as e:
                        raise ValueError(e)
    

    def input_page_prop(self, page_properties):
        self.prop = page_properties

        url = self.prop['url']
        url_hostname = qfutils._get_url_hostname(url)

        callers = ['vndirect', 'cafef', 'vcsc', 'stockbiz']

        for caller in callers:
            if caller in url_hostname:
                self.caller = caller
                break
        
        if not self.caller:
            logger.error(f'The provied URL {url} in page '
                         f'properties is not supported. '
                         f'See below for full page properties\n'
                         f'{self.prop}')
            raise ValueError
    
    
    def load_page(self):

        self.delete_all_cookies()
        self.implicitly_wait(20)
        self.get(self.prop['url'])
        
        return


    def scrape_data(self, method, symbol_data, records=None):

        if method == 'pandas':
            internal_method = self._pd_get_tabular_data
        elif method == 'bs4':
            internal_method = self._bs4_get_tabular_data
        else:
            logger.error(f'Method {method} is not recognized. '
                            'It must be either "pandas" or "bs4"')
            raise ValueError
        
        self._query_page(symbol_data)

        # Get data
        temp_df, self.load_status = internal_method(
            pd.DataFrame(),
            symbol_data[0],
            records
        )
            
        if temp_df.empty:
            logger.warning(f'{symbol_data[0]} - Empty data')
        
        else:
            temp_df[self.prop['symbol_index']] = symbol_data[0]
            temp_df = qfutils._set_columns(
                temp_df,
                self.prop['dict_cols'],
                self.prop['selected_cols'],
                self.prop['date_index'],
                self.prop['result_date_format']
            )
            self.consolidated_df = pd.concat(
                [self.consolidated_df, temp_df],
                ignore_index=True
            )
        
        return temp_df
    

    def _query_page(self, symbol_data, attempt_lim=5, attempts=0):

        try:
            self._query_symbol(symbol_data[0])
            self._query_dates(symbol_data)

            # Get pagination element before perform click operation
            # to check if website is loaded after click operation
            pagination_xpath = self.prop['pagination_xpath']
            pagination = WebDriverWait(self, 20).until(
                EC.presence_of_element_located((By.XPATH, pagination_xpath))
            )

            submit_button = self.find_element_by_xpath(self.prop['button_xpath'])
            submit_button.click()

            # Check if website is loaded if after click operation
            self._wait_for_staleness_of_elem(pagination)
            logger.info(f'{symbol_data[0]} - Page successfully loaded after form submission')
        
        except:
            if attempts == attempt_lim:
                logger.error(f'{symbol_data[0]} - Unable to query the page after {attempts} tries', exc_info=True)
                return
            
            else:
                attempts += 1
                logger.warning(f'Unable to query the page. Refresh and load again. '
                               f'Attempt {attempts}')
                
                self.refresh()
                
                return self._query_page(symbol_data, attempt_lim, attempts)
    

    def _query_dates(self, symbol_data):

        symbol = symbol_data[0]
        from_date = symbol_data[1]
        to_date = symbol_data[2]

        if self.caller == 'vcsc' and not from_date:
            from_date = dt.date(2000, 1, 1)

        try:
            from_date_filter = self.find_element_by_xpath(self.prop['from_date_xpath'])
            to_date_filter = self.find_element_by_xpath(self.prop['to_date_xpath'])

            from_date_filter.clear()
            if from_date:
                from_date = from_date.strftime(self.prop['query_date_format'])
                for char in from_date:
                    from_date_filter.send_keys(char)
            
            to_date_filter.clear()
            if to_date:
                to_date = to_date.strftime(self.prop['query_date_format'])
                for char in to_date:
                    to_date_filter.send_keys(char)
            
            logger.info(f'{symbol} - Date query form filled')
        
        except:
            logger.error(f'{symbol} - Unable to fill the Date query form', exc_info=True)
            raise
        
        return
    

    def _query_symbol(self, symbol):

        if not isinstance(self.prop['symbol_xpath'], list) \
        and not isinstance(self.prop['symbol_xpath'], tuple):
            symbol_xpaths = [self.prop['symbol_xpath']]
        
        else:
            symbol_xpaths = self.prop['symbol_xpath']

        try:
            for xpath in symbol_xpaths:
                symbol_filter = self.find_element_by_xpath(xpath)
                symbol_filter.click()
                
                if xpath == symbol_xpaths[-1]:
                    symbol_filter.clear()
                    symbol_filter.send_keys(symbol)
                    symbol_filter.send_keys(Keys.RETURN)
            
            logger.info(f'{symbol} - Symbol query form filled')
        
        except:
            logger.error(f'{symbol} - Unable to fill the Symbol query form', exc_info=True)
            raise
        
        return
    

    def _pd_get_tabular_data(self, df, symbol, records=None):
        
        table_attr = self.prop['table_attr']
        header = self.prop['header']
        skiprows = self.prop['skiprows']
        thousands = self.prop['thousands']
        decimal = self.prop['decimal']
        na_values = self.prop['na_values']

        try:
            # Check current pagination
            current_page, is_last_page = self._current_page(symbol)
            logger.info(f'{symbol} - Page {current_page} is loaded')
        
            # Get page source
            page_source = self.page_source

            # Parse table
            for i, attrs in enumerate(table_attr):
                try:
                    table = pd.read_html(
                        io=page_source,
                        attrs=attrs,
                        header=header,
                        skiprows=skiprows,
                        thousands=thousands,
                        decimal=decimal,
                        na_values=na_values
                    )
                    table = table[0]
                    
                    if len(header) > 1:
                        table.columns = table.columns.map('_'.join)
                    
                    logger.info(f'{symbol} - Read table data from web done')
                    table_attr[0], table_attr[i] = table_attr[i], table_attr[0]
                    # table = qfutils.df_numericalize(table)
                    df = pd.concat([df, table], ignore_index=True)
                    break
                
                except:
                    if attrs == table_attr[-1]:
                        logger.warning(f'{symbol} - Cannot get data with table attribute: {attrs}')
                        logger.error(f'{symbol} - None of the table attributes works', exc_info=True)
                        logger.error(f'{table_attr}')
                        raise
                    else:
                        logger.warning(f'{symbol} - Cannot get data with table attribute: {attrs}', exc_info=True)
                        logger.info(f'{symbol} - Trying with another table attribute')
                        pass
            
            df_length = len(df)
            
            # Return result if number of records reach the requirement
            if records and df_length >= records:
                df = df.iloc[:records]
                is_fully_load = True
                logger.info(f'{symbol} - Successfully extract '
                            f'{records} records')

                return df, is_fully_load

            # Go to next page
            if is_last_page:
                is_fully_load = not(df.empty)
                logger.info(f'{symbol} - Successfully reach to last page')

                if records and df_length < records:
                    logger.warning(f'{symbol} - Only {df_length} records '
                                    f'are extracted while the requiremnt '
                                    f'is {records}')
            else:
                self._to_next_page(symbol)
                return self._pd_get_tabular_data(df, symbol, records)
        
        except:
            is_fully_load = False
            
        return df, is_fully_load
            

    def _bs4_get_tabular_data(self, df, symbol, records=None):

        table_attr = self.prop['table_attr']
        header = self.prop['header']
        skiprows = self.prop['skiprows']
        thousands = self.prop['thousands']
        decimal = self.prop['decimal']
        na_values = self.prop['na_values']
        
        try:
            # Check current pagination
            current_page, is_last_page = self._current_page(symbol)
            logger.info(f'{symbol} - Page {current_page} is loaded')

            # Check page source
            page_source = self.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            soup_reader = SoupReader(soup)
            
            # Parse table
            for i, attrs in enumerate(table_attr):
                try:
                    text_data = soup_reader.read_html_tabular_text(
                        tag_attr=attrs,
                        skiprows=skiprows,
                        thousands=thousands,
                        decimal=decimal,
                        na_values=na_values
                    )
                    
                    if header:
                        columns = [text_data[i] for i in header]
                        header_rows = len(columns)
                        header_cols = len(columns[0])
                        columns = [
                            '_'.join([columns[i][j]
                                for i in range(header_rows)])
                                    for j in range(header_cols)
                        ]
                        
                        data = text_data[len(header):]
                        table = pd.DataFrame(data=data, columns=columns)
                    
                    else:
                        table = pd.DataFrame(text_data)
                    
                    logger.info(f'{symbol} - Read table data from web done')
                    table_attr[i], table_attr[0] = table_attr[0], table_attr[i]
                    # table = qfutils.df_numericalize(table)
                    df = pd.concat([df, table], ignore_index=True)
                    break
                
                except:
                    if attrs == table_attr[-1]:
                        logger.warning(f'{symbol} - Cannot get data with table attribute: {attrs}', exc_info=True)
                        logger.error(f'{symbol} - None of the table attributes works')
                        logger.error(f'{table_attr}')
                        raise
                    else:
                        logger.warning(f'{symbol} - Cannot get data with table attribute: {attrs}', exc_info=True)
                        logger.info(f'{symbol} - Trying with another table attribute')
                        pass
            
            df_length = len(df)

            # Return result if number of records reach the requirement
            if records and df_length >= records:
                df = df.iloc[:records]
                is_fully_load = True
                logger.info(f'{symbol} - Successfully extract '
                            f'{records} records')

                return df, is_fully_load
            
            # Go to next page
            if is_last_page:
                is_fully_load = not(df.empty)
                logger.info(f'{symbol} - Successfully reach to last page')

                if records and df_length < records:
                    logger.warning(f'{symbol} - Only {df_length} records '
                                    f' are extracted while the requiremnt '
                                    f' is {records}')
            else:
                self._to_next_page(symbol)
                return self._bs4_get_tabular_data(df, symbol, records)
        
        except:
            is_fully_load = False
            
        return df, is_fully_load
    

    def _to_next_page(self, symbol, attempt_lim=5, attempts=0):
        
        try:
            pagination = self.find_element_by_xpath(self.prop['pagination_xpath'])
            new_page = pagination.find_element_by_link_text(self.prop['next_page'])
            new_page.click()
            logger.info(f'{symbol} - Next page selected')
            
            self._wait_for_staleness_of_elem(new_page)
        
        except:
            if attempts == attempt_lim:
                logger.error(f'{symbol} - Cannot go to next page '
                             f'after {attempts} tries', exc_info=True)
                raise
            
            else:
                attempts += 1
                logger.warning(f'{symbol} - Cannot go to next page. Try again. Attempt {attempts}')
                self.refresh()

                return self._to_next_page(symbol, attempt_lim, attempts)
        
        return
    

    def _current_page(self, symbol):

        pagination = self.find_element_by_xpath(self.prop['pagination_xpath'])

        if pagination.text:
            next_page = self._find_next_page(pagination)

            if self.caller in ['vcsc', 'stockbiz']:
                pages = pagination.find_elements_by_xpath('.//*')
            
            elif self.caller == 'cafef':
                pages = pagination.find_elements_by_xpath('.//*')
                pages_text = [page.text for page in pages]
                dup_records = qfutils.list_duplicates(pages_text)
                to_remove = [pages[rec[1][0]] for rec in dup_records]

                for element in to_remove:
                    pages.remove(element)
                
                pages = pages[1:]
            
            elif self.caller == 'vndirect':
                pages = pagination.find_elements_by_tag_name('a')
            
            pages_text = [page.text for page in pages]

            next_page = self._find_next_page(pagination)

            if not next_page:
                return pages_text[-1], True
            
            else:
                if len(pages_text) <= 2:
                    return pages_text[0], False
                
                else:
                    if self.caller == 'stockbiz':
                        page_num = self._page_num_by_style(
                            pages, pages_text
                        )
                    else:
                        page_num = self._page_num_by_href(
                            pagination, next_page
                        )
                    
                    return page_num, False

        else:
            logger.error(f'{symbol} - Table is not loaded')
            raise ValueError

    
    def _find_next_page(self, pagination):
        
        try:
            next_page = pagination.find_element_by_link_text(
                self.prop['next_page']
            )
        except:
            next_page = None
        
        return next_page
    

    def _page_num_by_href(self, pagination, next_page):
        
        next_page_href = next_page.get_attribute('href')

        if self.caller == 'vcsc':
            url_hostname = qfutils._get_url_hostname(next_page_href)
            next_page_href = next_page_href.replace(url_hostname, '')
            next_page_href = '/' + next_page_href
        
        page_n1 = pagination.find_element_by_xpath(
            f'.//a[@href="{next_page_href}"]'
        )
        cur_page = str(int(page_n1.text) - 1)

        return cur_page
    

    def _page_num_by_style(self, pages, pages_text):

        color_prop = self.execute_script(
            'return window.getComputedStyle(arguments[0], null);', pages[0]
        )
        color_prop = [prop for prop in color_prop if prop.find('color') >= 0]
        page_prop = {}

        for idx, page_obj in enumerate(pages):
            page_prop[idx] = {'page_obj': page_obj,}
            for prop in color_prop:
                page_prop[idx][prop] = page_obj.value_of_css_property(prop)
        
        for idx in page_prop:
            for item in page_prop[idx]:
                if item == 'page_obj': continue
                
                next_idx = idx + 1
                
                if page_prop[idx][item] != page_prop[next_idx][item]:
                    if page_prop[next_idx][item] != page_prop[next_idx+1][item]:
                        return pages_text[next_idx]
                    else:
                        return pages_text[idx]
    

    def _wait_for_elem_presence(self, xpath, delay=20):
        element = WebDriverWait(self, delay).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return element
    

    def _wait_for_staleness_of_elem(self, element, delay=20):
        WebDriverWait(self, delay).until(EC.staleness_of(element))
        return


class Bs4Browsing(mechanicalsoup.StatefulBrowser):

    def __init__(self):
        mechanicalsoup.StatefulBrowser.__init__(
            self,
            soup_config={'features': 'lxml'},
            raise_on_404=True
        )
        self.consolidated_df = pd.DataFrame()
        self.load_status = False
    

    def input_page_prop(self, page_properties):

        self.prop = page_properties

        url = self.prop['url']
        self.url_hostname = qfutils._get_url_hostname(url)

        callers = ['cophieu68']

        for caller in callers:
            if caller in self.url_hostname:
                self.caller = caller
                break
        
        if not self.caller:
            logger.error(f'The provied URL {url} in page '
                         f'properties is not supported. '
                         f'See below for full page properties\n'
                         f'{self.prop}')
            raise ValueError
    
    
    def load_page(self):

        if not self.prop['login_url'] and self.prop['login_form']:
            logger.error('Form URL is not provided! Note: Form URL could be '
                        'the same as main URL if the form(s) is/are located '
                        'in the same main URL')
            raise ValueError

        if self.prop['login_url'] and not self.prop['login_form']:
            logger.error('Form attributes are not provided: form\'s index '
                        '(integer), form\'s field name (string) and form\'s '
                        'field value. It should be in dictionary format. '
                        'Example: yform = {1: {"username": "abc@gmail.com",'
                        '"password": "xYz"}}')
            raise ValueError

        # Login to the web page before sending any requests
        if self.prop['login_form']:
            self._login()
            
            if self.login_success:
                self.open(self.prop['url'])
            else:
                msg = 'Failed to login. Wrong username or password.'
                login_auth = self.prop['login_form']['fields']
                login_auth = f'Login authentication: {login_auth}'
                logger.error(' '.join([msg, login_auth]))
                raise ValueError

        return
    

    def cp68_extract_data(
        self,
        symbol,
        link,
        data,
        records=None,
        to_folder=None
    ):
        self.symbol = symbol

        try:
            dwl_link = self._get_dwl_link(link)
            bytes_data = self._get_bytes_data(dwl_link)
            self.load_status = True
        except:
            logger.error(f'{self.symbol} - Cannot get download link')
            self.load_status = False
        
        if to_folder:
            try:
                filename = qfutils._get_url_filename(dwl_link)
                self._to_folder(bytes_data, filename, to_folder)
            except:
                self.load_status = False
                logger.error(f'{self.symbol} - Cannot save file '
                             f'{filename} to folder {to_folder}')
            
            return
        
        else:
            try:
                df = self._to_file_object(data, bytes_data, records)
            except:
                self.load_status = False
                logger.error(f'{self.symbol} - Cannot read table data')
            
            try:
                self.consolidated_df = pd.concat(
                    [self.consolidated_df, df],
                    ignore_index=True
                )
            except:
                logger.warning(f'{self.symbol} - Cannot append result data '
                               'to class dataframe')

            return df
    

    def get_text_link_data(self):

        soup = self.get_current_page()
        soup_reader = SoupReader(soup)

        text_data = soup_reader.read_html_tabular_text(
            tag_attr=self.prop['table_attr'],
            skiprows=self.prop['skiprows'],
            skipcols=self.prop['skipcols']
        )

        link_data = soup_reader.read_html_tabular_link(
            tag_attr=self.prop['table_attr'],
            skiprows=self.prop['skiprows'],
            skipcols=self.prop['skipcols']
        )
    
        return text_data, link_data
    

    def _to_folder(self, bytes_data, filename, folder_directory):
        
        filename = f'{folder_directory}/{filename}.txt'
        try:
            with open(filename, 'wb') as f:
                f.write(bytes_data.content)
                logger.info(f'{self.symbol} - File {filename} is created')
        except:
            logger.error(f'{self.symbol} - Unable to download data from web. '
                         f'Filename: {filename}',
                         exc_info=True)
        
        return
    

    def _to_file_object(self, data, bytes_data, records):

        try:
            raw_data = StringIO(str(bytes_data.content, 'utf-8'))
            
            df = pd.read_csv(raw_data, nrows=records)
            
            df = qfutils._set_columns(
                df=df,
                dict_cols=self.prop[data]['dict_cols'],
                selected_cols=self.prop[data]['selected_cols'],
                date_cols=self.prop['date_index'],
                result_date_format=self.prop[data]['result_date_format']
            )

            if not records:
                records = len(df)
            
            logger.info(f'{self.symbol} - Successfully extract '
                        f'{records} records')
        
        except:
            logger.info(f'{self.symbol} - Empty data')

        return df
    

    def _get_bytes_data(self, dwl_link):

        try:
            bytes_data = self.get(dwl_link)
            return bytes_data
        except:
            logger.error(f'{self.symbol} - Unable to download '
                         f'data from web. Link: {dwl_link}',
                         exc_info=True)
            raise ValueError
    

    def _get_dwl_link(self, link):

        try:
            dwl_link = ''.join([self.url_hostname, link])
            return dwl_link
        except:
            logger.warning(f'{self.symbol} - There is no '
                           f'downloadable data')
            raise ValueError
    

    def _login(self):

        self._fill_form(
            self.prop['login_url'],
            self.prop['login_form']
        )

        soup = self.get_current_page()
        soup_reader = SoupReader(soup)
        msg = soup_reader._read_soup(self.prop['failed_login'])

        if not msg:
            self.login_success = True
        else:
            self.login_success = False
        
        return

    def _fill_form(self, url, form):

        self.open(url)
        self.select_form('form', form['idx'])

        for field_name, field_value in form['fields'].items():
            self[field_name] = field_value

        self.submit_selected()

        return
