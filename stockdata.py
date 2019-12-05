# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 02:42:52 2019

@author: baoloc.lam
"""

import mechanicalsoup
import bs4
import string
import pandas as pd
import time
import warnings
#from io import StringIO
from datetime import datetime as dt, time as t
from urllib.parse import urlparse


class StockData():

    def __init__(self, *args, **kwargs):
        # Use the __init__ method from DataFrame to ensure
        # that we're inheriting the correct behavior
        # super(StockData, self).__init__(*args, **kwargs)
        # Call the inner class Validation
        self.validation = self.Validation()
        # Define class arguments
        self.main_url = ''
        self.form_url = ''
        self.row_col_pair = ['tr', 'td']
        self.multi_pages = False
        self.page_start_string = ''
        self.page_end_string = ''
        self.main_table_config = {
            'tag_type': 'table',
            'attr': None,
            'attr_val': None,
            'pos': 0,
            'lookup_type': 'find_all'
        }
        self.pagination_config = {
            'tag_type': None,
            'attr_val': None,
            'pos': 0,
            'lookup_type': 'find'
        }
        self.form_config = {}
    
    # @property
    # def _constructor(self):
    #     return StockData

    # @property
    # def _constructor_sliced(self):
    #     return StockData
    

    def convert_datetime(self, datetime_list):
        datetime_list = [dt.strptime(date, '%d/%m/%Y') for date in datetime_list]

        return datetime_list
    
    
    def dwl_data(self, ticker_col=None, excluded_rows=[], excluded_cols=[], get_link_from_cols=[], save_folder_directory=None):

        if dt.now().weekday() < 5 and dt.now().time() >= t(8) \
            and dt.now().time() <= t(15):
            warnings.warn('The website could disapprove data download during trading hours!')
        
        if ticker_col in excluded_cols:
            excluded_cols.remove(ticker_col)
        
        # Get all downloadable links
        links = self._construct_tabular_data(excluded_rows, excluded_cols, get_link_from_cols)
        total_lines = len(links)

        # Refine URL & save folder directory
        hostname_url = self._url_elements(get_hostname_url=True, url=self.main_url)
        save_folder_directory = self._refine_directory(save_folder_directory)

        # Login to website and scrape all downloadable links to get data
        browser = self._browse_url(self.main_url, self.form_url, self.form_config)

        for i, row in enumerate(links):
            ticker = row[ticker_col]

            for j, col in enumerate(row):
                if j == ticker_col:
                    continue

                dwl_link = col

                if not 'http' in dwl_link:
                    dwl_link = ''.join([hostname_url, dwl_link])

                try:
                    raw_data = browser.get(dwl_link)
                    filename = self._url_elements(get_filename=True, url=dwl_link)
                    filename = ''.join([save_folder_directory, '/', filename, '.txt'])
                    with open(filename, 'wb') as f:
                        f.write(raw_data.content)
                
                except Exception as e:
                    print(e)
                    print(dwl_link)
                    print(filename)

            print(ticker, 'downloaded. Progress:', end=' ')
            print(''.join([str(round(i*100/total_lines, 2)), '%']))

            time.sleep(0.1)

        browser.close()

        print('Download complete!')

        return


    def get_live_snapshot(self, excluded_rows=[], excluded_cols=[], result_column_names=[]):

        if self.multi_pages and not self.page_start_string:
            raise ValueError('Please specify the string in the url '
                            'which identifies the start point of pagination!')
        
        if self.multi_pages and not self.page_end_string:
            raise ValueError('Please specify the string in the url '
                            'which identifies the end point of pagination!')
        if not result_column_names:
            raise ValueError('Please specify the column\'s name of the result dataframe!')

        print('Reading the table from the URL...')
        df = self._construct_tabular_data(excluded_rows, excluded_cols, get_link_from_cols=[])

        # If the returned tabulator data (2-dimension table-like list) has
        # different number of columns comparing to the pre-defined columns,
        # still return a dataframe but without column's name
        if not df:
            print(self.form_config)
            warnings.warn('No data were found. Check your inputs & URL!')
        
        if len(df[0]) != len(result_column_names):
            extracted_cols = len(df[0])
            pre_def_cols = len(result_column_names)
            warnings.warn(
                f'The number of pre-defined columns ({pre_def_cols})'
                f' does not equal the number of columns in the'
                f' extracted table ({extracted_cols})!'
                f'The returned data is a headless dataframe.')
            df = pd.DataFrame(df)
        
        else:
            df = pd.DataFrame(df, columns=result_column_names)
        
        return df


    def _refine_directory(self, directory):

        if not directory:
            raise ValueError('Please specify the download folder directory')

        directory.replace('\\', '/')

        if directory[-1] == '/':
            directory = directory[:-1]

        return directory


    def _url_elements(self, get_hostname_url=False, get_filename=False, url=''):

        parsed_url = urlparse(url)

        if get_hostname_url:
            return f'{parsed_url.scheme}://{parsed_url.hostname}/'

        elif get_filename:
            prefix = parsed_url.path
            prefix = prefix[prefix.rindex('/') + 1:prefix.rindex('.')]

            suffix = parsed_url.query[parsed_url.query.index('=') + 1:]
            suffix = suffix.translate(str.maketrans('', '', string.punctuation))

            return ''.join([prefix, '_', suffix])


    def _construct_tabular_data(self, excluded_rows, excluded_cols, get_link_from_cols):

        # If the html table has only one page,
        # return the re-structured 2-dimension table-like list
        if not self.multi_pages:
            print('Getting the table from the URL...')
            browser = self._browse_url(self.main_url, '', {})
            tabular_data = self._get_data_from_table(
                soup=browser.get_current_page(),
                excluded_rows=excluded_rows,
                excluded_cols=excluded_cols,
                get_link_from_cols=get_link_from_cols,
            )
            browser.close()

            print('All done!')

            return tabular_data

        # If the html table has multiple pages, find the last pagination
        # and loop through all pages and return a combined 2-dimension table-like list
        else:
            # Find the last page of the table
            print('Looking for pagination part of the table...')
            last_page = self._find_last_pagination()
            split_url = self.main_url[:self.main_url.index(self.page_start_string)]
            tabular_data = []

            if not last_page:
                raise ValueError('Cannot get the last page of the '
                                'table from the provided link!')

            else:
                print(f'There are {last_page} pages in the table.\n'
                      f'Start looping through all pages of the table...')

                for page in range(1, last_page + 1):
                    print(f'Page {page}/{last_page}', end='...')

                    # Concatenate full link of this particular page
                    new_main_url = ''.join([split_url, self.page_start_string,
                                        str(page), self.page_end_string])

                    # Extract tabulator data
                    print('Getting the data from the table', end='...')
                    browser = self._browse_url(new_main_url, '', {})
                    data = self._get_data_from_table(
                                soup=browser.get_current_page(),
                                excluded_rows=excluded_rows,
                                excluded_cols=excluded_cols,
                                get_link_from_cols=get_link_from_cols,
                            )
                    tabular_data.extend(data)
                    browser.close()

                    print('Done!')
                    time.sleep(0.1)

                print('All done!')

                return tabular_data


    def _get_data_from_table(self, soup, excluded_rows, excluded_cols, get_link_from_cols):

        data = []
        table_soup = self._get_soup(soup, self.main_table_config)
        rows = table_soup.find_all(self.row_col_pair[0])

        for i, row in enumerate(rows):
            values = []
            if excluded_rows and i in excluded_rows:
                continue
            
            cols = row.find_all(self.row_col_pair[1])

            for j, col in enumerate(cols):
                if excluded_cols and j in excluded_cols:
                    continue

                if j in get_link_from_cols:
                    val = col.find('a')

                    if not val:
                        warnings.warn(
                            f'Cannot get any link from data cell at row {i} '
                            f'column {j}. Row initial value: {data[i][0]}.\n'
                            f'Moving to the next data cell...')
                    
                    else:
                        val = val.get('href')

                else:
                    val = col.text
                    val = ''.join(val.split())  # Remove redundant spaces

                values.append(val)

            data.append(values)

        return data


    def _find_last_pagination(self):

        browser = self._browse_url(self.main_url, self.form_url, self.form_config)
        soup = self._get_soup(browser.get_current_page(), self.pagination_config)

        # Find last page
        last_a = str(soup.find_all('a')[-1])
        page_start_pos = last_a.find(self.page_start_string) + len(self.page_start_string)
        page_end_pos = last_a.find(self.page_end_string)
        last_page = last_a[page_start_pos:page_end_pos]

        browser.close()

        return int(last_page)


    def _get_soup(self, soup, tag):

        if not soup and isinstance(soup, bs4.BeautifulSoup):
            raise ValueError('The provided object is not '
                            'a bs4.BeautifulSoup object!')

        if tag['attr'] and not tag['attr_val']:
            raise ValueError(f"Tag\'s attribute value is missing! "
                            f"{tag['attr']}, {tag['attr_val']}")

        if not tag['attr'] and tag['attr_val']:
            raise ValueError('Tag\'s attribute name is missing!')

        if tag['lookup_type'] == 'find_all':
            soup = soup.find_all(tag['tag_type'],
                                {tag['attr']: tag['attr_val']})[tag['pos']]

        elif tag['lookup_type'] == 'find':
            soup = soup.find(tag['tag_type'], {tag['attr']: tag['attr_val']})

        else:
            raise ValueError('The internal get_soup function only support either'
                            ' \'find\' or \'find_all\' features!')

        return soup


    def _browse_url(self, main_url, form_url, form_config):

        if not form_url and form_config:
            raise ValueError('Form URL is not provided! Note: Form URL could be '
                            'the same as main URL if the form(s) is/are located '
                            'in the same main URL')

        if form_url and not form_config:
            raise ValueError('Form attributes are not provided: form\'s position '
                            '(integer), form\'s field name (string) and form\'s '
                            'field value.\nIt should be in dictionary format. '
                            'Example:\n'
                            'form = {1: {"username": "abc@gmail.com",'
                            '"password": "xYz"}}')

        browser = mechanicalsoup.StatefulBrowser(soup_config={'features': 'lxml'},
                                                raise_on_404=True)

        if form_config:
            browser = self._fill_form(form_url, browser, form_config)

        if main_url == form_url:
            return browser
        else:
            try:
                browser.open(main_url)
            except IndexError as e:
                browser.close()
                print(f"Link not found!\nURL: {main_url}")
                print(e)
            except AttributeError as e:
                browser.close()
                print("Not sure what error it is!\nURL: {main_url}")
                print(e)
            except Exception as e:
                browser.close()
                print("Unexpected error!\nURL: {main_url}")
                print(e)

            return browser


    def _fill_form(self, form_url, browser, form_config):

        if not isinstance(browser, mechanicalsoup.stateful_browser.StatefulBrowser):
            raise ValueError('Your browser variable is not a '
                            '<mechanicalsoup.stateful_browser.StatefulBrowser> '
                            'object!')

        try:
            browser.open(form_url)
        except IndexError as e:
            browser.close()
            print(e)
            print('Link not found!')
        except AttributeError as e:
            browser.close()
            print(e)
            print('Not sure what error it is!')

        browser.select_form('form', form_config['pos'])
        for field_name, field_value in form_config['fields'].items():
            browser[field_name] = field_value

        browser.submit_selected()

        return browser


    class Validation:
        
        def __init__(self):
            self.df = pd.DataFrame()
            self.base_cols = []
            self.ref_cols = []
            self.validation_criteria = ['base_null', 'ref_null', 'data_mismatch']

        
        def validate_data(self):

            if not isinstance(self.df, pd.DataFrame):
                warnings.warn('The input data is not a pandas.DataFrame object. '
                              'It will be converted to pandas.DataFrame object. '
                              'There could be some unexpected erros arising thereon.')
                self.df = pd.DataFrame(self.df)

            validation = self._validation_create(len(self.df.index))

            for base_col, ref_col in zip(self.base_cols, self.ref_cols):
                validation = self._validation_process(validation, base_col, ref_col)

            # Show the result
            for key in validation:
                print(key, 'test')
                print(validation[key].describe(), end='\n\n')

            return validation


        def _validation_create(self, length):

            dummy_field = [False] * len(self.df.index)
            validation = {test: dummy_field for test in self.validation_criteria}

            return validation


        def _validation_process(self, validation, base_col, ref_col):

            validation['base_null'] = self.df[base_col].isna() ^ validation['base_null']
            validation['ref_null'] = self.df[ref_col].isna() ^ validation['ref_null']
            validation['data_mismatch'] = (self.df[base_col] != self.df[ref_col]) ^ validation['data_mismatch']

            return validation