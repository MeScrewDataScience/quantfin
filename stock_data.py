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


def convert_datetime(datetime_list):
    datetime_list = [dt.strptime(date, '%d/%m/%Y') for date in datetime_list]

    return datetime_list


def validate_data(same_df=True,
                  df_base=pd.DataFrame(),
                  df_ref=pd.DataFrame(),
                  base_cols=[],
                  ref_cols=[]):

    if not isinstance(df_base, pd.DataFrame):
        df_base = pd.DataFrame(df_base)
        warnings.warn('The base data is not a pandas.DataFrame object. '
                      'It will be converted to pandas.DataFrame object. '
                      'There could be some unexpected erros rising thereon.')
    if not isinstance(df_ref, pd.DataFrame):
        df_ref = pd.DataFrame(df_ref)
        warnings.warn('The ref data is not a pandas.DataFrame object. '
                      'It will be converted to pandas.DataFrame object. '
                      'There could be some unexpected erros rising thereon.')

    if len(df_base.index) != len(df_ref.index):
        raise ValueError('The number of rows in base dataframe does not '
                         'equal the number of ref dataframe')

    validation = validation_create(tests=['base_null',
                                          'ref_null',
                                          'data_mismatch'],
                                   length=len(df_base.index))

    if same_df:
        if not df_ref.empty:
            del df_ref
            warnings.warn('Ref dataframe is not neccessary because we are '
                          'validating data within the same DataFrame')

        for base_col, ref_col in zip(base_cols, ref_cols):
            validation = validation_process(validation,
                                            df_base,
                                            df_base,
                                            base_col,
                                            ref_col)

    else:
        if df_ref.empty:
            raise ValueError('The ref data is not provided!')

        for base_col, ref_col in zip(base_cols, ref_cols):
            validation = validation_process(validation,
                                            df_base,
                                            df_ref,
                                            base_col,
                                            ref_col)

    # Show the result
    for key in validation:
        print(key, 'test')
        print(validation[key].describe(), end='\n\n')

    return validation


def validation_create(tests=['base_null', 'ref_null', 'data_mismatch'],
                      length=0):

    dummy_field = [False] * length
    validation = {test: dummy_field for test in tests}

    return validation


def validation_process(validation, df_base, df_ref, base_col, ref_col):

    validation['base_null'] = df_base[base_col].isna() ^ validation['base_null']
    validation['ref_null'] = df_ref[ref_col].isna() ^ validation['ref_null']
    validation['data_mismatch'] = (df_base[base_col] != df_ref[ref_col]) \
                                   ^ validation['data_mismatch']

    return validation


def dwl_data(dwl_directory,
             main_url,
             form_url,
             form={},
             dwl_table={'tag_type': 'table',
                        'attr': None,
                        'attr_val': None,
                        'pos': 0,
                        'lookup_type': 'find_all'},
             row_col_pair=['tr', 'td'],
             header_row=0,
             ticker_col=0,
             get_link_from_cols=[],
             exclude_rows=[],
             exclude_cols=[],
             login_required=False):

    if dt.now().weekday() < 5 and dt.now().time() >= t(8) \
    and dt.now().time() <= t(15):
        raise ValueError('This website doesn\'t allow to'
                         ' download data during trading hours!')

    exclude_rows_copy = exclude_rows.copy()
    exclude_cols_copy = exclude_cols.copy()

    if header_row in exclude_rows_copy:
        exclude_rows_copy.remove(0)

    if ticker_col in exclude_cols_copy:
        exclude_cols_copy.remove(0)

    # Get links from table
    browser = browse_url(main_url)
    table = get_table(browser.get_current_page(),
                      dwl_table,
                      row_col_pair,
                      header_row,
                      get_link_from_cols,
                      exclude_rows_copy,
                      exclude_cols_copy)
    total_lines = len(table)

    # Refine URL & directory
    hostname_url = url_elements(get_hostname_url=True, url=main_url)
    dwl_directory = refine_directory(dwl_directory)

    # Login to website
    browser = fill_form(form_url, browser, form)

    # Scrape all downloadable links and get data
    for i, row in enumerate(table):
        if i == header_row:
            continue

        ticker = row[ticker_col]

        for j, col in enumerate(row):
            if j == ticker_col:
                continue

            dwl_link = col
            if not 'http' in dwl_link:
                dwl_link = ''.join([hostname_url, dwl_link])

            try:
                raw = browser.get(dwl_link)
                filename = url_elements(get_filename=True, url=dwl_link)
                filename = ''.join([dwl_directory, '/', filename, '.txt'])
                with open(filename, 'wb') as f:
                    f.write(raw.content)
            except Exception as e:
                print(e)
                print(dwl_link)
                print(filename)

        print(ticker, 'downloaded. Progress:', end=' ')
        print(''.join([str(round(i*100/total_lines, 2)), '%']))

        time.sleep(0.5)

    browser.close()

    print('Download complete!')

    return


def refine_directory(directory):

    directory.replace('\\', '/')

    if directory[-1] == '/':
        directory = directory[:-1]

    return directory


def url_elements(get_hostname_url=False, get_filename=False, url=''):

    parsed_url = urlparse(url)

    if get_hostname_url:

        return f'{parsed_url.scheme}://{parsed_url.hostname}/'

    elif get_filename:
        prefix = parsed_url.path
        prefix = prefix[prefix.rindex('/') + 1:prefix.rindex('.')]

        suffix = parsed_url.query[parsed_url.query.index('=') + 1:]
        suffix = suffix.translate(str.maketrans('', '', string.punctuation))

        return ''.join([prefix, '_', suffix])


def scrape_table(main_url=None,
                 form_url=None,
                 form={},
                 multi_pages=False,
                 main_table={'tag_type': 'table',
                             'attr': None,
                             'attr_val': None,
                             'pos': 0,
                             'lookup_type': 'find_all'},
                 row_col_pair=['tr', 'td'],
                 header_row=0,
                 pagination_box={'tag_type': None,
                                 'attr_val': None,
                                 'pos': 0,
                                 'lookup_type': 'find'},
                 page_start_string='',
                 page_end_string='',
                 exclude_rows=[],
                 exclude_cols=[],
                 df_columns=[]):

    if multi_pages and not page_start_string:
        raise ValueError('Please specify the string in the url '
                         'which identifies the start point of pagination!')
    elif multi_pages and not page_end_string:
        raise ValueError('Please specify the string in the url '
                         'which identifies the end point of pagination!')
    elif not df_columns:
        raise ValueError('Please specify column\'s name of output dataframe!')

    else:
        print('Reading the table from the URL...')
        df = extract_tabulator_data(main_url,
                                    form_url,
                                    form,
                                    multi_pages,
                                    main_table,
                                    row_col_pair,
                                    header_row,
                                    pagination_box,
                                    page_start_string,
                                    page_end_string,
                                    [],
                                    exclude_rows,
                                    exclude_cols)

        # If the returned tabulator data (2-dimension table-like list) has
        # different number of columns comparing to the pre-defined columns,
        # still return a dataframe but without column's name
        if not df:
            print(form)
            warnings.warn('No data were found. Check your inputs & URL!')
        elif len(df[0]) != len(df_columns):
            extracted_cols = len(df[0])
            pre_def_cols = len(df_columns)
            df = pd.DataFrame(df)
            warnings.warn(f'The number of pre-defined columns ({pre_def_cols})'
                          f' does not equal the number of columns in the'
                          f' extracted table ({extracted_cols})!'
                          f'The returned data is a headless dataframe.')
            return df

        else:
            df = pd.DataFrame(df, columns=df_columns)
            return df


def extract_tabulator_data(main_url=None,
                           form_url=None,
                           form={},
                           multi_pages=False,
                           main_table={'tag_type': 'table',
                                       'attr': None,
                                       'attr_val': None,
                                       'pos': 0,
                                       'lookup_type': 'find_all'},
                           row_col_pair=['td', 'tr'],
                           header_row=0,
                           pagination_box={'tag_type': None,
                                           'attr_val': None,
                                           'pos': 0,
                                           'lookup_type': 'find'},
                           page_start_string='',
                           page_end_string='',
                           get_link_from_cols=[],
                           exclude_rows=[],
                           exclude_cols=[]):

    # If the html table has only one page,
    # return the re-structured 2-dimension table-like list
    if not multi_pages:
        print('Getting the table from the URL...')
        browser = browse_url(main_url, form_url, form)
        data = get_table(browser.get_current_page(),
                         main_table,
                         row_col_pair,
                         header_row,
                         get_link_from_cols,
                         exclude_rows,
                         exclude_cols)
        browser.close()

        print('All done!')

        return data

    # If the html table has multiple pages, find the last pagination
    # and loop through all pages and return a joined 2-dimension list
    else:
        # Find the last page of the table
        print('Looking for pagination part of the table...')
        last_page = find_last_pagination(main_url,
                                         form_url,
                                         form,
                                         pagination_box,
                                         page_start_string,
                                         page_end_string)
        split_url = main_url[:main_url.index(page_start_string)]
        data = []

        if not last_page:
            raise ValueError('Cannot get the last page of the '
                             'table from the provided link!')

        else:
            print(f'There are {last_page} pages in the table.\n'
                  f'Start looping through all pages of the table...')

            for page in range(1, last_page + 1):
                print(f'Page {page}/{last_page}', end='...')

                # Concatenate full link of this particular page
                main_url = ''.join([split_url, page_start_string,
                                   str(page), page_end_string])

                # Extract tabulator data
                print('Getting the table from the URL', end='...')
                browser = browse_url(main_url, form_url, form)
                table = get_table(browser.get_current_page(),
                                  main_table,
                                  row_col_pair,
                                  header_row,
                                  get_link_from_cols,
                                  exclude_rows,
                                  exclude_cols)
                data.extend(table)
                browser.close()

                print('Done!')
                time.sleep(0.5)

            print('All done!')

            return data


def get_table(soup,
              table_container={'tag_type': 'table',
                               'attr': None,
                               'attr_val': None,
                               'pos': 0,
                               'lookup_type': 'find_all'},
              row_col_pair=['tr', 'td'],
              header_row=None,
              get_link_from_cols=[],
              exclude_rows=[],
              exclude_cols=[]):

    data = []

    table = get_soup(soup, table_container)
    rows = table.find_all(row_col_pair[0])

    for i, row in enumerate(rows):
        values = []
        if exclude_rows and i in exclude_rows:
            continue
        cols = row.find_all(row_col_pair[1])

        for j, col in enumerate(cols):
            if exclude_cols and j in exclude_cols:
                continue

            if i != header_row and j in get_link_from_cols:
                val = col.find('a')
                if not val:
                    continue
                val = val.get('href')

            else:
                val = col.text
                val = ''.join(val.split())  # Remove redundant spaces

            values.append(val)

        data.append(values)

    return data


def find_last_pagination(main_url=None,
                         form_url=None,
                         form={},
                         pagination_box={'tag_type': None,
                                         'attr': None,
                                         'attr_val': None,
                                         'pos': 0,
                                         'lookup_type': 'find'},
                         page_start_string='',
                         page_end_string=''):

    browser = browse_url(main_url, form_url, form)
    soup = get_soup(browser.get_current_page(), pagination_box)

    # Find last page
    last_a = str(soup.find_all('a')[-1])
    page_start_pos = last_a.find(page_start_string) + len(page_start_string)
    page_end_pos = last_a.find(page_end_string)
    last_page = last_a[page_start_pos:page_end_pos]

    browser.close()

    return int(last_page)


def get_soup(soup, tag={'tag_type': None,
                        'attr': None,
                        'attr_val': None,
                        'pos': 0,
                        'lookup_type': 'find_all'}):

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


def browse_url(main_url=None, form_url=None, form={}):

    if not form_url and form:
        raise ValueError('Form URL is not provided! Note: Form URL could be '
                         'the same as main URL if the form(s) is/are located '
                         'in the same main URL')

    if form_url and not form:
        raise ValueError('Form attributes are not provided: form\'s position '
                         '(integer), form\'s field name (string) and form\'s '
                         'field value.\nIt should be in dictionary format. '
                         'Example:\n'
                         'form = {1: {"username": "abc@gmail.com",'
                                     '"password": "xYz"}}')

    browser = mechanicalsoup.StatefulBrowser(soup_config={'features': 'lxml'},
                                             raise_on_404=True)

    if form:
        browser = fill_form(form_url, browser, form)

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


def fill_form(url, browser, form={}):

    if not isinstance(browser, mechanicalsoup.stateful_browser.StatefulBrowser):
        raise ValueError('Your browser variable is not a '
                         '<mechanicalsoup.stateful_browser.StatefulBrowser> '
                         'object!')

    try:
        browser.open(url)
    except IndexError as e:
        browser.close()
        print(e)
        print('Link not found!')
    except AttributeError as e:
        browser.close()
        print(e)
        print('Not sure what error it is!')

    browser.select_form('form', form['pos'])
    for field_name, field_value in form['fields'].items():
        browser[field_name] = field_value

    browser.submit_selected()

    return browser
