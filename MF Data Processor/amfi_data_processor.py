import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger

from amfi_service import get_all_fund_house_and_scheme_names


@logger.catch
def get_data_frame_from_html_code(html_code, date_string):
    logger.info('Processing HTML code for {}', date_string)
    data_frame = get_all_fund_house_and_scheme_names()
    if html_code is None or not len(html_code) > 0:
        logger.critical('HTML code for date {} was missing', date_string)
        return data_frame
    scheme_names = data_frame['Scheme Name']
    fund_house_names = data_frame['Fund House Name'].tolist()
    soup = BeautifulSoup(html_code, 'html5lib')
    table_rows = soup.find('tbody').find_all('tr')
    table_rows = table_rows[2:]  # omitting headers
    found_scheme_name_in_html_row = False
    scheme_found_index = -1
    scheme_found_name = ''
    scheme_index_series = pd.Index(scheme_names)
    navs_found = 0
    zero_nav_count = 0
    navs_not_found_after_scheme = 0
    fund_house_name = ''
    for row in table_rows:
        headers = row.find_all('th')
        if len(headers) > 0:
            header_text = headers[0].text.strip()
            if header_text in fund_house_names:
                fund_house_name = header_text
        columns = row.find_all('td')
        row_value = ''
        if columns is not None and len(columns) > 0:
            first_column = columns[0]
            row_value = first_column.text.strip()

        if len(row_value) > 0:
            if found_scheme_name_in_html_row:
                nav = -1
                try:
                    nav = float(row_value)
                except Exception as e:
                    pass
                if nav > 0:
                    data_frame.at[scheme_found_index, date_string] = nav
                    navs_found += 1
                elif nav < 0:
                    navs_not_found_after_scheme += 1
                    # logger.info('NAV value was invalid for \n{} - {}', fund_house_name, scheme_found_name)
                else:
                    zero_nav_count += 1
            elif not scheme_names[scheme_names == row_value].empty:
                indexes_matching_with_row_value = data_frame[data_frame['Scheme Name'] == row_value].index
                scheme_found_index = scheme_index_series.get_loc(row_value)

                if len(indexes_matching_with_row_value) == 1:
                    scheme_found_index = indexes_matching_with_row_value[0]
                else:
                    for index in indexes_matching_with_row_value:
                        if fund_house_name == data_frame.loc[index, 'Fund House Name']:
                            scheme_found_index = index
                            break
                scheme_found_name = row_value
                found_scheme_name_in_html_row = True
                continue
            else:
                # pass
                logger.debug('No scheme matched for the row \n{} - {}', fund_house_name, row_value)

        found_scheme_name_in_html_row = False
        scheme_found_index = -1
        scheme_found_name = ''
    logger.success('NAV values were found for {} schemes for the date {}', navs_found, date_string)
    logger.success('NAV values were NaN for {} schemes for the date {}', navs_not_found_after_scheme,
                   date_string)
    logger.success('NAV values were zero for {} schemes for the date {}', zero_nav_count, date_string)
    return data_frame
