from datetime import datetime

import pandas as pd

from general_utils import find_difference_between_two_lists


def get_valid_dates_for_a_year(year):
    if year == '2006':
        start_date = '01-04-2006'
    else:
        start_date = '01-01-' + year
    current_year = str(datetime.now().year)
    if year == current_year:
        return get_missing_dates_from_list([], start_date_string=start_date)
    else:
        end_date = '31-12-' + year
        return get_missing_dates_from_list([], start_date_string=start_date, end_date_required=True,
                                           end_date_string=end_date)


def get_missing_dates_from_list(existing_dates=[], start_date_string='01-04-2006', end_date_required=False,
                                end_date_string=''):
    date_format = '%d-%m-%Y'
    start_date = pd.to_datetime(start_date_string, format=date_format)
    if end_date_required and len(end_date_string) == 10:
        end_date = pd.to_datetime(end_date_string, format=date_format)
    else:
        end_date = pd.to_datetime(datetime.now()) - pd.offsets.DateOffset(days=1)  # yesterday
    all_dates = pd.date_range(start_date, freq='D', end=end_date)
    all_dates = all_dates.format(formatter=lambda x: x.strftime(date_format))
    return find_difference_between_two_lists(all_dates, existing_dates)


def get_missing_dates_list_from_data_frame(data_frame, start_date_string='01-04-2006', end_date_required=False,
                                           end_date_string=''):
    existing_dates = data_frame.columns.values[2:]
    return get_missing_dates_from_list(existing_dates, start_date_string, end_date_required, end_date_string)


def get_years_from_dates(dates):
    return sorted(set(map(lambda x: x[-4:], dates)))


def re_format_date(date_string, current_format, new_format):
    return datetime.strptime(date_string, current_format).strftime(new_format)
