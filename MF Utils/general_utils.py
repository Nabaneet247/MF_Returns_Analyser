import math

import pandas as pd

from Interval import Interval
from string_utils import get_period_labels_from_string


def find_difference_between_two_lists(first_list, second_list):
    missing_values = []
    for values in first_list:
        if values not in second_list:
            missing_values.append(values)
    return missing_values


def get_interval_data_from_strings(strings):
    intervals = []
    for string in strings:
        interval = get_interval_data_from_string(string)
        intervals.append(interval)
    return intervals


def get_interval_data_from_string(string):
    interval = Interval()
    freq_value, freq_unit = get_period_labels_from_string(string)
    if freq_unit.startswith('y'):
        interval.value = freq_value
        interval.unit = 'Year'
        interval.change_rate_label = str(freq_value) + ' Year ' + 'CAGR'
        interval.abbreviation = str(freq_value) + 'Y'
        interval.offset = pd.offsets.DateOffset(years=freq_value)
        interval.max_dates_padded = freq_value * 14
    elif freq_unit.startswith('m'):
        interval.value = freq_value
        interval.unit = 'Month'
        interval.change_rate_label = str(freq_value) + ' Month ' + 'SAGR'
        interval.abbreviation = str(freq_value) + 'M'
        interval.offset = pd.offsets.DateOffset(months=freq_value)
        interval.max_dates_padded = freq_value * 3
    else:
        interval.value = freq_value
        interval.unit = 'Day'
        interval.change_rate_label = str(freq_value) + ' Day ' + 'SAGR'
        interval.abbreviation = str(freq_value) + 'D'
        interval.offset = pd.offsets.DateOffset(days=freq_value)
        interval.max_dates_padded = freq_value // 30 * 3
    return interval


def calculate_percentage(numerator, denominator, digits_after_decimal, calculate_change=False):
    numerator = float(numerator)
    denominator = float(denominator)
    if calculate_change:
        return round((numerator - denominator) / denominator * 100, digits_after_decimal)
    else:
        return round(numerator / denominator * 100, digits_after_decimal)


def compute_annualised_percentage_change(freq_unit, freq_value, absolute_percentage_change, digits_after_decimal):
    if freq_unit.startswith('D'):
        # Simple Interest
        return round(365 / freq_value * absolute_percentage_change, digits_after_decimal)
    elif freq_unit.startswith('M'):
        # Simple Interest
        return round(12 / freq_value * absolute_percentage_change, digits_after_decimal)
    else:
        # Compound Interest
        return round((((absolute_percentage_change + 100) / 100) ** (1 / freq_value) - 1) * 100, digits_after_decimal)


def get_log_base_10(x):
    return math.log(x, 10)


def get_log_base_2(x):
    try:
        return math.log(x, 2)
    except Exception as e:
        print(x)
        print(type(e))
        print(e)


def get_log_value(x, base):
    return math.log(x, base)
