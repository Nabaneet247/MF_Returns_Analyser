import plotly.express as px
from loguru import logger

from Interval import Interval
from data_labels import get_common_label_values, get_all_stat_labels_for_an_interval, get_stat_label_for_key, \
    get_common_label_for_key
from general_utils import get_interval_data_from_strings
from mf_data_provider import get_analysed_file_for_date
from mf_data_saver import save_html_plot

supported_intervals = ['30D', '3M', '6M', '1Y', '2Y', '5Y']


# Adjusts the V-Score to the range of 0-100
def adjust_v_score(v_score, max_v, min_v):
    if max_v == min_v:
        return 0
    return round((v_score - min_v) / (max_v - min_v) * 100, 1)


def adjust_floating_precision(x):
    if isinstance(x, float):
        return round(x, 3)
    else:
        return x


def get_filtered_data(data, interval):
    returns_count_label = get_stat_label_for_key(key='count', interval=interval)
    vol_score_label = get_stat_label_for_key(key='v score', interval=interval)
    mean_label = get_stat_label_for_key(key='mean', interval=interval)
    filtered_data = data[data[vol_score_label].notna()]
    filtered_data = filtered_data.applymap(adjust_floating_precision)
    v_score_lower_limit = filtered_data[vol_score_label].quantile(0.05)
    v_score_upper_limit = filtered_data[vol_score_label].quantile(0.95)
    filtered_data = filtered_data[filtered_data[vol_score_label] >= v_score_lower_limit]
    filtered_data = filtered_data[filtered_data[vol_score_label] <= v_score_upper_limit]
    filtered_data[vol_score_label] = filtered_data[vol_score_label].apply(adjust_v_score, args=(
        v_score_upper_limit, v_score_lower_limit))
    filtered_data = filtered_data[filtered_data[returns_count_label] > 30]
    filtered_data = filtered_data[filtered_data[mean_label] > 0]
    logger.info('{} rows were removed before plotting', len(data.index) - len(filtered_data.index))
    return filtered_data


def filter_active_schemes(data):
    active_label = get_common_label_for_key('scheme active')
    return data[data[active_label] == 'Yes']


def plot_data_for_an_interval(data, interval: Interval, date):
    common_labels = ['Fund House Name', 'Scheme Name']
    common_labels.extend(get_common_label_values())
    stat_labels = get_all_stat_labels_for_an_interval(interval)

    x_axis_column_name = get_stat_label_for_key(key='v score', interval=interval)
    y_axis_column_name = get_stat_label_for_key(key='mean', interval=interval)
    hover_data = common_labels
    hover_data.extend(stat_labels)

    # plot = px.scatter(get_filtered_data(data, interval), x=x_axis_column_name, y=y_axis_column_name,
    #                   hover_data=hover_data, color='Fund House Name')
    # save_html_plot(plot, date, interval)

    plot = px.scatter(get_filtered_data(filter_active_schemes(data), interval), x=x_axis_column_name,
                      y=y_axis_column_name,
                      hover_data=hover_data, color='Fund House Name')
    save_html_plot(plot, date, interval, file_name_prefix='Active')


def plot_analysed_data(data, date):
    data_columns = data.columns.values[2:]
    present_intervals = set()
    for interval in supported_intervals:
        for column in data_columns:
            if column.startswith(interval):
                present_intervals.add(interval)
                break
    present_intervals = list(present_intervals)
    logger.info('{} intervals were found for {}', present_intervals, date)
    for interval in get_interval_data_from_strings(present_intervals):
        plot_data_for_an_interval(data, interval, date)


def plot_analysed_data_file_for_a_date(date):
    try:
        data = get_analysed_file_for_date(date)
        plot_analysed_data(data, date)
    except FileNotFoundError:
        logger.error('Analysed file for {} was not found', date)


if __name__ == '__main__':
    plot_analysed_data_file_for_a_date('06-07-2021')
