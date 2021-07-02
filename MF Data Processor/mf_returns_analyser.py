import pandas as pd
from loguru import logger

import file_locations
from Interval import Interval
from data_labels import get_stat_label_for_key as s_label, get_common_label_for_key as c_label
from general_utils import get_interval_data_from_strings
from general_utils import get_log_base_2
from mf_data_provider import get_combined_year_wise_data_chunks, get_list_of_all_schemes
from mf_data_saver import save_analysed_data

pd.set_option("display.max_rows", None, "display.max_columns", None)
log_filename = file_locations.ANALYSED_DATA_FOLDER_PATH + 'analyser_logs.log'
logger_message_format = "{time} - {name} - {level} - {message}"
logger.add(log_filename, level="INFO", format=logger_message_format)
logger.add(log_filename, level="DEBUG", format=logger_message_format)
logger.add(log_filename, level="WARNING", format=logger_message_format)
logger.add(log_filename, level="ERROR", format=logger_message_format)


def analyse_returns_for_an_interval(data, i: Interval):
    analysed_data = data.iloc[:, :2]
    for index, data_row in data.iloc[:, 2:].iterrows():
        mean = data_row.mean()
        std = data_row.std()
        if data_row.count() == 0:
            continue
        analysed_data.loc[index, s_label('mean', i)] = round(mean, 2)
        analysed_data.loc[index, s_label('std', i)] = round(std, 3)
        analysed_data.loc[index, s_label('count', i)] = data_row.count()
        analysed_data.loc[index, s_label('min', i)] = round(data_row.quantile(0.0), 2)
        analysed_data.loc[index, s_label('10', i)] = round(data_row.quantile(0.1), 2)
        analysed_data.loc[index, s_label('50', i)] = round(data_row.quantile(0.5), 2)
        analysed_data.loc[index, s_label('90', i)] = round(data_row.quantile(0.9), 2)
        analysed_data.loc[index, s_label('max', i)] = round(data_row.quantile(1.0), 2)
        if not pd.isna(std) and std > 0:
            score = round(get_log_base_2(std), 3) * (-10)
            analysed_data.loc[index, s_label('v score', i)] = score
    return analysed_data


def analyse_and_add_common_scheme_data_for_a_chunk(data):
    analysed_data = data.iloc[:, :2]
    activity_days_threshold = 30
    for index, data_row in data.iloc[:, 2:].iterrows():
        if data_row.count() == 0:
            continue
        first_date = data_row.dropna().index.values[0]
        last_date = data_row.dropna().index.values[-1]
        last_nav = data_row[last_date]
        analysed_data.loc[index, c_label('first date')] = first_date
        analysed_data.loc[index, c_label('last date')] = last_date
        analysed_data.loc[index, c_label('last nav')] = round(last_nav, 3)
        analysed_data.loc[index, c_label('nav count')] = data_row.dropna().count()
        analysed_data.loc[index, c_label('scheme active')] = 'Yes' if data_row.iloc[
                                                                      -activity_days_threshold:].count() > 0 else 'No'
    return analysed_data


def analyse_returns_for_intervals(intervals):
    analysed_data = get_list_of_all_schemes()

    logger.debug('Filling interval independent data for all schemes')
    chunks_count = 100
    nav_data_chunks = get_combined_year_wise_data_chunks('cln nav', chunks_count=chunks_count)
    last_available_date = nav_data_chunks[0].columns.values[-1]
    chunks_count = len(nav_data_chunks)
    common_data = pd.DataFrame()
    count = 1
    while len(nav_data_chunks) > 0:
        data_chunk = nav_data_chunks.pop(0)
        logger.warning('Analysing NAV data for chunk {}/{}, Starting Row Index: {}, Ending Row Index: {}',
                       count,
                       chunks_count,
                       data_chunk.index.values[0],
                       data_chunk.index.values[-1])
        common_data = common_data.append(analyse_and_add_common_scheme_data_for_a_chunk(data_chunk))
        count += 1

    analysed_data = analysed_data.join(
        common_data.set_index(['Fund House Name', 'Scheme Name']),
        on=['Fund House Name', 'Scheme Name'])

    logger.debug('Filling interval dependent data for all schemes')
    for interval in get_interval_data_from_strings(intervals):
        logger.info('Gonna analyse {} returns', interval.abbreviation)

        analysed_data_for_current_interval = pd.DataFrame()
        chunks_count = 100
        returns_data_chunks = get_combined_year_wise_data_chunks(interval.file_type,
                                                                 chunks_count=chunks_count)
        chunks_count = len(returns_data_chunks)
        count = 1
        while len(returns_data_chunks) > 0:
            data_chunk = returns_data_chunks.pop(0)
            logger.warning('Analysing returns for {} - chunk {}/{}, Starting Row Index: {}, Ending Row Index: {}',
                           interval.abbreviation, count,
                           chunks_count,
                           data_chunk.index.values[0],
                           data_chunk.index.values[-1])
            analysed_data_for_current_interval = analysed_data_for_current_interval.append(
                analyse_returns_for_an_interval(data_chunk, interval))
            count += 1

        analysed_data = analysed_data.join(
            analysed_data_for_current_interval.set_index(['Fund House Name', 'Scheme Name']),
            on=['Fund House Name', 'Scheme Name'])
        logger.info('{} returns were analysed', interval.abbreviation)

    save_analysed_data(analysed_data, last_date_analysed=last_available_date)
    return last_available_date


if __name__ == '__main__':
    analyse_returns_for_intervals(intervals=['1Y', '2Y', '5Y', '6M', '3M', '30D'])
