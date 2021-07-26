import sys

import pandas as pd
from icecream import ic
from loguru import logger

import file_locations
from Interval import Interval
from data_frame_utils import convert_data_frame_to_list_of_chunks_row_wise, check_for_schemes_mismatch_in_data_frames
from general_utils import compute_annualised_percentage_change, calculate_percentage, \
    get_interval_data_from_strings
from mf_data_provider import get_combined_year_wise_data_chunks, get_list_of_all_schemes
from mf_data_saver import split_and_save_combined_data

pd.set_option("display.max_rows", None, "display.max_columns", None)
log_filename = file_locations.PROCESSED_DATA_FOLDER_PATH + 'returns_generator_logs.log'
logger_message_format = "{time} - {name} - {level} - {message}"
logger.add(log_filename, level="INFO", format=logger_message_format)
logger.add(log_filename, level="DEBUG", format=logger_message_format)
logger.add(log_filename, level="WARNING", format=logger_message_format)
logger.add(log_filename, level="ERROR", format=logger_message_format)


@logger.catch
def update_returns_for_a_interval(interval: Interval, nav_data: pd.DataFrame, existing_returns_data: pd.DataFrame,
                                  absolute_start_date_obj,
                                  check_for_schemes_mismatch=True):
    if check_for_schemes_mismatch:
        if check_for_schemes_mismatch_in_data_frames(nav_data, existing_returns_data):
            logger.critical('Schemes were not matching in the chunks')
            sys.exit(1)
    padded_data = nav_data.fillna(method='bfill', axis=1, limit=interval.max_dates_padded)

    dates = nav_data.columns.values[2:]

    date_format = '%d-%m-%Y'
    returns_updated = 0
    try:
        for date in dates:
            previous_date_obj = pd.to_datetime(date, format=date_format) - interval.offset
            if absolute_start_date_obj > previous_date_obj:
                continue
            previous_date = previous_date_obj.strftime(date_format)
            for index in existing_returns_data.index.values:
                if not (pd.isna(nav_data.loc[index, date]) or pd.isna(padded_data.loc[index, previous_date])):
                    try:
                        if not pd.isna(existing_returns_data.loc[index, date]):
                            continue
                    except KeyError:
                        pass
                    current_nav_value = nav_data.loc[index, date]
                    previous_nav_value = padded_data.loc[index, previous_date]
                    absolute_percentage_change = calculate_percentage(current_nav_value,
                                                                      previous_nav_value,
                                                                      digits_after_decimal=2,
                                                                      calculate_change=True)
                    annualised_percentage_change = compute_annualised_percentage_change(interval.unit,
                                                                                        interval.value,
                                                                                        absolute_percentage_change,
                                                                                        digits_after_decimal=2)
                    existing_returns_data.loc[index, date] = annualised_percentage_change
                    returns_updated += 1

        logger.debug('Returns for {} were processed', interval.abbreviation)
    except Exception as e:
        logger.error('Faced error while generating returns for {}\nError: {} - {}', interval.abbreviation, type(e), e)
        sys.exit(1)
    return returns_updated


@logger.catch
def update_returns_for_intervals(intervals, update_latest_returns_only=False):
    chunks_count = 100
    for interval in get_interval_data_from_strings(intervals):
        returns_data = pd.DataFrame()

        nav_data_chunks = get_combined_year_wise_data_chunks('cln nav', chunks_count=chunks_count)
        existing_returns_data_chunks = get_combined_year_wise_data_chunks(interval.file_type,
                                                                          chunks_count=chunks_count)

        if len(existing_returns_data_chunks) < chunks_count:
            existing_returns_data_chunks = convert_data_frame_to_list_of_chunks_row_wise(get_list_of_all_schemes(),
                                                                                         chunks_count=chunks_count)

        # ensuring all schemes are same in the chunks of both data frames
        if len(nav_data_chunks) == len(existing_returns_data_chunks):
            chunks_count = len(nav_data_chunks)
        else:
            logger.critical('Chunk counts were different')
            sys.exit(1)
        for i in range(chunks_count):
            if check_for_schemes_mismatch_in_data_frames(nav_data_chunks[i], existing_returns_data_chunks[i]):
                logger.critical('{}th chunk had a mismatch', i + 1)
                sys.exit(1)
        logger.success('There is no mismatch in any chunk')

        date_format = '%d-%m-%Y'
        if update_latest_returns_only and len(existing_returns_data_chunks[0].columns.values) > 2:
            start_date = pd.to_datetime(existing_returns_data_chunks[0].columns.values[-1],
                                        format=date_format) - interval.offset  # - pd.offsets.DateOffset(days=2)
        else:
            start_date = pd.to_datetime(nav_data_chunks[0].columns.values[2], format=date_format)
        ic(start_date)

        count = 1
        while len(nav_data_chunks) > 0:
            nav_data_chunk = nav_data_chunks.pop(0)
            existing_returns_data_chunk = existing_returns_data_chunks.pop(0)
            logger.warning('Generating returns for chunk {}/{}, Starting Row Index: {}, Ending Row Index: {}', count,
                           chunks_count,
                           nav_data_chunk.index.values[0],
                           nav_data_chunk.index.values[-1])
            count += 1
            returns_updated = update_returns_for_a_interval(interval, nav_data=nav_data_chunk,
                                                            existing_returns_data=existing_returns_data_chunk,
                                                            absolute_start_date_obj=start_date,
                                                            check_for_schemes_mismatch=False)
            returns_data = returns_data.append(existing_returns_data_chunk)
            logger.info('{} returns for {} were updated in this chunk', returns_updated, interval.abbreviation)
            # if returns_updated > 0 and count % 50 == 0:
            #     split_and_save_combined_data(returns_data, interval.file_type)
        split_and_save_combined_data(returns_data, interval.file_type)


if __name__ == '__main__':
    # update_returns_for_intervals(intervals = ['30D', '3M', '6M'])
    update_returns_for_intervals(intervals=['1Y', '2Y', '6M', '3M', '30D'])
    # update_returns_for_intervals(intervals=['5Y'])
