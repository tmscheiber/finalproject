from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd

from os import mkdir
from os.path import isdir
import csv

import dir_mgmt_utils as dmu
import operation as bs_ops
import asset

logging.basicConfig(format='%(asctime)s %(module)s:%(lineno)d %(message)s', level=logging.INFO)

days_to_model = 940
first_day = datetime(2016, 1, 1)

date_list = [first_day + timedelta(days=x) for x in range(0, days_to_model)]
supported_operations = bs_ops.supported_operations.keys()
op_aggregated_dfs = bs_ops.make_aggregate_dfs()

markets = ['USD/CNY', 'USD/BTS', 'CNY/BTS']
asset_ids, market_asset_id_pairs = asset.distinct_asset_ids(markets)
logging.info('Distinct asset IDs       : {}'.format(asset_ids))
logging.info('Distinct market asset IDs: {}'.format(market_asset_id_pairs))

agg_cols_for_df = []
agg_cols_by_operation = {}
for op_key, op_class in bs_ops.supported_operations.items():
    agg_cols = op_class.get_agg_columns(asset_ids, market_asset_id_pairs)
    agg_cols_for_df += agg_cols
    agg_cols_by_operation[op_key] = agg_cols
logging.info('Processing columns: {}'.format(agg_cols_for_df))

# create the day, hour and minute dataframes
# agg_day_df = pd.DataFrame(index=pd.date_range(first_day, periods=days_to_model, freq=pd.DateOffset(days=1)), columns=agg_cols_for_df)
# agg_day_df.index.name = 'date'
# agg_hour_df = pd.DataFrame(index=pd.date_range(first_day, periods=days_to_model*24, freq=pd.DateOffset(hours=1)), columns=agg_cols_for_df)
# agg_hour_df.index.name = 'date'
# agg_minute_df = pd.DataFrame(index=pd.date_range(first_day, periods=days_to_model*24*60, freq=pd.DateOffset(minutes=1)), columns=agg_cols_for_df)
# agg_minute_df.index.name = 'date'
# agg_day_ar = [None]*days_to_model
# agg_hour_ar = [None]*(days_to_model*24)
# agg_min_ar = [None]*(days_to_model*24*60)

# write the data to csv files
daily_file_path = dmu.create_aggregate_path(
    dmu.Granularity.DAILY, markets) + '.testit.csv'
hourly_file_path = dmu.create_aggregate_path(
    dmu.Granularity.HOURLY, markets) + '.testit.csv'
minute_file_path = dmu.create_aggregate_path(
    dmu.Granularity.MINUTE, markets) + '.testit.csv'
logging.info('Creating aggregate files \n    {}\n    {}\n    {}'.format(
    daily_file_path, hourly_file_path, minute_file_path))

# open files to create
daily_file = open(daily_file_path, 'wt')
hourly_file = open(hourly_file_path, 'wt')
minute_file = open(minute_file_path, 'wt')
daily_writer = csv.writer(daily_file)
hourly_writer = csv.writer(hourly_file)
minute_writer = csv.writer(minute_file)

# write the headers
daily_writer.writerow(['date']+agg_cols_for_df)
hourly_writer.writerow(['date']+agg_cols_for_df)
minute_writer.writerow([']date']+agg_cols_for_df)

day = 0
for current_date in date_list:
    logging.info('Aggregating operations for {}'.format(current_date))

    this_day_values = []
    day_dir, day_path = dmu.get_daydir_daypath(current_date)

    # if the data directory does not exist, assume we are done
    if not isdir(day_path):
        break

    logging.info('Processing Day {}'.format(current_date))
    sorted_operations_dict = {}
    for op_key, op_class in bs_ops.supported_operations.items():
        file_path = day_path + '/operation-' + '{:02d}'.format(op_key) + '.csv'
        operations = pd.read_csv(file_path)
        operations['block_time'] = operations['block_time'].astype('datetime64[ns]') 
        sorted_operations = operations.sort_values(by=['block_time'])
        sorted_operations_dict[op_key] = sorted_operations
    
    this_day_values = []
    for op_key, op_class in bs_ops.supported_operations.items():
        this_day_values += op_class.get_agg_value_list(
            asset_ids, market_asset_id_pairs, sorted_operations_dict[op_key])

    # Add a new row to the day dataframe
    # agg_day_df.loc[current_date] = this_day_values
    # agg_day_ar[day] = [current_date] + this_day_values
    daily_writer.writerow([current_date] + this_day_values)

    # now lets process the hours
    for hour in range(24):
        this_hour_values = []
        current_hour = datetime(current_date.year, current_date.month, current_date.day, hour)
        # logging.info('Processing Hour {}'.format(current_hour))
        for op_key, op_class in bs_ops.supported_operations.items():
            op_operations = sorted_operations_dict[op_key]
            hourly_operations = op_operations[(op_operations['block_time'] >= current_hour) & (op_operations['block_time'] < current_hour + timedelta(hours=1))]
            this_hour_values += op_class.get_agg_value_list(
                asset_ids, market_asset_id_pairs, hourly_operations)

        # Add a new row to the day dataframe
        # agg_hour_df.loc[current_hour] = this_hour_values
        # agg_hour_ar[day*24 + hour] = [current_hour] + this_hour_values
        hourly_writer.writerow([current_hour] + this_hour_values)

        # and finally process by minutes
        for minute in range(60):
            this_minute_values = []
            current_minute = datetime(current_date.year, current_date.month, current_date.day, hour, minute)
            # logging.info('Processing Minute {}'.format(current_minute))
            for op_key, op_class in bs_ops.supported_operations.items():
                op_operations = sorted_operations_dict[op_key]
                minute_operations = op_operations[(op_operations['block_time'] >= current_minute) & (op_operations['block_time'] < current_minute + timedelta(minutes=1))]
                this_minute_values += op_class.get_agg_value_list(
                    asset_ids, market_asset_id_pairs, minute_operations)

            # Add a new row to the minute dataframe
            # agg_minute_df.loc[current_minute] = this_minute_values
            # agg_min_ar[((day*24 + hour)*60 + minute)] = [current_minute] + this_minute_values
            minute_writer.writerow([current_minute] + this_minute_values)

    # update the day count
    day += 1

daily_file.close()
hourly_file.close()
minute_file.close()

# write the data to csv files
daily_file_path = dmu.create_aggregate_path(dmu.Granularity.DAILY, markets)
hourly_file_path = dmu.create_aggregate_path(dmu.Granularity.HOURLY, markets)
minute_file_path = dmu.create_aggregate_path(dmu.Granularity.MINUTE, markets)

# logging.warning('Writing aggregate data to {}'.format(daily_file_path))
# print(agg_day_df)
# agg_day_df.to_csv(path_or_buf=daily_file_path)
# logging.warning('Writing aggregate data to {}'.format(hourly_file_path))
# agg_hour_df.to_csv(path_or_buf=hourly_file_path)
# logging.warning('Writing aggregate data to {}'.format(minute_file_path))
# agg_minute_df.to_csv(path_or_buf=minute_file_path)
