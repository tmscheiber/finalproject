from os import mkdir
from os.path import isdir
from  datetime import datetime
import logging

rootdir='/Volumes/Samsung_T5/BitShares/ParsedData'

# used to specify which aggregate to work on
class Granularity:
    DAILY='daily'
    HOURLY='hourly'
    MINUTE='minute'

    def __init__(self, granularity):
        self.units_per_day = 1
        self.window_days = 90
        self.min_lag_days = 7
        self.granularity = Granularity.DAILY
        self.freq = 'D'
        if granularity is Granularity.HOURLY:
            self.units_per_day = 24
            self.window_days = 30
            self.min_lag_days = 7
            self.granularity = Granularity.HOURLY
            self.freq = 'H'
        elif granularity is Granularity.MINUTE:
            self.units_per_day = 24*60
            self.window_days = 7
            self.min_lag_days = 7
            self.granularity = Granularity.MINUTE
            self.freq = 'M'

# get names for directory and full path for a given day's data
def get_daydir_daypath(current_day):
    day_dir = current_day.strftime('%Y%m%d')
    # logging.info(day_dir)
    day_path = rootdir + '/' + day_dir
    return day_dir, day_path

def create_aggregate_path(aggregate_type, markets):
    agg_dir = rootdir + '/aggregates'
    if not isdir(agg_dir):
        mkdir(agg_dir)
    agg_file_path = agg_dir + '/' + aggregate_type + '.'
    for market in markets:
        agg_file_path += market.replace('/', '_') + '.'
    agg_file_path +=  'csv'

    return agg_file_path

def get_result_dir():
    return rootdir + '/results/'

def create_result_path(markets):
    result_dir = get_result_dir()
    if not isdir(result_dir):
        mkdir(result_dir)
    result_file_path = result_dir
    for market in markets:
        result_file_path += market.replace('/', '_') + '.'
    result_file_path +=  'csv'
    logging.info('Opening result file {}'.format(result_file_path))

    return result_file_path
