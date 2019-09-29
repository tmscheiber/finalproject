from datetime import datetime
from datetime import date
from datetime import timedelta
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, connections
from elasticsearch.helpers import scan
import pandas as pd
import numpy as np
import json
import bitshares_operations as bso
from os import makedirs
from os import mkdir
from os.path import isdir
from pathlib import Path
import logging
import csv

import dir_mgmt_utils as dmu
import operation as bs_ops
import asset

# function to create the query for each day
# takes a list of supported operation ids and the current day to query for
#
def current_day_query(supported_operation_ids, current_day):
    next_day = current_day + timedelta(days=1)

    aQuery = {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "operation_type": supported_operation_ids
                        }
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "block_data.block_time": {
                                "gte": current_day.strftime(format='%Y%m%d'),
                                "lt": next_day.strftime(format='%Y%m%d'),
                                "format": "yyyMMdd"
                            }
                        }
                    }
                ]
            }
        }
    }
    return aQuery

logging.basicConfig(format='%(asctime)s %(module)s:%(lineno)d %(message)s', level=logging.WARNING)

es = Elasticsearch(
    [
        'http://localhost:9200/'
    ]
)
# s = Search(using=es)

# here is where we will store the parsed data retrieved from Elasticsearch
if not isdir(dmu.rootdir):
    makedirs(dmu.rootdir)
    logging.warning('created rootdir {}'.format(dmu.rootdir))

# this dictionary will store the data paths that we have created or validated
dir_dict = {}

# set a current day that is before the advent of BitShares

current_day = datetime(2015, 1, 1)                          # start absurdly early date to force first loop set up
operation_day = datetime(2016, 1, 1)                       # first operation day
day_dir, day_path = dmu.get_daydir_daypath(current_day)     # path components to storage location
day_operation_cnt = 1                                       # day operation count > 0 to force first loop
total_length = 0                                            # total count is just to keep track of total ops
max_operations = 1000000000                                 # stop when we reach this operations count
log_frequency  = 1000000                                    # used for limiting logging of operation progress
overwrite      = True                                       # indicates whether to overwrite earlier processing
operation_type_query = current_day_query([*bs_ops.supported_operations], operation_day)

# keep going until we break out or we run out of operations
while day_operation_cnt > 0:
    logging.info(day_operation_cnt)
    day_operation_cnt = 0

    # process each day
    skip = False
    for a_json_operation in scan(es,
                                 query=operation_type_query,
                                 size=10000,
                                 index='bitshares-*'):
                                #  index='bitshares-{:4d}-{:02d}'.format(index_year, index_month)):
        # increment the count of operations in current day
        day_operation_cnt += 1
        total_length += 1

        # parse the operation details structures from JSON string that elasticsearch doesn't understand
        if 'operation_type' in a_json_operation['_source'].keys():
            a_json_operation['_source']['operation_history']['op'] = json.loads(
                a_json_operation['_source']['operation_history']['op'])
            a_json_operation['_source']['operation_history']['operation_result'] = json.loads(
                a_json_operation['_source']['operation_history']['operation_result'])
        an_operation = bs_ops.parse_operation(a_json_operation)

        # decide if we need to bump the aggregation to the next day, hour, or minute (only day for now)
        operation_day = datetime(an_operation.block_time.year,
                                 an_operation.block_time.month,
                                 an_operation.block_time.day)

        # update current day and create day directory if necessary and initialize daily tracking
        if operation_day != current_day:
            day_dir, day_path = dmu.get_daydir_daypath(operation_day)
            if not day_dir in dir_dict:
                if not isdir(day_path):
                    mkdir(day_path)
                    logging.warning('created day dir {}'.format(day_path))
                logging.warning('set day key {}'.format(day_dir))
                dir_dict[day_dir] = {}
            current_day = operation_day

            # if we don't want to re-process existing results, check to see if there are existing results
            if not overwrite:
                skip = True
                for op_key in bs_ops.supported_operations.keys():
                    file_path = Path(day_path + '/operation-' + '{:02d}'.format(op_key) + '.csv')
                    if not file_path.is_file():
                        skip = False
                if skip:
                    break

            # create the dataframes for each of the supported operations
            op_lists = bs_ops.make_empty_lists()
            logging.debug("Processed {} operations".format(op_lists))

        # print a ticker of operations processed
        if total_length % log_frequency == 0:
            logging.warning("Processed {} operations".format(total_length))

        # append the processed operation to the dataframe aligned to that operation
        bs_ops.append_record(an_operation, op_lists)

    # write out the dataframes for each supported operation type
    if not skip:
        for op_key, op_list in op_lists.items():
            file_path = day_path + '/operation-' + '{:02d}'.format(op_key) + '.csv'
            logging.warning(file_path)
            np.savetxt(file_path, op_list, delimiter=",", fmt='%s')
    # break

    # exit early if we exceed the max count of records to be processed
    if total_length >= max_operations:
        break

    # set up the query for the next day
    operation_type_query = current_day_query([*bs_ops.supported_operations], current_day + timedelta(days=1))
