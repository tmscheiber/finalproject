import numpy as np
import pandas as pd
from datetime import datetime
import math
import statsmodels.api as sm
from statsmodels.tsa.api import VAR, DynamicVAR
# from statsmodels.tsa.stattools import grangercausality

import logging

import dir_mgmt_utils as dmu
import grainger_causality as gc
import var_model_processing as vmp
import bitshares_data_plotting as bdp
import asset

date_ranges = [
    [datetime(2016, 1, 1), datetime(2017, 2, 28)],
    [datetime(2017, 3, 1), datetime(2017, 12, 31)],
    [datetime(2018, 1, 1), datetime(2018, 7, 27)]]
logging.basicConfig(
    format='%(asctime)s %(module)s:%(lineno)d %(message)s', level=logging.INFO)

markets = ['USD/CNY', 'USD/BTS', 'CNY/BTS']

granularity = dmu.Granularity(dmu.Granularity.DAILY)
file_path = dmu.create_aggregate_path(granularity.granularity, markets)

agg_ops = pd.read_csv(file_path)
print(file_path)
print(agg_ops.tail())
agg_ops['date'] = agg_ops['date'].astype('datetime64[ns]')
agg_ops = agg_ops.set_index('date')

# missing value treatment
logging.info('Handling N/A in data file {}'.format(file_path))
columns = agg_ops.columns
for column in columns:
    # for the rates, interpolate across the N/As
    if 'rate' in column:
        agg_ops[column] = agg_ops[column].interpolate(limit_direction='both')
        if asset.is_fill_order_last10pct(column):
            logging.info('Interpolating columns {}'.format(column))
        elif agg_ops[column].isnull().any().any():
            # remove rate columns with nulls because they cause problems later
            agg_ops.drop(column, axis=1, inplace=True)

    # for everything else, missing data can be set to 0 (zero)
    else:
        agg_ops[column] = agg_ops[column].fillna(0)
columns = agg_ops.columns

# what do we want to predict? Let's predict large moves
# as a hand wave, lets look at standard devitaion in $ or Y per BTS standard deviation over 30 days
logging.info(
    'Getting standard deviation of median rates for {}'.format(file_path))
window = granularity.window_days*granularity.units_per_day
min_lag = granularity.min_lag_days*granularity.units_per_day
endog_binary = pd.DataFrame(index=agg_ops.index)
endog_continuous = pd.DataFrame(index=agg_ops.index)
target_binary_columns = []
target_continuous_columns = []

for column in columns:
    # let's find the /BTS median rates for transactions

    if asset.is_fill_order_last10pct(column):
        logging.info(
            'Getting last {} days standard deviations for {}'.format(window/granularity.units_per_day, column))

        stddevs = [0]*agg_ops.index.size
        is_2x_stddev = [False]*agg_ops.index.size
        last10pct_median_rates = agg_ops[column].values

        for row in range(min_lag, agg_ops.index.size):
            agg_ops_lag = agg_ops[column][row-min_lag:row-1]
            stddevs[row] = agg_ops_lag.std()
            is_2x_stddev[row] = ((abs(last10pct_median_rates[row] -
                                      last10pct_median_rates[row-min_lag])) >= 2*stddevs[row])
        binary_column = column+'_large_swing'
        target_continuous_columns.append(column)
        target_binary_columns.append(binary_column)
        endog_continuous[column] = agg_ops[column]
        endog_binary[binary_column] = np.array(is_2x_stddev)
        agg_ops.drop(column, axis=1, inplace=True)
    else:
        endog_continuous[column] = agg_ops[column]
        endog_binary[column] = agg_ops[column]

figure_number = 1
# bdp.create_exploratory_plots(agg_ops, endog_continuous, figure_number)
bdp.create_exploratory_plots(target_continuous_columns,
                             endog_continuous.diff().iloc[1:], window, min_lag, figure_number)

# run grainger causality analysis
# reduced_exog = gc.run_grainger_analysis(target_binary_columns, endog_binary, '000', min_lag*granularity.units_per_day, markets)
# reduced_exog = gc.run_grainger_analysis(target_continuous_columns, endog_continuous, '100', min_lag*granularity.units_per_day, markets)

# # get the total series naive forecast
# vmp.forecast_with_charts(target_continuous_columns,
#                          endog_continuous,
#                          figure_number=figure_number,
#                          case='full',
#                          order='000',
#                          forecast=1,
#                          freq=granularity.freq,
#                          max_lag=7,
#                          window=45)
# vmp.forecast_with_charts(target_continuous_columns,
#                          endog_continuous,
#                          figure_number=figure_number,
#                          case='full',
#                          order='000',
#                          forecast=2,
#                          freq=granularity.freq,
#                          max_lag=7,
#                          window=45)
# vmp.forecast_with_charts(target_continuous_columns,
#                          endog_continuous,
#                          figure_number=figure_number,
#                          case='full',
#                          order='100',
#                          forecast=1,
#                          freq='D',
#                          max_lag=7,
#                          window=45)
# vmp.forecast_with_charts(target_continuous_columns,
#                          endog_continuous,
#                          figure_number=figure_number,
#                          case='full',
#                          order='100',
#                          forecast=2,
#                          freq=granularity.freq,
#                          max_lag=7,
#                          window=45)
# vmp.forecast_with_charts(target_continuous_columns,
#                          endog_continuous,
#                          figure_number=figure_number,
#                          case='full',
#                          order='010',
#                          forecast=1,
#                          freq=granularity.freq,
#                          max_lag=7,
#                          window=45)
# vmp.forecast_with_charts(target_continuous_columns,
#                          endog_continuous,
#                          figure_number=figure_number,
#                          case='full',
#                          order='010',
#                          forecast=2,
#                          freq=granularity.freq,
#                          max_lag=7,
#                          window=45)
vmp.forecast_with_charts(target_continuous_columns,
                         endog_continuous,
                         figure_number=figure_number,
                         case='full',
                         order='101',
                         forecast=0,
                         freq=granularity.freq,
                         max_lag=7,
                         window=45)
# vmp.forecast_with_charts(target_continuous_columns,
#                          endog_continuous,
#                          figure_number=figure_number,
#                          case='full',
#                          order='101',
#                          forecast=1,
#                          freq=granularity.freq,
#                          max_lag=7,
#                          window=45)
# vmp.forecast_with_charts(target_continuous_columns,
#                          endog_continuous,
#                          figure_number=figure_number,
#                          case='full',
#                          order='101',
#                          forecast=2,
#                          freq=granularity.freq,
#                          max_lag=7,
#                          window=45)
