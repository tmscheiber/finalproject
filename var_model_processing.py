import pandas as pd
from math import floor, ceil, isnan, exp
from statsmodels.tsa.api import VAR, DynamicVAR, VARMAX
from statsmodels.tools.eval_measures import mse, meanabs
from statsmodels.tsa.stattools import adfuller
from scipy import stats

from matplotlib import pyplot

import logging
import csv

import dir_mgmt_utils as dmu

forecast_type = {
    None: 'No Forecast',
    1:    'Naive Forecast',
    2:    'VAR Forecast'
}

order_type = {
    '000': 'Original Values',
    '100': 'First Order Difference',
    '010': 'First Order % Change',
    '101': 'First order + BoxCox'

}

logging.basicConfig(
    format='%(asctime)s %(module)s:%(lineno)d %(message)s', level=logging.INFO)


def first_order_difference(endog):
    '''
    return the first order difference (xi - x(i-1)) for all columns in endog
    '''
    logging.info('First order differencing')
    return endog.diff().iloc[1:]


def inverse_first_order_difference_forecast(endog, forecast):
    '''
    Undo first order difference for forecast values so we can compare original measures to forecast
    '''
    forecast_inverse = pd.DataFrame(index=forecast.index)
    for column in forecast.columns:
        forecast_inverse[column] = forecast[column] + \
            endog[column][:(endog[column].size-1)]
    return forecast_inverse


def first_order_or_pct_change(target_columns, endog):
    logging.info(
        'First order percent change for rates. Target columns, amounts and counts first order difference')
    endog_diff = pd.DataFrame(index=endog.index[1:])
    for column in endog.columns:
        if 'rate' in column:
            endog_diff[column] = endog[column].pct_change(
                fill_method='ffill').iloc[1:].interpolate(limit_direction='both')
        # if column in target_columns:
        #     endog_diff[column] = endog[column].diff(
        #     ).iloc[1:].interpolate(limit_direction='both')
        else:
            endog_diff[column] = endog[column].diff().iloc[1:].fillna(0)
    return endog_diff


def box_cox_transform(endog):
    logging.info(
        'BoxCox transform for everything, return lambdas and shifts for restoring original data')
    endog_boxcox = pd.DataFrame(index=endog.index)
    lambdas = dict()
    shifts = dict()
    for column in endog.columns:
        # logging.info('Transforming column {} from boxcox space'.format(column))
        shift = 0
        if endog[column].min() <= 0.0:
            shift = -endog[column].min()*1.01 + 1
        endog_boxcox[column] = endog[column] + shift
        lambdas[column] = stats.boxcox_normmax(endog_boxcox[column])
        endog_boxcox[column] = stats.boxcox(
            endog_boxcox[column], lambdas[column])
        shifts[column] = shift
        # logging.info('Last value for column {} : {}'.format(column, endog_boxcox[column][-1:]))
    return endog_boxcox, lambdas, shifts


def inverse_box_cox_transform(endog_boxcox, lambdas, shifts):
    logging.info('restoring BoxCox transformed data to normal data space')
    endog = pd.DataFrame(index=endog_boxcox.index)
    for column in endog_boxcox.columns:
        # logging.info(
        #     'Restoring column {} from boxcox to normal space'.format(column))
        fitted_lambda = lambdas[column]
        endog[column] = exp(endog_boxcox[column]) if fitted_lambda == 0 else (
            fitted_lambda*endog_boxcox[column] + 1) ** (1/fitted_lambda)
        endog[column] = endog[column] - shifts[column]
    endog = endog.fillna(0)
    return endog

def transform_data(target_columns, endog, order='000'):
    # get the correct order for the data
    endog_transformed = endog
    lambdas = None
    shifts = None
    if order[0] == '1':
        logging.info(
            'Transforming using first order difference for {}'.format(target_columns))
        endog_transformed = first_order_difference(endog_transformed)
    if order[1] == '1':
        logging.info(
            'Transforming using first order pct difference for {}'.format(target_columns))
        endog_transformed = first_order_or_pct_change(
            target_columns, endog_transformed)
    if order[2] == '1':
        logging.info('Transforming using BoxCox for {}'.format(target_columns))
        endog_transformed, lambdas, shifts = box_cox_transform(
            endog_transformed)

    return endog_transformed, lambdas, shifts

def endog_untransform(transformed_data, order, lambdas, shifts):
    untransformed_data = transformed_data
    if order[2] == '1':
        logging.info('Restoring from boxcox to normal')
        untransformed_data = inverse_box_cox_transform(transformed_data, lambdas, shifts)
    return untransformed_data

def forecast_with_charts(target_columns, endog, figure_number=1, case='full', order='000', forecast=None, freq='D', max_lag=30, window=30):
    # one row of charts, one chart for each target column being reported on
    max_plot_rows = 1
    max_plot_cols = len(target_columns)
    # set up and title collections of charts
    fig, ax = pyplot.subplots(
        max_plot_rows, max_plot_cols, num=figure_number, squeeze=False, figsize=(12, 6))
    fig.suptitle('Forecast for {} with {}'.format(
        order_type.get(order), forecast_type.get(forecast)), fontsize=16)
    logging.info('Chart figure title {}'.format(fig.suptitle))

    # get the correct order for the data
    endog_transformed, lambdas, shifts = transform_data(target_columns, endog, order)

    # null count after transformation may help explain modeling or reporting challenges
    logging.info('Null count: {}'.format(
        endog_transformed.isnull().sum().sum()))

    # get the forecast
    forecast_data = None
    if forecast == None:
        valid_data = dict.fromkeys(target_columns)
        for column in target_columns:
            valid_data[column] = endog_transformed[column]
    elif forecast == 1:
        valid_data, forecast_data = naive_forecast(
            target_columns, endog_transformed)
    elif forecast == 2:
        valid_data, forecast_data = VAR_forecast(
            target_columns=target_columns, endog=endog_transformed, freq=freq, max_lag=max_lag, window=window)

    # if we did a boxcox transformation, restore valid and forecast data to normal space
    valid_data = endog_untransform(valid_data, order, lambdas, shifts)
    forecast_data = endog_untransform(forecast_data, order, lambdas, shifts)

    # graph and save data for each column being reported on
    col = 0
    for column in target_columns:
        # chart for each column
        ax1 = ax[0][col]
        ax1.set_title(column)
        ax1.tick_params(axis='x', rotation=60)
        ax1.plot(valid_data[column].index, valid_data[column], linewidth=0.5)

        if None != forecast:
            # for IDX in range(len(valid_data[column].values)):
            #     print('{} {}  {}'.format(
            #         valid_data[column].index.values[IDX], valid_data[column].iloc[IDX], forecast_data[column].iloc[IDX]))

            ax1.plot(valid_data[column].index,
                     forecast_data[column], linewidth=0.5)

            # log the MSE and MAE
            logging.info('Mean square error for {} forecast for data order {} for column {} : {}'.format(
                forecast_type[forecast],
                order_type[order],
                column,
                mse(valid_data[column], forecast_data[column])
            )
            )
            logging.info('Absolute square error for {} forecast for data order {} for column {} : {}'.format(
                forecast_type[forecast],
                order_type[order],
                column,
                meanabs(valid_data[column], forecast_data[column])
            )
            )
            forecast_cnt = len(valid_data.index.values)
            correct_forecast_direction = 0
            for data_idx in range(forecast_cnt):
                if (valid_data[column][data_idx] > 0) == (forecast_data[column][data_idx] > 0):
                    correct_forecast_direction = correct_forecast_direction + 1
            logging.info('Percent of valid data that is incerease from previous day {}'.format(
                valid_data[column].gt(0).sum()/forecast_cnt))
            logging.info('Percent of forecast that matches valid direction {}'.format(
                correct_forecast_direction/forecast_cnt))

            # write out the results to csv files for post processing
            output_file = open(dmu.create_result_path(
                [column + '.' + order_type[order].replace(' ', '_') + '.' + forecast_type[forecast].replace(' ', '_') + '.' + freq]), 'w')
            output = csv.writer(output_file, delimiter=',')
            row = ['date', 'valid', 'forecast', 'increase', 'same_direction']
            output.writerow(row)  # header
            for row_idx in range(valid_data.index.size):
                row = ['{:%Y-%m-%d}'.format(valid_data.index[row_idx]),
                       valid_data[column][row_idx],
                       forecast_data[column][row_idx],
                       valid_data[column][row_idx] > 0,
                       (valid_data[column][row_idx] > 0) == (forecast_data[column][row_idx] > 0)]
                output.writerow(row)
            output_file.close()
        col = col + 1

    # finish by closing plots. Learned hardway not doing this can cause memory problems
    pyplot.show()
    pyplot.close()


def naive_forecast(target_columns, endog):
    '''
    Function to create a naive (new value same as previous value) for the target columns in a dataframe.

    target_columns: an array of columns names to forecast
    endog: datafrom with the original data to use in forecast
    '''
    logging.info('Creating naive forecast for {} columns'.format(
        len(target_columns)))
    valid = pd.DataFrame(index=endog.index[1:])
    forecast = pd.DataFrame(index=endog.index[1:])
    for column in target_columns:
        # logging.info('Original Values\n {}'.format(endog[column]))
        endog_column = endog[column]
        valid[column] = endog_column[1:].values
        # logging.info('Valid Values\n {}'.format(valid[column]))
        forecast[column] = endog_column[0:valid[column].size].values
        # logging.info('Naive Forecast\n {}'.format(forecast[column]))
    return valid, forecast


def VAR_forecast(target_columns, endog, freq='D', max_lag=7, window=30):
    '''
    Function to create a vector autoregression model for the target columns in a dataframe
        Before creating forecast, it tests the stationarity of each column, only retaing columns that
        have consistent means and varition over the window being used in the forecast

    target_columns: an array of columns names to forecast
    endog: datafrom with the original data to use in forecast
    freq: frequency of the time series data ('D', 'H', 'M'). Defaults to 'D'
    max_lag: Number of data elements to calculate lags for in model. Defaults to 7
    window: Amount of history to use in forecast. Defaults to 30
    '''
    valid = pd.DataFrame(index=endog.index[(window+1):])
    forecast = pd.DataFrame(index=endog.index[(window+1):])

    for column in target_columns:
        valid[column] = endog[column].iloc[(window+1):]
        forecast[column] = valid[column].copy()

    time_range = range(len(endog.index.values) - window - 1)
    logging.info('Total Days: {}'.format(endog.index.size))
    logging.info('Window: {}'.format(window))
    logging.info('Day Range: {}'.format(time_range))

    for current_time_index in time_range:
        endog_window = endog[current_time_index:current_time_index+window]

        # check for stationarity, only include those columns that are stationary, P <= 5%
        columns_to_drop = []
        for column in endog_window.columns:
            if column not in target_columns:
                try:
                    if endog_window[column].isnull().sum() > 0:
                        columns_to_drop.append(column)
                    else:
                        result = adfuller(endog_window[column])
                        if result[0] > result[4]['5%']:
                            columns_to_drop.append(column)
                except:
                    columns_to_drop.append(column)

        if len(columns_to_drop) > 0:
            # logging.info('Dropping non-stationary columns {}'.format(columns_to_drop))
            endog_window = endog_window.drop(columns_to_drop, axis=1)

        if current_time_index % 100 == 0:
            log_str = 'Current day index: {} of {} -- null count {} -- column count {}'
            logging.info(log_str.format(
                current_time_index, time_range, endog_window.isnull().sum().sum(), len(endog_window.columns)))

        # VAR
        model = VAR(endog_window,
                    freq=freq
                    )
        # model.select_order(7)
        results = model.fit(
            maxlags=max_lag,
            # ic='aic',
            # ic='fpe',
            # ic='hqic',
            # ic='bic',
            # disp=False,
            # trend='c',
            # trend='ct',
            # trend='ctt',
            trend='nc',
        )

        # generate a ones step forecast and save the forecasted values for the target columns
        aForecast = results.forecast(
            endog_window.values,
            # trend_coefs=None,
            steps=1,
        )
        for column in target_columns:
            if isnan(aForecast[0][endog_window.columns.get_loc(column)]):
                forecast[column].iloc[current_time_index] = valid[column].iloc[current_time_index-1]
            else:
                forecast[column].iloc[current_time_index] = aForecast[0][endog_window.columns.get_loc(
                    column)].copy()

    return valid, forecast
