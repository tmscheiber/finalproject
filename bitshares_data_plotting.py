import pandas as pd
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.tsa.stattools import acf
from matplotlib import pyplot
from math import ceil
import logging
from datetime import datetime

logging.basicConfig(
    format='%(asctime)s %(module)s:%(lineno)d %(message)s', level=logging.INFO)

def create_exploratory_plots(target_cols, endogs, window, lags, figure_number):

    max_plot_rows = 2
    max_plot_cols = len(target_cols)
    fig, ax = pyplot.subplots(max_plot_rows, max_plot_cols, num=figure_number, squeeze=False)
    fig.label = 'Last Day Close Density'
    col = 0
    for column in target_cols:
        endog = endogs[column]
        
        ax1 = ax[0][col]
        ax2 = ax[1][col]
        ax1.set_title(column)
        ax1.hist(endog.values, bins=49)
        endog.plot(kind='kde', ax=ax2)
        col += 1
    pyplot.show()
    pyplot.close()

    max_plot_rows = 10
    figure_number = 1
    fig, ax = pyplot.subplots(max_plot_rows, 2, num=figure_number, squeeze=False)
    fig.label = 'ACF and PACF autocorrelation for exagenous variables'
    row = 0
    for column in endogs.columns:
        # if 'LimitOrderCreate_1.3.113/1.3.121_medianrate' == column:
        logging.info('creating acf for {}'.format(column))

        ax1 = ax[row][0]
        ax1.set_title('ACF: {}'.format(column))
        try:
            logging.info(acf(endogs[column][0:window],
                     nlags=lags,
                     alpha=0.05,
                     ))
        except:
            logging.warning('Could not create ACF for column {}'.format(column))
        try:
            plot_acf(endogs[column][0:90],
                     ax=ax1,
                    #  lags=lags,
                     title='ACF: {}'.format(column))
        except:
            logging.warn('Could not create ACF plot for column {}'.format(column))
        
        ax2 = ax[row][1]
        ax2.set_title('PACF: {}'.format(column))
        try:
            plot_pacf(endogs[column][0:window],
                      ax=ax2,
                    #   lags=lags,
                      title='PACF: {}'.format(column))
        except:
            logging.warn('Could not create PACF plot for column {}'.format(column))

        row += 1
        if row == max_plot_rows:
            row = 0
            pyplot.show()
            pyplot.close()
            figure_number += 1
            fig, ax = pyplot.subplots(max_plot_rows, 2, num=figure_number, squeeze=False)
            fig.label = 'ACF and PACF autocorrelation for exagenous variables'
            break
    pyplot.show()
    pyplot.close()

