#
# Heavily based on Piglet Blog
# https://blog.projectpiglet.com/2018/01/causality-in-cryptomarkets/
#
import csv
import random
import logging

from collections import OrderedDict

import numpy as np
import pandas as pd
import scipy.stats as ss
from statsmodels.tsa import stattools

import matplotlib.pyplot as plt

import dir_mgmt_utils as dmu
import var_model_processing as vmp


def generate_random_walk_vector(length):
    """
    Generate a random walk vector of a particular length
    """
    data = [0]
    for j in range(length-1):
        step_x = random.randint(0, 1)
        val = 0.0
        if step_x == 1:
            val = data[j] + 0.3 + 0.05*np.random.normal()
            if val > 1.0:
                val = 1.0
            else:
                val = data[j] - 0.3 + 0.05*np.random.normal()
                if val < -1.0:
                    val = -1.0
        data.append(val)
    return np.array(data)


def create_random_test_vector(assessment_file):
    """
    Create a new combined vector for ingestion into
    the granger causality function
    Using a random walk for closing, as opposed to real data
    """

    comb_df = pd.read_csv(assessment_file)

    # Seperate out vectors and create random walks
    days = []
    for i in range(len(comb_df.values)):
        days.append(i)
    trend = generate_random_walk_vector(len(comb_df['trend'].values))
    close = generate_random_walk_vector(len(comb_df['trend'].values))

    # Resize and normalize
    days = days[1:]
    trend = ss.zscore(trend[1:])
    close = ss.zscore(np.diff(close))

    return (trend, close, days)


def create_combined_random_vector(assessment_file):
    """
    Create a new combined vector for ingestion into
    the granger causality function
    Using a random walk for closing, as opposed to real data
    """

    comb_df = pd.read_csv(assessment_file)

    # Seperate out vectors and create random walk(s)
    days = []
    for i in range(len(comb_df.values)):
        days.append(i)
    trend = comb_df['trend'].values
    close = generate_random_walk_vector(len(trend))

    # Resize and normalize
    days = days[1:]
    trend = ss.zscore(trend[1:])
    close = ss.zscore(np.diff(close))

    return (trend, close, days)


def show_comparison_graph(d1, v1, v2,
                          v1_label='v1_line',
                          v2_label='v2_line',
                          title=""):
    """
    Plot two vectors for comparison against one another
    This will pause the application until you close the plot
    """
    plt.plot(d1, v1, 'b-', label=v1_label)
    plt.plot(d1, v2/v2.max(), 'r-', label=v2_label)
    plt.title(title)
    plt.legend(loc='upper left')
    plt.show()


def run_grainger_analysis(target_columns, endog, order, maxlag, markets):
    fmt_output = []

    no_causality = []
    yes_causality = []

    strong_p_value = 0.05
    super_strong_p_value = 0.001

    total_lag = []  # p-value less than strong_p_value
    best_lag = []  # p-value less than super_strong_p_value

    # get the correct order for the data
    endog, lambdas, shifts = vmp.transform_data(target_columns, endog, order)

    days = endog.index.values
    reduced_endogs = pd.DataFrame(index=days)

    logging.info('Granger Causality for {} endogenous variables with max lag {}'.format(
        len(endog.columns), maxlag))
    for target_column in target_columns:
        for endog_column in endog.columns:
            # ignore the case where we both represent the same column
            if target_column == endog_column:
                break

            # transform to vectors
            endog_v = endog[endog_column].values
            target_v = endog[target_column].values
            combined_vector = [target_v, endog_v]

            # Test whether the time series in the second column Granger causes
            # the time series in the first column

            # The Null hypothesis for grangercausalitytests is that the time series in the
            # second column, x2, does NOT Granger cause the time series in the first column,
            # x1. Grange causality means that past values of x2 have a statistically significant
            # effect on the current value of x1, taking past values of x1 into account as
            # regressors. We reject the null hypothesis that x2 does not Granger cause x1
            # if the pvalues are below a desired size of the test.
            gc = []
            try:
                gc = stattools.grangercausalitytests(endog[[target_column, endog_column]],
                                                     maxlag,
                                                     addconst=True,
                                                     verbose=False)
            except Exception as e:
                logging.error(e)
                # logging.error(endog_v)
                continue

            # lag_pvalue = {}
            lag_numbers = []
            current_best_lag_numbers = []
            for lag in gc:

                # fp = (lag, gc[lag][0]['ssr_ftest'][0], gc[lag][0]['ssr_ftest'][1])

                if gc[lag][0]['ssr_ftest'][1] < strong_p_value and lag > 1:
                    lag_numbers.append(lag)
                    total_lag.append(lag)
                    if gc[lag][0]['ssr_ftest'][1] < super_strong_p_value:
                        current_best_lag_numbers.append(lag)
                        best_lag.append(lag)
                        reduced_endogs[target_column] = endog[target_column]

                        # print(asset_search, trend_search)

            if len(lag_numbers) > 0:
                yes_causality.append(
                    (target_column, endog_column, len(combined_vector)))
                logging.info("Found evidence that causes for {} and {}".format(
                    endog_column, target_column))
                logging.info('    for lags: {}'.format(lag_numbers))
                logging.info('    for best lags: {}'.format(
                    current_best_lag_numbers))
                if  len(lag_numbers) < 3:
                    show_comparison_graph(days, endog[target_column], endog[endog_column],
                                        v1_label=target_column,
                                        v2_label=endog_column,
                                        title=target_column + " vs " + endog_column)
            else:
                no_causality.append(
                    (endog_column, target_column, len(combined_vector)))
                logging.info("Did not find that {} caused {}".format(
                    endog_column, target_column))

            for lag in lag_numbers:

                try:
                    corr = np.corrcoef(endog_v[lag:], target_v[:-lag])[0][1]
                    fmt_output.append((target_column, endog_column,
                                       lag, gc[lag][0]['params_ftest'][1], corr))
                except Exception as e:
                    logging.info(e)
                    logging.info(lag)

    """
    Big formatted output section
    """
    output_file = open(dmu.create_result_path(markets) + '.' + order + '.csv', 'w')
    output = csv.writer(output_file, delimiter=',')
    row = ["target", "predictor", "lag", "p-value", "corr"]
    output.writerow(row)  # header
    for row in fmt_output:
        print("%s, %s, %3d, %7.5f, %5.3f" % row)
        output.writerow(row)

    count = 0

    print("\n===========================================================\n")

    print("No Causility\n")
    for row in no_causality:
        print("%s and %s showed NO causality, count: %d" % row)
        count += 1
    print("\n-------------------------------------------------------\n")
    print("Causality\n")
    for row in yes_causality:
        print("%s and %s showed causality, count: %d" % row)
        count += 1

    print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
    print("Causality Count: %d" % (len(yes_causality)))
    print("No Causality Count: %d\n" % (len(no_causality)))

    np_total_lag = np.array(total_lag)

    print('std', np.std(np_total_lag))
    print('avg', np.average(np_total_lag))

    if count == 0:
        count = 99999999999999

    print("Percent showing causality: %3.2f\n" %
          (float(len(yes_causality)) / float(count)))

    lag_dict = {x: total_lag.count(x) for x in total_lag}
    lag = OrderedDict(sorted(lag_dict.items()))

    best_lag_dict = {x: best_lag.count(x) for x in best_lag}
    best_lag = OrderedDict(sorted(best_lag_dict.items()))

    y = []
    x = []

    for i in lag:
        x.append(i)
        y.append(lag[i])

    x_best = []
    y_best = []
    for i in best_lag:
        x_best.append(i)
        y_best.append(best_lag[i])

    print("Correlation: %d" % np.sum(y))
    print("Strong Correlation: %d" % np.sum(y_best))

    """
    Generate graph
    """
    width = 1/1.5
    plt.bar(x, y, width, color="blue", label="Correlation")
    plt.bar(x_best, y_best, width, color="red", label="Strong Correlation")
    plt.title("Correlations Prior to Event (in Days)")
    plt.xlabel("Days Prior to an Event")
    plt.ylabel("Number of Correlations")
    plt.legend(loc='upper right')
    plt.show()

    return reduced_endogs
