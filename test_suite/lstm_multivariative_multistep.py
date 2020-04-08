import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
import pickle
from datetime import datetime,timedelta
import shutil
import os
import numpy as np
import cufflinks as cf
cf.go_offline()
cf.set_config_file(offline=True, world_readable=True)
# import tensorflow as tf
import matplotlib as mpl
import matplotlib.pyplot as plt
pd.plotting.register_matplotlib_converters()

# gpus = tf.config.experimental.list_physical_devices('GPU')
# if gpus:
#     try:
#         for gpu in gpus:
#             tf.config.experimental.set_memory_growth(gpu, True)

#     except RuntimeError as e:
#         print(e)

mpl.rcParams['figure.figsize'] = (8, 6)
mpl.rcParams['axes.grid'] = False

class postgresql_db_config:
    NAME = 'stock_data'
    PORT = 5432
    HOST = '162.246.156.44'
    USER = 'stock_data_admin'
    PASSWORD = 'ece493_team4_stock_data'
    
    
def mape(y,y_pr):
    y,y_pr = np.array(y),np.array(y_pr)
    return np.mean(np.abs((y-y_pr)/y))*100

def multivariate_data(dataset, target, start_index, end_index, history_size,
                      target_size, step, single_step=False):
    data = []
    labels = []

    start_index = start_index + history_size
    if end_index is None:
        end_index = len(dataset) - target_size

    for i in range(start_index, end_index):
        indices = range(i-history_size, i, step)
        data.append(dataset[indices])

        if single_step:
            labels.append(target[i+target_size])
        else:
            labels.append(target[i:i+target_size])
 
    return np.array(data), np.array(labels)


def get_data_from_db(stock_name):
    postgre_db = psycopg2.connect(dbname = postgresql_db_config.NAME,
                                    user = postgresql_db_config.USER,
                                    password = postgresql_db_config.PASSWORD,
                                    host = postgresql_db_config.HOST,
                                    port = postgresql_db_config.PORT)

    sql =f'''
    select * from public.stock_data_full where stock_name = '{stock_name}' order by time_stamp asc
    '''

    dat = sqlio.read_sql_query(sql, postgre_db)


    dat
    dat = dat.dropna()
    dat = dat.reset_index(drop=True)
    return dat


def normalized(features):
    dataset = features.values
    data_mean = dataset.mean(axis=0)
    data_std = dataset.std(axis=0)
    dataset = (dataset-data_mean)/data_std
    return dataset

# This funciton is to create the input for the model 
def multi_step_plot(history, true_future, prediction,iteration):
#   'open','volume','volume_obv','trend_macd_signal','trend_macd_diff','momentum_rsi','volume_vpt'
    plt.figure(figsize=(12, 6))
    num_in = create_time_steps(len(history))
    num_out = len(true_future)
    plt.subplot(211)
    plt.plot(num_in, np.array(history[:, 0]), label='open')
    plt.plot(num_in, np.array(history[:, 1]), label='volume')
    plt.plot(num_in, np.array(history[:, 2]), label='volume_obv')
    plt.plot(num_in, np.array(history[:, 3]), label='trend_macd')
    plt.plot(num_in, np.array(history[:, 4]), label='trend_macd_signal')
    plt.plot(num_in, np.array(history[:, 5]), label='trend_macd_diff')
    plt.plot(num_in, np.array(history[:, 6]), label='momentum_rsi')
    plt.plot(num_in, np.array(history[:, 7]), label='volume_vpt')
    plt.plot(np.arange(num_out)/STEP, np.array(true_future), 'b',
           label='True Future')
    if prediction.any():
        plt.plot(np.arange(num_out)/STEP, np.array(prediction), 'r',
        label='Predicted Future')
    plt.legend(loc='upper left')
    plt.title("Iterarion: {}".format(iteration))
    plt.show()
    
def create_time_steps(length):
    time_steps = []
    for i in range(-length, 0, 1):
        time_steps.append(i)
    return time_steps

def multi_step_plot_test(history, true_future, prediction,iteration):
        
    plt.figure(figsize=(12, 6))
    num_in = create_time_steps(len(history))
    num_out = len(true_future)
    plt.subplot(211)
    plt.plot(num_in, np.array(history[:, 0]), label='open')
    
    
    plt.plot(np.arange(num_out)/STEP, np.array(true_future), 'b',
           label='True Future')
    if prediction.any():
        plt.plot(np.arange(num_out)/STEP, np.array(prediction), 'r',
        label='Predicted Future')
    plt.legend(loc='upper left')
    plt.title("Iterarion: {}".format(iteration))
#     plt.savefig('pic2/pic{}.png'.format(iteration))
    plt.show()