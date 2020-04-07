from datetime import timedelta
from datetime import date
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import ta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.sensors import TimeDeltaSensor

import pickle
import numpy as np
import tensorflow as tf


gpus = tf.config.experimental.list_physical_devices('GPU')
if gpus:
    try:
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)

    except RuntimeError as e:
        print(e)


class postgresql_db_config:
    NAME = 'stock_data'
    PORT = 5432
    HOST = '162.246.156.44'
    USER = 'stock_data_admin'
    PASSWORD = 'ece493_team4_stock_data'

    
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


def get_data(start,end,stock_name):
    postgre_db = psycopg2.connect(dbname = postgresql_db_config.NAME,
                                    user = postgresql_db_config.USER,
                                    password = postgresql_db_config.PASSWORD,
                                    host = postgresql_db_config.HOST,
                                    port = postgresql_db_config.PORT)
    sql =f'''
    select * from public.stock_data_full where stock_name = '{stock_name}' order by time_stamp asc
    '''
    dat = sqlio.read_sql_query(sql, postgre_db)
    dat = dat.dropna()
    dat = dat.reset_index(drop=True)
    print(f"Now we are processing stock : {dat['stock_name'][0]}")
    features = dat[['open','volume','volume_obv','trend_macd','trend_macd_signal','trend_macd_diff','momentum_rsi','volume_vpt']]
    dataset = features.values
    data_mean = dataset.mean(axis=0)
    data_std = dataset.std(axis=0)
    dataset = (dataset-data_mean)/data_std
    if end == None:
        end = dataset.shape[0]
    if start == None:
        start = dataset.shape[0]-140
    return dataset[start:end]


def retrain_model(start,stock_name,TRAIN_SPLIT=None,end=None):
    dataset = get_data(start,end,stock_name)
    print(dataset.shape)
    past_history = 70
    future_target = 7
    STEP = 1
    x_train_multi, y_train_multi = multivariate_data(dataset, dataset[:, 0], 0,
                                                     TRAIN_SPLIT, past_history,
                                                     future_target, STEP)
    BATCH_SIZE = 30
    BUFFER_SIZE = x_train_multi[0].shape[0]
    train_data_multi = tf.data.Dataset.from_tensor_slices((x_train_multi, y_train_multi))
    train_data_multi = train_data_multi.cache().shuffle(BUFFER_SIZE).batch(BATCH_SIZE).repeat()
    multi_stap_model = tf.keras.models.load_model(f"/home/centos/airflow/dags/saved_model/{stock_name}.h5")
    multi_stap_model.fit(x_train_multi,y_train_multi,verbose=True,epochs = 15)
    multi_stap_model.save(f"/home/centos/airflow/dags/saved_model/{stock_name}.h5")







# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'zzhmtxxhh',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['zhihao9@ualberta.ca'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'model_retraining',
    default_args=default_args,
    description='Model retraining',
    schedule_interval='20 0 * * 6',
)


# t0 = BashOperator(
#     task_id='sleep_3',
#     bash_command='sleep 2',
#     dag=dag,
# )

# t1, t2 and t3 are examples of tasks created by instantiating operators
aapl_model_retrain = PythonOperator(
    task_id='aapl_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'aapl','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)
dis_model_retrain = PythonOperator(
    task_id='dis_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'dis','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)
ge_model_retrain = PythonOperator(
    task_id='ge_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'ge','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)
ibm_model_retrain = PythonOperator(
    task_id='ibm_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'ibm','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)
intc_model_retrain = PythonOperator(
    task_id='intc_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'intc','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)
jpm_model_retrain = PythonOperator(
    task_id='jpm_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'jpm','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)
msft_model_retrain = PythonOperator(
    task_id='msft_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'msft','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)
nke_model_retrain = PythonOperator(
    task_id='nke_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'nke','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)
v_model_retrain = PythonOperator(
    task_id='v_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'v','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)
wmt_model_retrain = PythonOperator(
    task_id='wmt_retraining',
    python_callable=retrain_model,
    op_kwargs={'stock_name': 'wmt','start':None,'end':None,'TRAIN_SPLIT':None},
    dag=dag
)




aapl_model_retrain >> dis_model_retrain >> ge_model_retrain >> ibm_model_retrain >> intc_model_retrain >> jpm_model_retrain >>msft_model_retrain >>nke_model_retrain>> v_model_retrain >> wmt_model_retrain
