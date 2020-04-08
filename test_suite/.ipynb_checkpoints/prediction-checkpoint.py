from datetime import timedelta
from datetime import date
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import ta
# The DAG object; we'll need this to instantiate a DAG
# from airflow import DAG
# # Operators; we need this to operate!
# from airflow.operators.bash_operator import BashOperator
# from airflow.utils.dates import days_ago
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.sensors import TimeDeltaSensor

import pickle
import numpy as np
# import tensorflow as tf


# gpus = tf.config.experimental.list_physical_devices('GPU')
# if gpus:
#     try:
#         for gpu in gpus:
#             tf.config.experimental.set_memory_growth(gpu, True)

#     except RuntimeError as e:
#         print(e)
        

class postgresql_db_config:
    NAME = 'stock_data'
    PORT = 5432
    HOST = '162.246.156.44'
    USER = 'stock_data_admin'
    PASSWORD = 'ece493_team4_stock_data'




# time has to start from index 70 including 70
def get_input_data(time_stamp,dat,step_size):
    index_high = dat.loc[dat['time_stamp'] == time_stamp].index[0]
    index_low = index_high - step_size
    features = dat[['open','volume','volume_obv','trend_macd','trend_macd_signal','trend_macd_diff','momentum_rsi','volume_vpt']]
    dataset = features.values
    data_mean = dataset.mean(axis=0)
    data_std = dataset.std(axis=0)
    dataset = (dataset-data_mean)/data_std
    return dataset[index_low:index_high],data_mean,data_std


def get_predict_data(time_stamp,stock_name):
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
    features = dat[['open','volume','volume_obv','trend_macd','trend_macd_signal','trend_macd_diff','momentum_rsi','volume_vpt']]
    multi_step_model = tf.keras.models.load_model(f'/home/centos/airflow/dags/saved_model/{stock_name}.h5')
    print(f'load model from : ---- /home/centos/airflow/dags/saved_model/{stock_name}.h5')
    input_data,mean,std = get_input_data(time_stamp,dat,70)
    prediction_result_norm = multi_step_model.predict(np.expand_dims(input_data,axis = 0))
    prediction_result= np.add(np.multiply(prediction_result_norm,std[0]),mean[0])
    return prediction_result[0]


def get_lastest_date(step,stock_name):
    postgre_db = psycopg2.connect(dbname = postgresql_db_config.NAME,
                                    user = postgresql_db_config.USER,
                                    password = postgresql_db_config.PASSWORD,
                                    host = postgresql_db_config.HOST,
                                    port = postgresql_db_config.PORT)
    cur = postgre_db.cursor()

    sql =f'''
    select * from public.stock_data_full where stock_name = '{stock_name}' order by time_stamp desc
    '''
    dat = sqlio.read_sql_query(sql, postgre_db)
    dat = dat.dropna()
    dat = dat.reset_index(drop=True)
    return dat['time_stamp'][0:step]
    
def store_prediction_to_db(step,stock_name):
    for time_stamp in get_lastest_date(step,stock_name):
        prediction = get_predict_data(time_stamp,stock_name)
        print(prediction)
        print(time_stamp)
        
        conn = psycopg2.connect(dbname = postgresql_db_config.NAME,
                                        user = postgresql_db_config.USER,
                                        password = postgresql_db_config.PASSWORD,
                                        host = postgresql_db_config.HOST,
                                        port = postgresql_db_config.PORT)
        cur = conn.cursor()
        cur.execute( f'''
        INSERT INTO stock_prediction(stock_name,time_stamp,prediction)
        VALUES ('{stock_name}','{time_stamp}', ARRAY[{prediction[0]},{prediction[1]},{prediction[2]},{prediction[3]},{prediction[4]},{prediction[5]},{prediction[6]}])
        ON CONFLICT (stock_name,time_stamp) DO UPDATE 
            SET prediction = excluded.prediction
        '''
        )
        cur.close()
        conn.commit()
        conn.close() 
    
    
    
    
    
# # These args will get passed on to each operator
# # You can override them on a per-task basis during operator initialization
# default_args = {
#     'owner': 'zzhmtxxhh',
#     'depends_on_past': False,
#     'start_date': days_ago(0),
#     'email': ['zhihao9@ualberta.ca'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=3),
#     # 'queue': 'bash_queue',
#     # 'pool': 'backfill',
#     # 'priority_weight': 10,
#     # 'end_date': datetime(2016, 1, 1),
#     # 'wait_for_downstream': False,
#     # 'dag': dag,
#     # 'sla': timedelta(hours=2),
#     # 'execution_timeout': timedelta(seconds=300),
#     # 'on_failure_callback': some_function,
#     # 'on_success_callback': some_other_function,
#     # 'on_retry_callback': another_function,
#     # 'sla_miss_callback': yet_another_function,
#     # 'trigger_rule': 'all_success'
# }

# dag = DAG(
#     'stock_data_prediction',
#     default_args=default_args,
#     description='Data prediction',
#     schedule_interval='20 * * * *',
# )



# # t1, t2 and t3 are examples of tasks created by instantiating operators
# aapl_prediction = PythonOperator(
#     task_id='aapl_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'aapl'},
#     dag=dag
# )

# dis_prediction = PythonOperator(
#     task_id='dis_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'dis'},
#     dag=dag
# )

# ge_prediction = PythonOperator(
#     task_id='ge_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'ge'},
#     dag=dag
# )

# ibm_prediction = PythonOperator(
#     task_id='ibm_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'ibm'},
#     dag=dag
# )

# intc_prediction = PythonOperator(
#     task_id='intc_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'intc'},
#     dag=dag
# )

# jpm_prediction = PythonOperator(
#     task_id='jpm_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'jpm'},
#     dag=dag
# )

# msft_prediction = PythonOperator(
#     task_id='msft_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'msft'},
#     dag=dag
# )

# nke_prediction = PythonOperator(
#     task_id='nke_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'nke'},
#     dag=dag
# )

# v_prediction = PythonOperator(
#     task_id='v_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'v'},
#     dag=dag
# )

# wmt_prediction = PythonOperator(
#     task_id='wmt_prediction',
#     python_callable=store_prediction_to_db,
#     op_kwargs={'step': 2,'stock_name':'wmt'},
#     dag=dag
# )

# aapl_prediction >> dis_prediction >> ge_prediction >> ibm_prediction >> intc_prediction >> jpm_prediction >> msft_prediction >>nke_prediction >> v_prediction >> wmt_prediction