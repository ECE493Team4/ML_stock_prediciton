from datetime import timedelta
from datetime import date
import pandas as pd
import psycopg2
import ta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.sensors import TimeDeltaSensor

curtis_key = 'ZCVD46TFPHFYT4XE'
walter_key = 'M8ISBP0ISFFRNX1'

def get_stock_indicator(stock_name):
    conn = psycopg2.connect(user = "stock_data_admin",
                            password = "ece493_team4_stock_data",
                            host = "162.246.156.44",
                            port = "5432",
                            database = "stock_data")

    stock = pd.read_sql_query("select * from stock_data where stock_name = '" + stock_name +"' order by time_stamp asc",con=conn)
    stock = stock.dropna()
    stock = ta.add_all_ta_features(
    stock, open="open", high="high", low="low", close="close", volume="volume")
    stock = stock[['stock_name','time_stamp','open','low','high','close','volume',
        'volume_obv','volume_vpt','trend_macd','trend_macd_signal','trend_macd_diff',
        'trend_ichimoku_a','trend_ichimoku_b','trend_visual_ichimoku_a',
        'trend_visual_ichimoku_b','momentum_rsi','momentum_kama',]]
    
    stock = stock.astype(object).where(pd.notnull(stock), None)
    return stock



def save_indicator(stock_name):
    
    stock = get_stock_indicator(stock_name)
  
    conn = psycopg2.connect(user = "stock_data_admin",
                            password = "ece493_team4_stock_data",
                            host = "162.246.156.44",
                            port = "5432",
                            database = "stock_data")
    
    cur = conn.cursor()
      
    for index, row in stock.iterrows():
        cur.execute(f'''
        INSERT INTO stock_data_full(stock_name,time_stamp,open,high,low,close,volume,volume_obv,volume_vpt,trend_macd,trend_macd_signal,trend_macd_diff,trend_ichimoku_a,trend_ichimoku_b,trend_visual_ichimoku_a,trend_visual_ichimoku_b,momentum_rsi,momentum_kama)
        VALUES (%s, %s,%s,%s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s)
        ON CONFLICT (stock_name,time_stamp) DO UPDATE 
        SET stock_name = excluded.stock_name, 
            time_stamp = excluded.time_stamp,
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            volume_obv = excluded.volume_obv,
            volume_vpt = excluded.volume_vpt,
            trend_macd = excluded.trend_macd,
            trend_macd_signal = excluded.trend_macd_signal,
            trend_macd_diff = excluded.trend_macd_diff,
            trend_ichimoku_a = excluded.trend_ichimoku_a,
            trend_ichimoku_b = excluded.trend_ichimoku_b,
            trend_visual_ichimoku_a = excluded.trend_visual_ichimoku_a,
            trend_visual_ichimoku_b = excluded.trend_visual_ichimoku_b,
            momentum_rsi = excluded.momentum_rsi,
            momentum_kama = excluded.momentum_kama; 
        ''', (row['stock_name'],row['time_stamp'],row['open'],row['high'],row['low'],row['close'],row['volume'],row['volume_obv'],row['volume_vpt'],row['trend_macd'],row['trend_macd_signal'],row['trend_macd_diff'],row['trend_ichimoku_a'],row['trend_ichimoku_b'],row['trend_visual_ichimoku_a'],row['trend_visual_ichimoku_b'],row['momentum_rsi'],row['momentum_kama']))
    
    
    cur.close()
    conn.commit()
    conn.close() 







def data_collection(stock_name,api_key):
    df = pd.read_json('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='+stock_name.upper()+'&interval=60min&outputsize=full&apikey='+api_key)# df = pd.DataFrame(df['Time Series (60min)'].values)
    data = df['Time Series (60min)'][6:15]
    df = pd.DataFrame (columns = ['stock_name','time_stamp','open','high','low','close','volume'])
    for i in data.keys():
        df.loc[i] = [stock_name] + [i] + [data[i]['1. open']] + [data[i]['2. high']] +[data[i]['3. low']] + [data[i]['4. close']] + [data[i]['5. volume']]
    df['time_stamp'] = pd.to_datetime(df['time_stamp'], format="%Y-%m-%d %H:%M:%S") - pd.Timedelta(minutes = 30)
    df.reset_index(inplace=True)
    df = df[['stock_name','time_stamp','open','high','low','close','volume']]


    conn = psycopg2.connect(user = "stock_data_admin",
                                      password = "ece493_team4_stock_data",
                                      host = "162.246.156.44",
                                      port = "5432",
                                      database = "stock_data")
    cur = conn.cursor()

    for index, row in df.iterrows():
        cur.execute(f'''
        INSERT INTO stock_data(stock_name,time_stamp,open,high,low,close,volume)
        VALUES ('{row['stock_name']}','{row['time_stamp']}', {row['open']},{row['high']},{row['low']},{row['close']},{row['volume']})
        ON CONFLICT (stock_name,time_stamp) DO UPDATE 
        SET stock_name = excluded.stock_name, 
            time_stamp = excluded.time_stamp,
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume;
        ''')
    cur.close()
    conn.commit()
    conn.close()
    
def remove_zero():
    conn = psycopg2.connect(user = "stock_data_admin",
                                      password = "ece493_team4_stock_data",
                                      host = "162.246.156.44",
                                      port = "5432",
                                      database = "stock_data")
    cur = conn.cursor()

    
    cur.execute(f'''
    DELETE 
    FROM stock_data
    WHERE volume = 0;
    ''')
    cur.close()
    conn.commit()
    conn.close() 





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
    'stock_data_collection',
    default_args=default_args,
    description='Data Collection',
    schedule_interval='0 * * * *',
)


t0 = BashOperator(
    task_id='sleep_3',
    bash_command='sleep 3',
    dag=dag,
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
aapl = PythonOperator(
    task_id='aapl_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'aapl','api_key':walter_key},
    dag=dag
    
)

dis = PythonOperator(
    task_id='dis_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'dis','api_key':walter_key},
    dag=dag
)
ge = PythonOperator(
    task_id='ge_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'ge','api_key':walter_key},
    dag=dag
)
ibm = PythonOperator(
    task_id='ibm_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'ibm','api_key':walter_key},
    dag=dag
)
intc = PythonOperator(
    task_id='intc_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'intc','api_key':walter_key},
    dag=dag
)
jpm = PythonOperator(
    task_id='jpm_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'jpm','api_key':curtis_key},
    dag=dag,
)
msft = PythonOperator(
    task_id='msft_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'msft','api_key':curtis_key},
    dag=dag,
)

nke = PythonOperator(
    task_id='nke_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'nke','api_key':curtis_key},
    dag=dag,
)
v = PythonOperator(
    task_id='v_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'v','api_key':curtis_key},
    dag=dag,
)

wmt = PythonOperator(
    task_id='wmt_collection',
    python_callable=data_collection,
    op_kwargs={'stock_name': 'wmt','api_key':curtis_key},
    dag=dag,
)

rm_zero = PythonOperator(
    task_id='remove_zero_volume',
    python_callable=remove_zero,
    dag=dag,
)


aapl_ind = PythonOperator(
    task_id='aapl_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'aapl'},
    dag=dag
    
)

dis_ind = PythonOperator(
    task_id='dis_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'dis'},
    dag=dag
    
)
ge_ind = PythonOperator(
    task_id='ge_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'ge'},
    dag=dag
    
)
ibm_ind = PythonOperator(
    task_id='ibm_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'ibm'},
    dag=dag
)
intc_ind = PythonOperator(
    task_id='intc_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'intc'},
    dag=dag
)

msft_ind = PythonOperator(
    task_id='msft_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'msft'},
    dag=dag,
)

nke_ind = PythonOperator(
    task_id='nke_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'nke'},
    dag=dag,
)
v_ind = PythonOperator(
    task_id='v_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'v'},
    dag=dag,
)

wmt_ind = PythonOperator(
    task_id='wmt_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'wmt'},
    dag=dag,
)

jpm_ind = PythonOperator(
    task_id='jpm_indicator',
    python_callable=save_indicator,
    op_kwargs={'stock_name': 'jpm'},
    dag=dag,
)

[jpm,dis,nke,ibm,intc] >> t0 >>[aapl,msft,ge,v,wmt] >> rm_zero >> [jpm_ind,aapl_ind,dis_ind,ge_ind,ibm_ind,intc_ind,msft_ind,nke_ind,v_ind,wmt_ind]
