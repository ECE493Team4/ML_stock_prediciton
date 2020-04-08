import pandas as pd
import numpy as np
import unittest
import psycopg2
import tensorflow as tf
from sklearn.metrics import mean_absolute_error
from lstm_multivariative_multistep import get_data_from_db,normalized,mape,create_time_steps,multivariate_data
from stock_data_collection import get_stock_indicator,data_collection
from prediction import get_input_data,get_lastest_date
from model_retrain import get_data

gpus = tf.config.experimental.list_physical_devices('GPU')
if gpus:
    try:
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)

    except RuntimeError as e:
        print(e)

        
class ML_test(unittest.TestCase):
    print("Testing start")
#   Fetch information from the database based on stock name
  
    def test_get_data_from_db(self):
        print("Testing cases 1 ")
        self.assertEqual(get_data_from_db("aapl")['stock_name'][0], 'aapl')
        self.assertEqual(get_data_from_db("v")['stock_name'][0], 'v')
        self.assertEqual(get_data_from_db("intc")['stock_name'][0], 'intc')
        
        
        
#   Normalized function works
    def test_normalized(self):
        self.assertTrue(np.allclose(normalized(pd.DataFrame([1,2,3])) ,pd.DataFrame([[-1.22474487],[ 0. ],[ 1.22474487]])))
        self.assertTrue(np.allclose(normalized(pd.DataFrame([1,2,1])) ,pd.DataFrame([[-0.70710678],[ 1.41421356],[-0.70710678]])))
        
#   Test the mape is coorect 
    def test_mape(self):
        self.assertTrue((mape([1,2,3],[1,1,1]) - 38.8888) < 0.01)
        self.assertTrue((mape([1,1,1],[1,2,1]) - 33.33333333333333) < 0.01)
        
    def test_create_time_steps(self):
        self.assertEqual(create_time_steps(10),[-10, -9, -8, -7, -6, -5, -4, -3, -2, -1])
        self.assertEqual(len(create_time_steps(5)),5)
        self.assertEqual(len(create_time_steps(-5)),0)
        self.assertEqual(create_time_steps(-1),[])
        
    def test_multivariate_data(self):
        dat = get_data_from_db("aapl")
        features = dat[['open','volume','volume_obv','trend_macd','trend_macd_signal','trend_macd_diff','momentum_rsi','volume_vpt']]
        dataset = normalized(features)
        a,b = multivariate_data(dataset,dataset[:,0],start_index = 0,
                         end_index = 1400,history_size = 70,
                         target_size = 7,step = 1)
        self.assertEqual((a[0] - dataset[0:70]).sum(),0.0)
        self.assertEqual((b[0] - dataset[70:77][:,0]).sum(),0.0)
        self.assertEqual((a[1] - dataset[1:71]).sum(),0.0)
        self.assertEqual((b[1] - dataset[71:78][:,0]).sum(),0.0)
   
    def test_get_stock_indicator(self):
        stock_indicator = get_stock_indicator('aapl')
        self.assertEqual(stock_indicator['stock_name'][0] , 'aapl')
        self.assertTrue(stock_indicator['open'][1700] , 260.16)
        self.assertTrue((stock_indicator['volume'][1700] - 8542125) < 0.01 )
        self.assertTrue((stock_indicator['volume_vpt'][1700] + 66918.8) < 0.01)
        self.assertTrue((stock_indicator['trend_macd'][1700] - 4.48167) < 0.01)
        self.assertTrue((stock_indicator['trend_macd_signal'][1700] - 3.92683) < 0.01)
        self.assertTrue((stock_indicator['trend_macd_diff'][1700] - 0.554836)< 0.01)
        self.assertTrue((stock_indicator['momentum_rsi'][1700] - 59.3258) < 0.01)
        
    def test_data_collection(self):
        key_1 = 'ZCVD46TFPHFYT4XE'
        key_2 = 'M8ISBP0ISFFRNX1'
        data_1 = data_collection('aapl', key_1)
        data_2 = data_collection('aapl',key_2)
        self.assertTrue(data_1.equals(data_2))
        self.assertEqual(data_1['stock_name'][0],'aapl')
        
    def test_get_lastest_date(self):
        date_1 = get_lastest_date(100,"aapl")[0]
        date_2 = get_lastest_date(100,"aapl")[1]
        self.assertTrue(date_1 > date_2)
        date_3 = get_lastest_date(100,"apple")
        self.assertEqual(len(date_3),0)
        
    def test_get_input_data(self):
        dat = get_data_from_db("aapl")
        data_test_1,mean,std = get_input_data('2020-04-06 14:00:00',dat,1)
        features = dat[['open','volume','volume_obv','trend_macd','trend_macd_signal','trend_macd_diff','momentum_rsi','volume_vpt']]
        dataset = features.values
        data_mean = dataset.mean(axis=0)
        data_std = dataset.std(axis=0)
        dataset = (dataset-data_mean)/data_std
        self.assertTrue(np.array_equal(std,data_std))
        self.assertTrue(np.array_equal(mean,data_mean))
        self.assertFalse(np.array_equal(std,data_mean))
        index_high = dat.loc[dat['time_stamp'] == '2020-04-06 14:00:00'].index[0]
        index_low = index_high - 1
        self.assertTrue(np.array_equal(dataset[index_low:index_high],data_test_1))
        data_test_2,std,mean = get_input_data('2020-04-06 14:00:00',dat,3)
        index_high = dat.loc[dat['time_stamp'] == '2020-04-06 14:00:00'].index[0]
        index_low = index_high - 3
        self.assertTrue(np.array_equal(dataset[index_low:index_high],data_test_2))
        
    def test_get_data(self):
        data = get_data(start=None,end=None,stock_name='aapl')
        dat = get_data_from_db("aapl")
        features = dat[['open','volume','volume_obv','trend_macd','trend_macd_signal','trend_macd_diff','momentum_rsi','volume_vpt']]
        dataset = features.values
        data_mean = dataset.mean(axis=0)
        data_std = dataset.std(axis=0)
        dataset = (dataset-data_mean)/data_std
        self.assertTrue(np.array_equal(data,dataset[-140:]))
        data_1 = get_data(start=0,end=1400,stock_name='aapl')
        self.assertTrue(np.array_equal(data_1,dataset[0:1400]))

        
if __name__ == "__main__":
    unittest.main()