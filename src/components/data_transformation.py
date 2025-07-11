import os
import sys
sys.path.append(os.path.join(os.getcwd()))
from constant import *

from src.exceptions import *
from src.logger import *
from src.components.get_data import *

import warnings
warnings.filterwarnings('ignore')

from pymongo import MongoClient
import pandas as pd
from datetime import date, timedelta
import numpy as np

class data_transformation():
    def __init__(self,i):
        self.i = i
        self.today = date.today() - timedelta(self.i)
        self.yest = self.today - timedelta(1)
        self.tom = self.today + timedelta(1)

        self.today_ymd = self.today.strftime('%Y-%m-%d')
        self.yest_ymd = self.yest.strftime('%Y-%m-%d')
        self.tom_ymd = self.tom.strftime('%Y-%m-%d')

        self.today_int = int(self.today.strftime('%Y%m%d'))
        self.yest_int = int(self.yest.strftime('%Y%m%d'))
        self.tom_int = int(self.tom.strftime('%Y%m%d'))
        self.city_dict = city_dict
        self.features = features

    def process_data(self):
        '''
            Perform data processing
        '''
        try:
            gfd = get_forecast_data(self.i)
            demand = gfd.get_demand_data()
            weather = gfd.get_weather_data()
            

            weather['weight'] = weather['city_id'].map(self.city_dict)
            weather['temperaturefeelslike'] = weather['temperaturefeelslike']*weather['weight']
            weather['precipitation'] = weather['precipitation']*weather['weight']
            weather['windgust'] = weather['windgust']*weather['weight']
            weather['cloudcover'] = weather['cloudcover']*weather['weight']
            # weather['precipchance'] = weather['precipchance']*weather['weight']


            weather = weather.groupby(['date_int','hour']).sum().rename_axis(None,axis=1).reset_index()
            
            


            df = pd.merge(demand,weather,on=['date_int','hour'],how='left')
            df = df.dropna()
            df = df.sort_values(['date_int','hour']).reset_index(drop=True)
            df = self.create_date_para(df)
            logging.info('Date parameters created')

            df = self.create_lags(df,'demand')

            columns_list = self.features.copy()
            columns_list.append('demand')
            df = df[columns_list]

            logging.info('Data prepared for training')

            return df

        
        

        except Exception as e:
            
            logging.error(CustomException(e,sys))


    def process_test_data(self):

        try:
            logging.info('Creating test data')
            gtd = get_test_data(self.i)
            test_df = gtd.create_test_data()
            

        except Exception as e:
            logging.error(CustomException(e,sys))

        # add date and lag parameters
        try:
            test_df = self.create_date_para(test_df)
            
            test_df = self.create_lags(test_df,'demand')

            test_df = test_df[test_df['date_int']==self.tom_int].reset_index(drop=True)
            test_df = test_df[self.features]

            logging.info('Test data created successfully...')

            return test_df

            
        except Exception as e:
            logging.error(CustomException(e,sys))



        

    def create_date_para(self,df):
        '''
            Enter dataframe with dateint as a column with the format "%Y%m%d"
        '''

        try:
            df["month"] = pd.to_datetime(df['date_int'],format='%Y%m%d').dt.month
            df["year"] = pd.to_datetime(df['date_int'],format='%Y%m%d').dt.year
            df["day"] = pd.to_datetime(df['date_int'],format='%Y%m%d').dt.day
            df['week']=pd.to_datetime(df['date_int'],format='%Y%m%d').dt.isocalendar().week
            df['dayofweek'] = pd.to_datetime(df['date_int'],format='%Y%m%d').dt.dayofweek
            df['quarter'] = pd.to_datetime(df['date_int'],format='%Y%m%d').dt.quarter
            df['dayofyear'] = pd.to_datetime(df['date_int'],format='%Y%m%d').dt.dayofyear
            
            logging.info('Date parameters created successfully...')

        except Exception as e:
            logging.error(CustomException(e,sys))

        return df
    
    def create_lags(self,df,column_name):
        '''
        Enter dataframe to lags of demand
        '''
        try:
            df = df.sort_values(['date_int','hour']).reset_index(drop=True)
            for i in range(1,8):
                df[f'lag{i}'] = df[column_name].shift(i*24)

            df = df.dropna(subset=[f'lag{i}'])

            logging.info('Lags created successfully...')

        except Exception as e:
            # print(e)
            logging.error(CustomException(e,sys))
        return df
    


