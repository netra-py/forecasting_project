import os
import sys
sys.path.append(os.path.join(os.getcwd()))
from constant import *

from src.exceptions import *
from src.logger import *

from pymongo import MongoClient
import pandas as pd
from datetime import date, timedelta
import numpy as np

import warnings
warnings.filterwarnings('ignore')


# try:
#     a = 1/0
# except Exception as e:
#     logging.error(CustomException(e,sys))


# class to prepare mongo db query
class get_mongo_query():
    def __init__(self):
        self.client = MongoClient(mongo_url)
        self.db = self.client[db]
        

    def get_mongo_query(self,**conditions):

        '''
            Enter conditions to get query
            in case of list pass arguments as []
            in case of gte, lte pass arguments as (start,end)
        '''
        query = {}

        for key,value in conditions.items():
            if value is None:
                continue

            elif isinstance(value, list):
                query[key] = {'$in':value}

            elif isinstance(value, tuple) and len(value) == 2:
                query[key] = {'$gte':value[0],'$lte':value[1]}

            else:
                query[key] = value
            

        return query
        
    def get_db_data(self,collection_name,query,column_list):
        '''
            get data from DB
        '''
        collection = self.db[collection_name]

        column_dict = {'_id':0}
        for column in column_list:
            column_dict[column] = 1

        data = collection.find(query,column_dict)
        df = pd.DataFrame(data)

        return df
    

# class to get 24 hrs data
class get_forecast_data(get_mongo_query):
    '''
        prepare train data where 
        i:- number to be subtract from today (0 for tomorrow's forecast)
        
    '''
    def __init__(self,i):
        super().__init__()
        self.today = date.today() - timedelta(i)
        self.yest = self.today - timedelta(1)
        self.tom = self.today + timedelta(1)

        self.today_ymd = self.today.strftime('%Y-%m-%d')
        self.yest_ymd = self.yest.strftime('%Y-%m-%d')
        self.tom_ymd = self.tom.strftime('%Y-%m-%d')

        self.today_int = int(self.today.strftime('%Y%m%d'))
        self.yest_int = int(self.yest.strftime('%Y%m%d'))
        self.tom_int = int(self.tom.strftime('%Y%m%d'))

        self.city_dict = city_dict
        self.features = col_list
        
        self.demand_db = db_for_demand
        self.forecast_db = db_for_forecasted_demand
        self.weather_db = db_for_nowcast_weather
        self.cc_db = db_for_cloudcover
        self.forecast_weather_db = db_for_forecast_weather

        # date to get 60 days data from forecast date
        latest = self.tom - timedelta(60)
        self.latest_int = int(latest.strftime('%Y%m%d'))

        # getting last 60 days data from forecast date from previous and its previous year
        start1 = self.tom - timedelta(385)
        self.start1_int = int(start1.strftime('%Y%m%d'))
        end1 = self.tom - timedelta(335)
        self.end1_int = int(end1.strftime('%Y%m%d'))

        start2 = self.tom - timedelta(790)
        self.start2_int = int(start2.strftime('%Y%m%d'))
        end2 = self.tom - timedelta(700)
        self.end2_int = int(end2.strftime('%Y%m%d'))
        

        
    def get_demand_data(self):
        '''
            get data for previous 3 months and same period data for previous two years
        '''
        
        try:
            logging.info(f'Preparing forecast for {self.tom_ymd}')
            logging.info('Preparing data...')
            
            # getting demand data for two months in current year
            # firstly prepare query from function 
            query = self.get_mongo_query(date_int = (self.latest_int,self.yest_int))
            # give columns list
            column_list = ['date_int','hour','demand']
            # now get data from db from function
            dem1 = self.get_db_data(self.demand_db,query,column_list)
            # data from db is ready

            # getting demand data for two months in previous year
            query = self.get_mongo_query(date_int = (self.start1_int,self.end1_int))
            dem2 = self.get_db_data(self.demand_db,query,column_list)

            # getting demand data for two months in previous previous year
            query = self.get_mongo_query(date_int = (self.start2_int,self.end2_int))
            dem3 = self.get_db_data(self.demand_db,query,column_list)
            
            
            dem = pd.concat([dem1,dem2,dem3]).reset_index(drop=True)
            logging.info('Demand data fetched')
        except Exception as e:
            logging.error(CustomException(e,sys))

        # getting today's forecast
        try:
            query = self.get_mongo_query(date_int = self.today_int)
            column_list = ['date_int','hour','value']
            fore = self.get_db_data(self.forecast_db,query,column_list)
            
            
            fore = fore.rename(columns={'value':'demand'})
            
            fore = fore[['date_int','hour','demand']]
            logging.info('Forecast data fetched')

        except Exception as e:
            logging.error(CustomException(e,sys))

        # concat demand and forecast data
        try:
            dem = pd.concat([dem,fore])
            dem = dem.sort_values(['date_int','hour']).reset_index(drop=True)
            
            logging.info('Demand data has been prepared')

            return dem
        except:
            logging.error(CustomException(e,sys))

        
    def get_weather_data(self):    
        # getting weather data
        try:
            # getting weather data for two months in previous year
            query = self.get_mongo_query(date_int = (self.latest_int,self.yest_int), city_id = list(self.city_dict.keys()))
            column_list = ['date_int','hour','city_id','temperaturefeelslike','precipitation','windgust']
            wea1 = self.get_db_data(self.weather_db,query,column_list)

            query = self.get_mongo_query(date_int = (self.start1_int,self.end1_int), city_id = list(self.city_dict.keys()))
            wea2 = self.get_db_data(self.weather_db,query,column_list)
            
            query = self.get_mongo_query(date_int = (self.start2_int,self.end2_int), city_id = list(self.city_dict.keys()))
            wea3 = self.get_db_data(self.weather_db,query,column_list)
            
            # concat data 
            wea = pd.concat([wea1,wea2,wea3]).reset_index(drop=True)
            logging.info('Nowcast weather data fetched')

        except Exception as e:
            logging.error(CustomException(e,sys))

        # getting cloudcover nowcast data
        try:
            query = self.get_mongo_query(date_int = (self.latest_int,self.yest_int), city_id = list(self.city_dict.keys()))
            column_list = ['date_int','time_block','city_id','cloudcover']
            wea1 = self.get_db_data(self.cc_db,query,column_list)

            query = self.get_mongo_query(date_int = (self.start1_int,self.end1_int), city_id = list(self.city_dict.keys()))
            wea2 = self.get_db_data(self.cc_db,query,column_list)
            
            query = self.get_mongo_query(date_int = (self.start2_int,self.end2_int), city_id = list(self.city_dict.keys()))
            wea3 = self.get_db_data(self.cc_db,query,column_list)
            
            # concat data 
            cc = pd.concat([wea1,wea2,wea3]).reset_index(drop=True)
            
            cc['hour'] = np.where(cc['time_block']%4==0, cc['time_block']/4,np.nan)
            cc = cc[~cc['hour'].isna()]
            cc = cc.reset_index(drop=True)
            logging.info('Cloudcover Nowcast weather data fetched')
        

        except Exception as e:
            logging.error(CustomException(e,sys))

        try:
            now_weather = pd.merge(wea,cc,on=['date_int','city_id','hour'],how='left').reset_index(drop=True)
            logging.info('Nowcast and cloudcover data concated')
        except:
            logging.error(CustomException(e,sys))

        


        # getting forecasted weather data for today
        try:
            query = self.get_mongo_query(date_int = self.today_int, city_id = list(self.city_dict.keys()))
            
            column_list = ['date_int','time_block','city_id','temperaturefeelslike','precipitation','windgust','cloudcover']
            tod_wea = self.get_db_data(self.forecast_weather_db,query,column_list)
            tod_wea['windgust'] = tod_wea['windgust'].fillna(0)
            
            tod_wea['hour'] = np.where(tod_wea['time_block']%4==0, tod_wea['time_block']/4,np.nan)
            tod_wea = tod_wea[~tod_wea['hour'].isna()]
            tod_wea = tod_wea.reset_index(drop=True)
            logging.info('Today weather data fetched')
        except Exception as e:
            logging.error(CustomException(e,sys))

        try:
            now_weather = pd.concat([now_weather,tod_wea])
            now_weather = now_weather.sort_values(['date_int','city_id','hour']).reset_index(drop=True)

            # now_weather2
            now_weather = now_weather[['city_id','hour', 'date_int', 'precipitation', 'temperaturefeelslike',
       'windgust',  'cloudcover']]
            logging.info('Weather data prepared')
            logging.info('Demand and Weather data fetched successfully...')
        except Exception as e:
            logging.error(CustomException(e,sys))

        return now_weather
    
    
    
class get_test_data(get_mongo_query):  
    def __init__(self,i):
        super().__init__()
        
        self.city_dict = city_dict
        

        self.features = features
        self.today = date.today() - timedelta(i)
        self.yest = self.today - timedelta(1)
        self.tom = self.today + timedelta(1)
        self.tom_int = int((self.tom).strftime('%Y%m%d'))
        self.forecast_weather_db = db_for_forecast_weather

        
      
    
    def create_test_data(self):

        # getting demand
        dem = get_forecast_data(0)
        df = dem.get_demand_data()
        
        # getting demand 8 days prior to forecast date
        tom_8 = self.tom - timedelta(8)
        tom_8_int = int(tom_8.strftime('%Y%m%d'))

        

        df = df[['date_int','hour','demand']]
        df = df[(df['date_int']>=tom_8_int)&(df['date_int']<self.tom_int)]

        master_df = pd.DataFrame()
        start = pd.to_datetime(self.tom_int,format='%Y%m%d')
        end = pd.to_datetime(self.tom_int,format='%Y%m%d')

        
        datess = pd.date_range(start=start,end = (end+timedelta(1)),freq='1H')
        
        datess = datess[:-1]
        
        # for i in list(self.city_dict.keys()):
        blank_df = pd.DataFrame()
        blank_df['date_int'] = (datess.strftime('%Y%m%d')).astype(int)

        blank_df['hour'] = np.tile(np.arange(1,25,1),len(blank_df))[:len(blank_df)]
        
        master_df = blank_df.copy()
        

        

        dff = pd.concat([df,master_df])
        
        

        

        # getting forecasted weather for forecast date
        try:
            query = self.get_mongo_query(date_int = self.tom_int, city_id = list(self.city_dict.keys()))
            
            column_list = ['date_int','time_block','city_id','temperaturefeelslike','precipitation','windgust','cloudcover']
            tom_wea = self.get_db_data(self.forecast_weather_db,query,column_list)
            tom_wea['windgust'] = tom_wea['windgust'].fillna(0)

            
            tom_wea['hour'] = np.where(tom_wea['time_block']%4==0, tom_wea['time_block']/4,np.nan)
            tom_wea = tom_wea[~tom_wea['hour'].isna()]
            tom_wea = tom_wea.reset_index(drop=True)

            tom_wea['weight'] = tom_wea['city_id'].map(self.city_dict)
            tom_wea['temperaturefeelslike'] = tom_wea['temperaturefeelslike']*tom_wea['weight']
            tom_wea['precipitation'] = tom_wea['precipitation']*tom_wea['weight']
            tom_wea['windgust'] = tom_wea['windgust']*tom_wea['weight']
            tom_wea['cloudcover'] = tom_wea['cloudcover']*tom_wea['weight']

            tom_wea = tom_wea.groupby(['date_int','hour']).sum().rename_axis(None,axis=1).reset_index()
            tom_wea = tom_wea.drop('city_id',axis=1)


            logging.info('Forecasted weather data fetched')
        except Exception as e:
            logging.error(CustomException(e,sys))

        # concat weather and demand
        try:
            test_df = pd.merge(dff,tom_wea,on=['date_int','hour'],how='left')
            
            logging.info('Demand and forecast weather concated successfully...')

            return test_df
            
        except:
            logging.error(CustomException(e,sys))

        
    