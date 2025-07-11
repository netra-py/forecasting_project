import os
import sys
sys.path.append(os.path.join(os.getcwd()))
from constant import *

from src.exceptions import *
from src.logger import *
from src.components.get_data import *
from src.components.data_transformation import *

import warnings
warnings.filterwarnings('ignore')


from pymongo import MongoClient
import pandas as pd
from datetime import date, timedelta
import numpy as np
from sklearn.linear_model import LinearRegression,Lasso,Ridge,RidgeCV,ElasticNet,ElasticNetCV
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler,MinMaxScaler
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor, AdaBoostRegressor, GradientBoostingRegressor
from sklearn.model_selection import RandomizedSearchCV
from sklearn.tree import DecisionTreeRegressor
import xgboost as xgb
from scipy.stats import uniform, randint
from sklearn.pipeline import make_pipeline

from pymongo import UpdateOne


class train_model():
    def __init__(self,i):
        self.i = i
        self.features = features
        self.today = date.today() - timedelta(self.i)
        self.yest = self.today - timedelta(1)
        self.tom = self.today + timedelta(1)

        self.today_ymd = self.today.strftime('%Y-%m-%d')
        self.yest_ymd = self.yest.strftime('%Y-%m-%d')
        self.tom_ymd = self.tom.strftime('%Y-%m-%d')

        self.insertion_db = db_for_insertion
        self.db = db

    def train_model(self):
        try:
            obj = data_transformation(self.i)
            train_df = obj.process_data()
            test_df = obj.process_test_data()
            


            X_train = train_df[features]
            y_train = train_df['demand']
            X_test = test_df[features]


            X_train['is_day'] = np.where((X_train['hour']<7)&(X_train['hour']>21),1,0)
            X_test['is_day'] = np.where((X_test['hour']<7)&(X_test['hour']>21),1,0)

            logging.info('Training and Testing data prepared')

        except Exception as e:
             logging.error(CustomException(e,sys))


        model_dict = {
            
            'Lasso': {
                    'model': make_pipeline(MinMaxScaler(), Lasso(max_iter=10000)),
                    'params': {
                        'lasso__alpha': uniform(0.0001, 0.1)  # Uniform distribution from 0.0001 to 0.1001
                    },
            },
            'Ridge': {
                    'model': make_pipeline(MinMaxScaler(), Ridge(max_iter=10000)),
                    'params': {
                        'ridge__alpha': uniform(0.0001, 1000.0)  # alpha in range [0.0001, 10.0001]
                    },
                },
            

            'RandomForest_ht':{
                 'model':RandomForestRegressor(),
                 'params':{
                    'n_estimators': [100, 200, 300, 500],
                    'max_depth': [None, 10, 20, 30, 50],
                    'min_samples_split': [2, 5, 10],
                    'min_samples_leaf': [1, 2, 4],
                    'max_features': [ 'sqrt', 'log2'],
                    'bootstrap': [True, False]
                     
                 },
             },
             
             'GradientBoost_ht':{
                 'model':GradientBoostingRegressor(),
                 'params':{
                    'n_estimators': [100, 300, 500],
                    'learning_rate': [0.01, 0.05, 0.1],
                    'max_depth': [3, 5, 7],
                    'min_samples_split': [2, 5],
                    'min_samples_leaf': [1, 3],
                    'subsample': [0.8, 1.0],
                    'max_features': ['sqrt', 'log2']
                    }
                 
             },
             'XGBoost_ht':{
                'model':xgb.XGBRegressor(),
                'params':{'n_estimators': randint(100, 500),
                'learning_rate': uniform(0.01, 0.2),
                'max_depth': randint(3, 10),
                'subsample': uniform(0.5, 0.5),
                'colsample_bytree': uniform(0.5, 0.5),
                'reg_alpha': uniform(0, 1),
                'reg_lambda': uniform(0, 1)},

            },
        }

        forecast_master = pd.DataFrame()
        for model_name in model_dict.keys():
            try:
                model_class = model_dict[model_name]['model']
                model_params = model_dict[model_name]['params']

                random_cv = RandomizedSearchCV(model_class,param_distributions=model_params,cv=5,scoring='neg_mean_squared_error',n_iter=20,error_score='raise')
                random_cv.fit(X_train,y_train)
                y_pred = random_cv.best_estimator_.predict(X_test)

                forecast_df = pd.DataFrame()
                forecast_df['forecast'] = y_pred
                forecast_df['forecast'] = round(forecast_df['forecast'],4)
                forecast_df['date'] = self.tom_ymd
                forecast_df['hour'] = np.arange(1,25,1)
                forecast_df['category'] = model_name
                forecast_df['inserted_date'] = datetime.now()

                self.bulk_upsert(self.insertion_db,forecast_df,['category','date','hour'])
                logging.info(f'{model_name} done for {self.tom_ymd}')

                forecast_master = pd.concat([forecast_master,forecast_df])
            
            except Exception as e:
                 logging.error(CustomException(e,sys))

    def bulk_upsert(self,collection_name, df, key_names):
            """
            Perform bulk upsert in MongoDB based on specified key names.

            Args:
            - collection_name: Name of the MongoDB collection to perform upsert operation.
            - df: DataFrame containing data to be upserted.
            - key_names: List of key names based on which the upsert operation will be performed.
            """
            client = MongoClient(mongo_url)
            db1 = client[self.db]
            # collection = database["uk_testforecast_96"]
            try:
                # Convert DataFrame to dictionary for bulk upsert
                data_dict = df.to_dict(orient='records')

                # Construct list of update operations using UpdateOne
                update_operations = [
                    UpdateOne(
                        {key: data[key] for key in key_names},  # Filter based on key_names
                        {'$set': data},  # Set entire record
                        upsert=True  # Upsert if record does not exist
                    )
                    for data in data_dict
                ]

                # Assuming self.db is the MongoDB database object
                collection = db1[collection_name]

                # Perform bulk upsert using bulk_write
                result = collection.bulk_write(update_operations)
                logging.info("Bulk upsert successful:")
            except Exception as e:
                logging.error(CustomException(e,sys))
            
                