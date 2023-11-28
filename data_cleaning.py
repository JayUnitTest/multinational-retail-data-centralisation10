import pandas as pd
import numpy as np
import re
from datetime import datetime


class DataCleaning:
    def clean_user_data(self, df):
        df = self.clean_user_date_errors(df, "date_of_birth")
        df = self.clean_user_date_errors(df, "join_date")
        df = self.clean_user_data_types_to_string(df, "first_name")
        df = self.clean_user_data_types_to_string(df, "last_name")
        df = self.clean_missing_values_nan_nulls(df)
        df = self.clean_duplicated_data(df, "user_uuid")
        df = self.clean_user_phone_numbers(df, "phone_number")
        df = self.valid_email_address(df, 'email_address')
        # print(df)
        return df

    def clean_user_data_types_to_string(self, df, column_name):
        df[column_name] = df[column_name].astype("string")
        return df

    def valid_email_address(self, df, column_name):
        regex = re.compile(
            r"([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+"
        )
        for i, email in enumerate(df[column_name]):
            if not re.fullmatch(regex, email):
                df.loc[i, column_name] = np.nan

        df.dropna(subset=[column_name], inplace=True)

        return df

    def clean_duplicated_data(self, df, column_name):
        df[column_name] = df[column_name].drop_duplicates()
        return df

    def clean_missing_values_nan_nulls(self, df):
        df = df.fillna(np.nan)
        df = df.replace("NULL", np.nan)
        df = df.dropna()
        return df

    def clean_user_phone_numbers(self, df, column_name):
        uk_regex_expression = "^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$"
        international_regex = r"^\+(?:[0-9] ?){6,14}[0-9]$"
        df.loc[
            ~df[column_name].str.match(uk_regex_expression)
            & ~df[column_name].str.match(international_regex),
            column_name,
        ] = np.nan
        return df

    def clean_user_date_errors(self, df, column_name):
        df[column_name] = pd.to_datetime(
            df[column_name], format="%Y-%m-%d", errors="ignore"
        )
        df[column_name] = pd.to_datetime(
            df[column_name], format="%B-%Y-%d", errors="ignore"
        )
        df[column_name] = pd.to_datetime(
            df[column_name], format="%Y-%B-%d", errors="ignore"
        )
        df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
        df.dropna(subset=column_name, how="any", inplace=True)
        return df
    
    # def check_for_expired_cards(self,column_name,df):
    #     df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
    #     current_date = pd.to_datetime('now')
    #     df['card_status'] = pd.cut(df[column_name], dtype=)
        
    def clean_card_details(self, df):
        self.clean_user_date_errors(df, 'date_payment_confirmed')
        self.clean_missing_values_nan_nulls(df)
        self.clean_duplicated_data(df, 'card_number')
        self.clean_user_data_types_to_string(df, 'card_number')
        return df
        
    def clean_store_data(self, df):
        self.clean_missing_values_nan_nulls(df)
        self.clean_user_date_errors(df, 'opening_date')
        df['longitude'] = pd.to_numeric(df['longitude'], errors="coerce")
        df['latitude'] = pd.to_numeric(df['latitude'], errors="coerce")
        df['latitude'].fillna(df['latitude'].mean(), inplace=True)
        df['longitude'].fillna(df['longitude'].mean(), inplace=True)
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: re.sub(r'\D', '', str(x))) 
        df['staff_numbers'] = df['staff_numbers'].astype(int)
        df.drop(columns='lat', inplace=True)
        return df
    
    def convert_products_weights(self, df, column_name):
        units_regex = r'(\d+\.\d+|\d+)(kg|g|ml)'
        df[['weight', 'unit']] = df['weight'].str.extract(units_regex)
        df['weight'].dropna(inplace = True)
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce').astype('float')
        df[['weight', 'unit']] = df.apply(self.convert_units_to_kg, axis=1, result_type='expand')
        print(df[['product_name', 'weight', 'unit']])
        
    def convert_units_to_kg(self, row):
        if pd.notna(row['weight']):
            if row['unit'] == 'kg':
                return row['weight'], 'kg'
            elif row['unit'] == 'g':
                return row['weight'] / 1000, 'kg'
            elif row['unit'] == 'ml':
                return row['weight'] / 1000, 'kg'
        return None, None
    
    def clean_products_data(self, df):
        df.dropna(how='any', inplace = True)
        df.drop(columns = 'Unnamed: 0', inplace = True)
        self.clean_user_date_errors(df, 'date_added')
        self.clean_duplicated_data(df, 'product_code')
        df = df[['product_name', 'product_price', 'weight', 'unit', 'category', 'EAN', 'date_added', 'uuid', 'removed', 'product_code']]
        return df
    
    def clean_orders_data(self, df):
        df.drop(columns='first_name', inplace=True)
        df.drop(columns='last_name', inplace = True)
        df.drop(columns='1', inplace=True)
        df.drop(columns='level_0', inplace=True)
        df.dropna(inplace=True)
        return df
    
    def clean_date_times_s3(self, df):
        df = pd.read_json(df, convert_dates={'timestamp' : 'datetime64[ns]'})
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors= 'coerce')
        df['timestamp'] = df['timestamp'].dt.time
        df['year'] = df['year'].astype(str) 
        df['month'] = df['month'].astype(str).str.zfill(2)
        df['day'] = df['day'].astype(str).str.zfill(2) 
        df['date'] = df['year'] + '-' + df['month'] + '-' + df['day']
        self.clean_user_date_errors(df, 'date')
        self.clean_missing_values_nan_nulls(df)
        df['date'] = df['date'].dt.date
        df = df[['date','timestamp', 'day', 'month', 'year','time_period', 'date_uuid']]
        return df
        
             
        
