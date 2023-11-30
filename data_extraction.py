import pandas as pd
import tabula as tb 
import requests
import boto3
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning


class DataExtractor:
    def read_rds_table(self, engine, table_name):
        with engine.begin() as conn:
            return pd.read_sql_table(table_name, conn)
        
    def retrieve_pdf_data(self, pdf_link):
        try:
            tables = tb.read_pdf(pdf_link, pages='all', multiple_tables=True)

            df = pd.concat(tables)
            
            return df
        except Exception as e:
            print(f"An error occurred during PDF extraction: {e}")
            
    def _api_key(self):
        return {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    
    def list_number_of_stores(self):
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        response = requests.get(url, headers=self.api_key())
        try:
            no_of_stores = response.json()['number_stores']
            print(no_of_stores)
            return no_of_stores
        except KeyError as e: 
            print(f"Error: could not find 'number_stores' in response")
            return None
        
    def retrieve_stores_data(self):
        dfs = []
        all_stores = self.list_number_of_stores()
        
        for store_number in range(all_stores):
            url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
            
            try:
                response = requests.get(url, headers=self.api_key())
                response.raise_for_status()
                dfs.append(pd.json_normalize(response.json()))
                # print(f"successfull processed store {store_number}")
            except Exception as e:
                print(f"Error:{store_number}: {e}")
        
        result = pd.concat(dfs)
        # print(result)
        return result
    
    def extract_from_s3(self,bucket, obj_name, file_loc ):
        s3 = boto3.client('s3')
        try:
            s3.download_file(bucket, obj_name, file_loc)
            print(f"download {obj_name} from S3 to {file_loc}")
            df = pd.read_csv(file_loc)
            print(df)
            return df
        except Exception as e:
            print(f"Error downloading {obj_name} from S3: {e}")
    
    def display_s3_extract(self, extract):
        df = pd.read_csv(extract)
        print(df)
        return df
        
    
    def extract_orders_table(self):
        connector = DatabaseConnector()
        creds = connector.read_db_creds('db_creds.yaml')
        engine = connector.init_db_engine(creds)
        engine.connect()
        df = self.read_rds_table(engine, table_name='orders_table')
        engine.dispose()
        print(f"orders table: ", df)
        return df
