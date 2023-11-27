import pandas as pd
import tabula as tb 
import requests
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
            
    def api_key(self):
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
            
        
        
if __name__ == "__main__":
    
    data_extractor = DataExtractor()
    
    # data_extractor.list_number_of_stores()
    
    # data_extractor.retrieve_stores_data()
    
    data_cleaner = DataCleaning()
    
    store_data = data_extractor.retrieve_stores_data()
    
    cleaned = data_cleaner.clean_store_data(store_data)
    
    # print(cleaned)
    
    # pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

    # pdf_data = data_extractor.retrieve_pdf_data(pdf_link)
    
    # cleaned_pdf = data_cleaner.clean_card_details(pdf_data)
    
    # if pdf_data is not None:
    #     print(pdf_data)
    #     print(pdf_data.info())
    #     print(f"cleaned pdf{cleaned_pdf}")
        
    connector =  DatabaseConnector()
    local_creds = connector.read_db_creds("db_creds_local.yaml")
    local_engine = connector.init_db_engine(local_creds)
    local_engine.connect()
    
    try:
        connector.upload_to_db(local_engine,cleaned, "dim_store_details")
        print("succesfully uploaded to db")
    except Exception as e:
        print(f"failed to upload to db: {e}")
    
    # try:
    #     connector.upload_to_db(local_engine, cleaned_pdf, "dim_card_details")
    # except Exception as e:
    #     print(f"failed to upload to db: {e}")
    
    # connector = DatabaseConnector()
    # credentials = connector.read_db_creds("db_creds.yaml")
    # engine = connector.init_db_engine(credentials)
    # data_extractor = DataExtractor()
    # data_cleaner = DataCleaning()

    # local_creds = connector.read_db_creds("db_creds_local.yaml")
    # local_engine = connector.init_db_engine(local_creds)
    # local_engine.connect()

    # table_names = connector.list_db_tables(engine)
    # print("Available tables:", table_names)
    
    

    # try:
    #     for table_name in table_names:
    #         if "legacy_users" in table_name.lower():
    #             df = data_extractor.read_rds_table(engine, table_name)
    #             if df is not None:
    #                 print(f"(OG) table name is: {table_name}\n", df)

    #                 df = data_cleaner.clean_user_data(df)

    #                 print(f"Cleaned data from table '{table_name}':\n", df)

    #                 connector.upload_to_db(local_engine, df, "dim_users")
    # except Exception as e:
    #     print(f"An error occurred during the process: {e}")
    # finally:
    #     local_engine.dispose()
