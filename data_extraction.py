import pandas as pd
import tabula as tb 
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
        
if __name__ == "__main__":
    
    data_extractor = DataExtractor()
    
    data_cleaner = DataCleaning()
    
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

    pdf_data = data_extractor.retrieve_pdf_data(pdf_link)
    
    cleaned_pdf = data_cleaner.clean_card_details(pdf_data)
    
    if pdf_data is not None:
        print(pdf_data)
        print(pdf_data.info())
        print(f"cleaned pdf{cleaned_pdf}")
        
    connector =  DatabaseConnector()
    local_creds = connector.read_db_creds("db_creds_local.yaml")
    local_engine = connector.init_db_engine(local_creds)
    local_engine.connect()
    
    try:
        connector.upload_to_db(local_engine, cleaned_pdf, "dim_card_details")
    except Exception as e:
        print(f"failed to upload to db: {e}")
    
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
