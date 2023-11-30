import pandas as pd
import tabula as tb 
import requests
import boto3
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning


class DataExtractor:
    ''' This class serves as a utility class holding methods to extract data from external sources '''
    
    def read_rds_table(self, engine, table_name):
        """Reads a table from a database and extracts it into a Pandas DataFrame.

        Args:
            engine (sqlalchemy.engine): The SQLAlchemy engine used to connect to the database.
            table_name (str): The name of the table to be extracted.

        Returns:
            pandas.DataFrame: A DataFrame containing the data from the specified table.
        """
        with engine.begin() as conn:
            return pd.read_sql_table(table_name, conn)
        
    def retrieve_pdf_data(self, pdf_link):
        """Pass a link to a pdf file to this function to then extract the tables into a pandas DataFrame.
        
        Args:
            pdf_link (str): The URL or local path to the PDF file.

        Returns:
            pandas.DataFrame: A DataFrame containing the extracted tables from the PDF.
        """
        try:
            tables = tb.read_pdf(pdf_link, pages='all', multiple_tables=True)

            df = pd.concat(tables)
            
            return df
        except Exception as e:
            print(f"An error occurred during PDF extraction: {e}")
            
    def _api_key(self):
        """Returns the API key as a dictionary for authentication.

        Returns:
            dict: A dictionary containing the API key.
        """
        return {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    
    def list_number_of_stores(self):
        """Fetches the number of stores from an API
        
        Returns: 
            int or None: The number of stores, or None if an error occurs
        """
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
        """Retrieves the store details from an API and returns the data as a pandas DataFrame
        
        Returns: pandas.DataFrame: A DataFrame containing store details. 
        """
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
        """Downloads a file from an s3 bucket and reads it into a pandas DataFrame
        
        Args: 
            bucket (str): The name of the s3 bucket.
            obj_name (str): The name of the object in the s3 bucket.
            file_loc (str): The local file location to save the download to.
            
        Returns: 
            pandas.DataFrame: A pandas DataFrame containing the data from the downloaded file.
        """
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
        """Displays the contents of a CSV file from a given file location
        
        Args:
            extract (str): The local file location of the CSV file. 
        
        Returns: 
            pandas.DataFrame: A DataFrame containing the data from the CSV file.
        """
        df = pd.read_csv(extract)
        print(df)
        return df
        
    def extract_orders_table(self):
        """Extracts the orders_table from RDS database and returns a pandas DataFrame
        
        Returns: 
            pandas.DataFrame: A DataFrame containing the data from "orders_table"
        """
        connector = DatabaseConnector()
        creds = connector.read_db_creds('db_creds.yaml')
        engine = connector.init_db_engine(creds)
        engine.connect()
        df = self.read_rds_table(engine, table_name='orders_table')
        engine.dispose()
        print(f"orders table: ", df)
        return df