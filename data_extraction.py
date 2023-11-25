import pandas as pd
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning


class DataExtractor:
    def read_rds_table(self, engine, table_name):
        with engine.begin() as conn:
            return pd.read_sql_table(table_name, conn)


if __name__ == "__main__":
    connector = DatabaseConnector()
    credentials = connector.read_db_creds("db_creds.yaml")
    engine = connector.init_db_engine(credentials)
    data_extractor = DataExtractor()
    data_cleaner = DataCleaning()

    local_creds = connector.read_db_creds("db_creds_local.yaml")
    local_engine = connector.init_db_engine(local_creds)
    local_engine.connect()

    table_names = connector.list_db_tables(engine)
    print("Available tables:", table_names)

    try:
        for table_name in table_names:
            if "legacy_users" in table_name.lower():
                df = data_extractor.read_rds_table(engine, table_name)
                if df is not None:
                    print(f"(OG) table name is: {table_name}\n", df)

                    df = data_cleaner.clean_user_data(df)

                    print(f"Cleaned data from table '{table_name}':\n", df)

                    connector.upload_to_db(local_engine, df, "dim_users")
    except Exception as e:
        print(f"An error occurred during the process: {e}")
    finally:
        local_engine.dispose()
