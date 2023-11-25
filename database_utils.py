import yaml
import psycopg2
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
    def read_db_creds(self, file_name):
        with open(file_name, "r") as file:
            try:
                credentials = yaml.safe_load(file)
                print(credentials)
                return credentials
            except yaml.YAMLError as e:
                print(e)

    def init_db_engine(self, credentials):
        engine = create_engine(
            f"postgresql+psycopg2://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        )
        return engine

    def list_db_tables(self, engine):
        Inspector = inspect(engine)
        return Inspector.get_table_names()

    def upload_to_db(self, engine, df, table_name):
        return df.to_sql(table_name, engine, if_exists="replace", index=False)

if __name__ == "__main__":
    connector = DatabaseConnector()
    creds = connector.read_db_creds("db_creds.yaml")
    engine = connector.init_db_engine(creds)
    engine.connect()
    print("Connection successful")
    print(engine)
    tables_list = connector.list_db_tables(engine)
    print(tables_list)
    # ['legacy_store_details', 'legacy_users', 'orders_table']
