import yaml
import psycopg2
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
    '''This class contains methods for connecting and uploading to a database'''
    
    def read_db_creds(self, file_name):
        """
        Reads the database credentials from a YAML file.

        Args:
            file_name (str): The name of the YAML file containing database credentials.

        Returns:
            dict: A dictionary containing database credentials.
        """
        with open(file_name, "r") as file:
            try:
                credentials = yaml.safe_load(file)
                print(credentials)
                return credentials
            except yaml.YAMLError as e:
                print(e)

    def init_db_engine(self, credentials):
        """
        Initializes and returns a SQLAlchemy database engine.

        Args:
            credentials (dict): Dictionary containing database connection details.

        Returns:
            sqlalchemy.engine: An SQLAlchemy engine object.
        """
        engine = create_engine(
            f"postgresql+psycopg2://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        )
        return engine

    def list_db_tables(self, engine):
        """
        Retrieves a list of table names in the connected database.

        Args:
            engine (sqlalchemy.engine.Engine): SQLAlchemy engine object.

        Returns:
            list: A list of table names in the connected database.
        """
        Inspector = inspect(engine)
        return Inspector.get_table_names()

    def upload_to_db(self, engine, df, table_name):
        """
        Uploads a Pandas DataFrame to the specified database table.

        Args:
            engine (sqlalchemy.engine): SQLAlchemy engine object.
            df (pandas.DataFrame): Pandas DataFrame to be uploaded.
            table_name (str): Name of the table.

        Returns:
            None
        """
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
