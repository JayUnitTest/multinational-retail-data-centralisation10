import pandas as pd
import numpy as np
import re


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
