# Multinational Retail Data Centralisation

This project aims to enhance the accessability and analysability of a Multinational company's sales data. The data is currently scattered across different sources, which causes problems in effective decision making due to hindering efficient analysis.

#### Main objective
- create a centralised database system that consolidates the company's sales data acting as a singular source of truth. 

The purpose of streamlining access to this data is because this will empower the organization in making informed and data-driven decisions. 

## File Structure

- data_cleaning.py - This script contains a class 'DataCleaning' with methods to clean the data from each of the data sources.
- database_utils.py - This script contains a class called 'DatabaseConnector' which is used to connected to and upload data to the database. 
- data_extraction.py - This script contains a class 'DataExtractor'. This class acts as a utility class in which the methods within help extract data from different data sources. These are sources such as: CSV files, APIs and an S3 bucket. 

### License information

tbc