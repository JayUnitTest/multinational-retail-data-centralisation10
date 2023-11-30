# Multinational Retail Data Centralisation

This project is dedicated to improving the accessibility and analyzability of a multinational company's sales data. Presently, the data is dispersed across various sources, posing challenges to efficient decision-making and hindering comprehensive analysis.

#### Main objective
- The primary goal of this project is to establish a centralized database system that consolidates the company's sales data, serving as a unified source of truth. 

Access to this consolidated data is pivotal. This will empower the organization to make informed, data-driven decisions. Furthermore, the centralization process will enhance the overall data management and analytical capabilities of the company.

## File Structure

- data_cleaning.py - This script contains a class 'DataCleaning' with methods to clean the data from each of the data sources.
- database_utils.py - This script contains a class called 'DatabaseConnector' which is used to connect to and upload data to the database. 
- data_extraction.py - This script contains a class 'DataExtractor'. This class serves as a utility class in which the methods within help extract data from different data sources. These are sources such as: CSV files, APIs and an S3 bucket. 

## Data Sources

The data consolidation process involves extracting information from various sources to establish a comprehensive and unified data repository.

1. AWS RDS Database
- Source Data: Historical sales and user data stored in an AWS RDS database.
- Extraction Method: Utilized methods in the data_extraction and database_utils classes.
- Tables Extracted:
 orders_table, dim_users
2. AWS S3 Bucket
- Source Data: Products data saved as a CSV file in an AWS S3 bucket.
- Extraction Method: Leveraged boto3 for downloading and extraction. This is then turned into a Pandas DataFrame.
- Tables Extracted: dim_products, dim_date_times
3. AWS Link (PDF)
- Source Data: PDF file stored in an AWS S3 bucket.
- Extraction Method: Utilized tabula to read tables from the PDF and convert them into a pandas DataFrame.
- Tables Extracted: dim_card_details
4. RESTful API
- Source Data: Store data retrieved from an API endpoint.
- Extraction Method: HTTP GET requests to the API, followed by normalization of the originally received JSON data. 
- Tables Extracted: dim_store_details

This approach centralizes data from multiple origins, providing a cohesive foundation for comprehensive analysis and reporting.

## Installation 

### Technologies Used
- Python (Pandas, NumPy)
- PostgreSQL
- AWS (boto3)
- SQLalchemy
- psycopg2
- tabula-py
- YAML (library)

To execute files from this project, run the following command in the terminal: 
```
python {filename.py}
```

The project utilizes function calls under the "if \_\_name\_\_ == '\_\_main\_\_'" block. Alternatively, you can create a separate main file to invoke specific functions like database uploads and data extraction.

### License information

MIT License

Copyright (c) 2023 Jay Singh

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.