#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 11:42:55 2024

@author: simran28
"""


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import requests
import pandas as pd
import time
import logging
import mysql.connector
from mysql.connector import Error

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Constants
BASE_URL = "https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
COUNTRIES = [
    "ARG", "AUS", "BRA", "CAN", "CHN", "FRA", "DEU", "IND", "IDN", "ITA",
    "JPN", "MEX", "RUS", "SAU", "ZAF", "KOR", "TUR", "GBR", "USA", "EUU"
]  # G20 countries (EU represented as 'EUU')

INDICATORS = [
    "NY.GDP.MKTP.CD", "SP.POP.TOTL", "EN.ATM.CO2E.PC", "SP.DYN.LE00.IN",
    "SE.PRM.ENRR", "SL.UEM.TOTL.ZS", "EG.USE.PCAP.KG.OE", "BX.KLT.DINV.WD.GD.ZS",
    "IT.NET.USER.ZS", "SH.STA.MMRT", "SH.XPD.CHEX.GD.ZS", "AG.LND.FRST.ZS",
    "NY.GDP.PCAP.CD", "SP.POP.GROW", "NV.IND.MANF.ZS"
]
START_DATE = "2000"
END_DATE = "2023"

# MySQL Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'skalda',      # Change to your MySQL username
    'password': 'Jul2807$',  # Change to your MySQL password
    'database': 'WorldBank_data'
}

# Mapping World Bank indicator codes to SQL table column names
INDICATOR_MAPPING = {
    "NY.GDP.MKTP.CD": "gdp",
    "SP.POP.TOTL": "population",
    "EN.ATM.CO2E.PC": "co2_emissions",
    "SP.DYN.LE00.IN": "life_expectancy",
    "SE.PRM.ENRR": "school_enrollment",
    "SL.UEM.TOTL.ZS": "unemployment",
    "EG.USE.PCAP.KG.OE": "energy_use",
    "BX.KLT.DINV.WD.GD.ZS": "fdi",
    "IT.NET.USER.ZS": "internet_users",
    "SH.STA.MMRT": "maternal_mortality",
    "SH.XPD.CHEX.GD.ZS": "health_expenditure",
    "AG.LND.FRST.ZS": "forest_area",
    "NY.GDP.PCAP.CD": "gdp_per_capita",
    "SP.POP.GROW": "population_growth",
    "NV.IND.MANF.ZS": "manufacturing"
}

# Function to extract data from the World Bank API
def extract_data(country_code, indicators, start_date, end_date):
    all_data = []
    for indicator in indicators:
        url = BASE_URL.format(country_code=country_code, indicator=indicator)
        params = {"format": "json", "date": f"{start_date}:{end_date}", "per_page": 1000}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if len(data) > 1:
                all_data.extend(data[1])
                logger.info(f"Data fetched for {country_code} - {indicator}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {country_code}, {indicator}: {e}")
        time.sleep(0.1)
    return all_data

# Function to transform and clean the data
def transform_data(raw_data):
    processed_data = []
    for entry in raw_data:
        if entry["value"] is not None:
            processed_data.append({
                "country": entry["country"]["value"],
                "country_code": entry["countryiso3code"],
                "indicator": entry["indicator"]["id"],  # Use the indicator ID for mapping
                "year": entry["date"],
                "value": entry["value"]
            })
    df = pd.DataFrame(processed_data)

    # Pivot the table
    df_pivot = df.pivot_table(index=["country", "country_code", "year"], 
                              columns="indicator", values="value").reset_index()

    # Rename columns using the indicator mapping
    df_pivot.rename(columns=INDICATOR_MAPPING, inplace=True)
    return df_pivot

# Function to connect to MySQL and create database & table if not exists
def create_database_and_table():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS WorldBank_data")
        cursor.execute("USE WorldBank_data")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS indicators (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country VARCHAR(100),
                country_code VARCHAR(10),
                year INT,
                gdp DOUBLE,
                population BIGINT,
                co2_emissions DOUBLE,
                life_expectancy DOUBLE,
                school_enrollment DOUBLE,
                unemployment DOUBLE,
                energy_use DOUBLE,
                fdi DOUBLE,
                internet_users DOUBLE,
                maternal_mortality INT,
                health_expenditure DOUBLE,
                forest_area DOUBLE,
                gdp_per_capita DOUBLE,
                population_growth DOUBLE,
                manufacturing DOUBLE
            )
        """)
        connection.commit()
        logger.info("Database and table created successfully.")
    except Error as e:
        logger.error(f"Error creating database or table: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# Function to load data into MySQL database
def load_data_to_mysql(df):
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # Prepare the SQL INSERT query
        insert_query = """
            INSERT INTO indicators (
                country, country_code, year, gdp, population, co2_emissions, life_expectancy,
                school_enrollment, unemployment, energy_use, fdi, internet_users, maternal_mortality,
                health_expenditure, forest_area, gdp_per_capita, population_growth, manufacturing
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Insert each row into the database
        for _, row in df.iterrows():
            data_tuple = tuple(
                None if pd.isna(row.get(col)) else row.get(col)
                for col in [
                    "country", "country_code", "year", "gdp", "population", "co2_emissions",
                    "life_expectancy", "school_enrollment", "unemployment", "energy_use",
                    "fdi", "internet_users", "maternal_mortality", "health_expenditure",
                    "forest_area", "gdp_per_capita", "population_growth", "manufacturing"
                ]
            )
            cursor.execute(insert_query, data_tuple)

        connection.commit()
        logger.info("New data appended to MySQL database successfully.")

    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

# Main workflow
if __name__ == "__main__":
    create_database_and_table()
    all_raw_data = []
    for country in COUNTRIES:
        raw_data = extract_data(country, INDICATORS, START_DATE, END_DATE)
        all_raw_data.extend(raw_data)
    transformed_data = transform_data(all_raw_data)
    load_data_to_mysql(transformed_data)
    
    
    
