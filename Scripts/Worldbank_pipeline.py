import requests
import pandas as pd
import time
import logging
import mysql.connector
from mysql.connector import Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

BASE_URL = "https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
COUNTRIES = [
    "ARG", "AUS", "BRA", "CAN", "CHN", "FRA", "DEU", "IND", "IDN", "ITA",
    "JPN", "MEX", "RUS", "SAU", "ZAF", "KOR", "TUR", "GBR", "USA", "EUU",
    "AFG", "BGD", "TCD", "ETH", "HTI", "NPL", "NER", "SOM", "SDN", "ZWE"  
]  

INDICATORS = [
    "NY.GDP.MKTP.CD", "SP.POP.TOTL", "EN.ATM.CO2E.PC", "SP.DYN.LE00.IN",
    "SE.PRM.ENRR", "SL.UEM.TOTL.ZS", "EG.USE.PCAP.KG.OE", "BX.KLT.DINV.WD.GD.ZS",
    "IT.NET.USER.ZS", "SH.STA.MMRT", "SH.XPD.CHEX.GD.ZS", "AG.LND.FRST.ZS",
    "NY.GDP.PCAP.CD", "SP.POP.GROW", "NV.IND.MANF.ZS"
]
START_DATE = "2000"
END_DATE = "2023"

DB_CONFIG = {
    'host': 'localhost',
    'user': 'skalda',      
    'password': 'Jul2807$',  
    'database': 'WorldBank_data'
}

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

def extract_data(country_code, indicators, start_date, end_date):

    all_data = []
    
    for indicator in indicators:
        
        url = BASE_URL.format(country_code=country_code, indicator=indicator)
        
        params = {"format": "json","date": f"{start_date}:{end_date}", "per_page": 1000}
        
        try:
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if len(data) > 1:
                all_data.extend(data[1])
                logger.info(f"Data fetched for {country_code} - {indicator}")
                
            
        except requests.exceptions.RequestException as e:
            
            logger.error(f"Error fetching data for {country_code}, {indicator}: {e}")

        time.sleep(0.1) # AVOID API LIMITS
    
    
    return all_data

def transform_data(raw_data):
    
    processed_data = []
    
    for entry in raw_data:
        
        if entry["value"] is not None:
            processed_data.append({"country": entry["country"]["value"],
                                   "country_code": entry["countryiso3code"],
                                   "indicator": entry["indicator"]["id"],  # Use the indicator ID for mapping
                                   "year": entry["date"],
                                   "value": entry["value"]})
    
    
    df = pd.DataFrame(processed_data)
    
    df_pivot = df.pivot_table(index=["country", "country_code", "year"], columns="indicator", values="value").reset_index()
    
    df_pivot.rename(columns=INDICATOR_MAPPING, inplace=True)
    
    return df_pivot


def perform_data_quality_checks(df):
    
    logger.info("Data quality checks")
    
    
    # DUPLICATES
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        logger.warning(f"Data contains {duplicate_count} duplicate records")
        df = df.drop_duplicates()
    else:
        logger.info("No duplicate records found.")
    
    
    # MISSING
    missing_count = df.isnull().sum()
    if missing_count.sum() > 0:
        logger.warning(f"Data contains missing values:\n{missing_count}")
        df = df.fillna(0) 
    else:
        logger.info("No missing values detected.")
    

    # INVALID
    invalid_columns = [col for col in df.columns if df[col].dtypes not in ["float64", "int64", "object"]]
    if invalid_columns:
        
        logger.warning(f"Columns with unexpected data types: {invalid_columns}")
        
        for col in invalid_columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except Exception as e:
                logger.error(f"Failed to convert column {col}: {e}")
              
    numeric_columns = df.select_dtypes(include=["float64", "int64"]).columns
    for column in numeric_columns:
        logger.info(f"Rounding column '{column}' to two decimal places.")
        df[column] = df[column].round(2)

    numeric_columns = df.select_dtypes(include=["float64", "int64"]).columns
    for column in numeric_columns:
        
        if df[column].isnull().all():  
            continue

        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1
        outliers = df[(df[column] < q1 - 1.5 * iqr) | (df[column] > q3 + 1.5 * iqr)]
        if not outliers.empty:
            logger.warning(f"Column {column} has {len(outliers)} outliers.")
    
    logger.info("Data quality checks completed.")
    return df

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

def load_data_to_mysql(df):
    connection = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        insert_query = """
            INSERT INTO indicators (
                country, country_code, year, gdp, population, co2_emissions, life_expectancy,
                school_enrollment, unemployment, energy_use, fdi, internet_users, maternal_mortality,
                health_expenditure, forest_area, gdp_per_capita, population_growth, manufacturing
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

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

if __name__ == "__main__":
    create_database_and_table()
    all_raw_data = []
    for country in COUNTRIES:
        raw_data = extract_data(country, INDICATORS, START_DATE, END_DATE)
        all_raw_data.extend(raw_data)
    transformed_data = transform_data(all_raw_data)
    cleaned_data = perform_data_quality_checks(transformed_data)
    load_data_to_mysql(cleaned_data)
 
