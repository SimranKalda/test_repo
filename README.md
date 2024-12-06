# TradeDataAnalysis
**World Bank Data ETL Pipeline with Airflow**

An ETL pipeline utilizing Apache Airflow to automate the extraction, transformation, and loading of global economic and environmental data from the World Bank API into a MySQL database.

### Detailed Description:
This project implements an ETL (Extract, Transform, Load) pipeline that automates the process of fetching, cleaning, and storing global economic and environmental data from the World Bank API into a MySQL database. It leverages Apache Airflow for orchestration and scheduling of the ETL workflow. The pipeline retrieves data on various indicators such as GDP, population, CO2 emissions, and more, spanning from 2000 to 2023 for multiple countries.

**Key technical features**:

1. **Data Extraction**:
   - Extracts data from the World Bank API for various indicators (e.g., GDP, life expectancy, unemployment rate) for 20+ countries, including the USA, China, Germany, and others.
   - The extraction process is automated using Airflow tasks that pull data from the API in parallel, optimizing the ETL process.

2. **Data Transformation**:
   - The extracted data is cleaned and transformed using Python's pandas library. This includes:
     - **Pivoting**: Converts data into a more usable format by pivoting on multiple columns.
     - **Missing Value Handling**: Identifies and processes missing values.
     - **Duplicate Removal**: Removes duplicate rows to ensure data integrity.
     - **Outlier Detection**: Uses statistical methods (e.g., IQR) to identify and flag outliers in the dataset.

3. **Data Quality Checks**:
   - The pipeline includes robust data validation to ensure the integrity of the data before it is loaded into the database:
     - **Missing values**: Rows with excessive missing data are filtered out.
     - **Duplicates**: Duplicate records are removed, and unique data is ensured.
     - **Outliers**: Statistical outliers are flagged using quantile-based methods (IQR).
   
4. **Airflow Orchestration**:
   - Apache Airflow is used to orchestrate and automate the ETL tasks:
     - **Task Scheduling**: Airflow ensures the ETL pipeline runs at specific intervals, which can be configured for daily, weekly, or custom schedules.
     - **Parallel Execution**: Airflow executes tasks in parallel, allowing the extraction process for different countries and indicators to run concurrently, optimizing runtime.
     - **Logging & Monitoring**: Airflow's web interface provides a detailed view of task execution, logs, and potential issues, improving the monitoring and troubleshooting of the pipeline.

5. **Database Integration**:
   - The cleaned and transformed data is loaded into a MySQL database with a well-defined schema:
     - Data columns include: country, country code, year, GDP, population, CO2 emissions, life expectancy, school enrollment rate, unemployment rate, and more.
     - The database ensures the data is stored efficiently for further analysis and visualization.

6. **Logging and Error Handling**:
   - Throughout the pipeline, detailed logging is used to track task statuses, errors, and warnings. This logging is especially useful for debugging, ensuring that any issues with data extraction or transformation are quickly identified.

**Technologies Used**:
- **Apache Airflow**: Orchestration and scheduling of the ETL pipeline.
- **Python**: For data extraction, transformation, and loading processes.
- **Pandas**: Data manipulation and cleaning.
- **Requests**: API calls to fetch data from the World Bank API.
- **MySQL**: Database management to store the extracted data.
- **Logging**: Python's logging module for tracking the pipeline’s activity.

#### How to Run:
1. Install the required dependencies:
    ```bash
    pip install requests pandas mysql-connector-python apache-airflow
    ```
   
2. Set up your MySQL database and update the `DB_CONFIG` in the script with your MySQL credentials.

3. Initialize the Airflow database:
    ```bash
    airflow db init
    ```

4. Define your Airflow DAGs (Direct Acyclic Graphs) in the `dags/` directory for task scheduling and execution.

5. Trigger the ETL pipeline manually or set up a cron-like schedule in Airflow for automatic execution:
    ```bash
    airflow dags trigger <dag_id>
    ```

This project provides an efficient, automated approach to handling large-scale economic and environmental datasets. By leveraging Airflow for orchestration and combining robust data processing techniques, the pipeline allows users to extract meaningful insights from the World Bank’s data repository, with minimal manual intervention.
