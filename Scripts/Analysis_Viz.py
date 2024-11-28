#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 28 15:09:38 2024

@author: simran28
"""


import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import seaborn as sns

# Step 1: Establish a connection to the MySQL database
connection = mysql.connector.connect(
    host='localhost',  # Replace with your MySQL server host
    user='skalda',  # Replace with your MySQL username
    password='Jul2807$',  # Replace with your MySQL password
    database='WorldBank_data'  # Replace with your database name
)

# Step 2: Pull the data from the table into a pandas DataFrame
query = """
SELECT id, country, country_code, year, population, gdp, co2_emissions, life_expectancy, 
       school_enrollment, unemployment, energy_use, fdi, internet_users, maternal_mortality, 
       health_expenditure, forest_area, gdp_per_capita, population_growth, manufacturing
FROM indicators
"""
data = pd.read_sql(query, connection)

# Step 3: Close the database connection
connection.close()

# Convert columns to appropriate data types
data['co2_emissions'] = pd.to_numeric(data['co2_emissions'], errors='coerce')
data['gdp'] = data['gdp'].astype(float)
data['population'] = data['population'].astype(int)
data['gdp_per_capita'] = data['gdp_per_capita'].astype(float)
data['population_growth'] = data['population_growth'].astype(float)
data['unemployment'] = data['unemployment'].astype(float)
data['country'] = data['country'].astype('string')
data['country_code'] = data['country_code'].astype('string')

# Handle missing values (optional, depending on your data's nature)
data = data.dropna(subset=['gdp_per_capita', 'life_expectancy', 'unemployment', 'school_enrollment', 'maternal_mortality', 'health_expenditure'])

# Check the data info
data.info()

# Step 4: Perform basic statistical analysis
print("Data Overview:")
print(data.describe())
print(data.info())

# Step 5: Data Visualization

# Select only numeric columns for correlation calculation
numeric_data = data.select_dtypes(include=['float64', 'int64'])

# Calculate the correlation matrix
correlation_matrix = numeric_data.corr()

# Print the correlation matrix
print(correlation_matrix)

# 5.1 Distribution of GDP per Capita
plt.figure(figsize=(8, 6))
sns.histplot(data['gdp_per_capita'], kde=True)
plt.title('Distribution of GDP per Capita')
plt.xlabel('GDP per Capita')
plt.ylabel('Frequency')
plt.show()

# 5.2 Distribution of Life Expectancy
plt.figure(figsize=(8, 6))
sns.histplot(data['life_expectancy'], kde=True)
plt.title('Distribution of Life Expectancy')
plt.xlabel('Life Expectancy')
plt.ylabel('Frequency')
plt.show()

# 5.3 Correlation Heatmap
# Create a heatmap of correlations
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Correlation Heatmap')
plt.show()

# 5.4 GDP per Capita vs. Life Expectancy
plt.figure(figsize=(8, 6))
sns.scatterplot(x='gdp_per_capita', y='life_expectancy', data=data)
plt.title('GDP per Capita vs Life Expectancy')
plt.xlabel('GDP per Capita')
plt.ylabel('Life Expectancy')
plt.show()

# 5.5 Country-wise Average GDP per Capita in 2013 (Bar Plot)
data_2013 = data[data['year'] == 2013]
country_gdp = data_2013.groupby('country')['gdp_per_capita'].mean().sort_values(ascending=False)

plt.figure(figsize=(12, 8))
country_gdp.head(10).plot(kind='bar', color='skyblue')
plt.title('Top 10 Countries by Average GDP per Capita in 2013')
plt.xlabel('Country')
plt.ylabel('Average GDP per Capita')
plt.xticks(rotation=45)
plt.show()

# 5.6 Unemployment vs School Enrollment (Scatter Plot)
plt.figure(figsize=(8, 6))
sns.scatterplot(x='school_enrollment', y='unemployment', data=data)
plt.title('School Enrollment vs Unemployment')
plt.xlabel('School Enrollment')
plt.ylabel('Unemployment')
plt.show()

# 5.7 Maternal Mortality vs Health Expenditure (Scatter Plot)
plt.figure(figsize=(8, 6))
sns.scatterplot(x='health_expenditure', y='maternal_mortality', data=data)
plt.title('Maternal Mortality vs Health Expenditure')
plt.xlabel('Health Expenditure')
plt.ylabel('Maternal Mortality')
plt.show()

# Step 6: Optional - Save the plots as images (if needed)
# Uncomment the following lines to save the plots
plt.savefig('gdp_per_capita_distribution.png')
plt.savefig('life_expectancy_distribution.png')


