#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  9 21:34:36 2025

@author: simran28
"""

import pandas as pd
import matplotlib.pyplot as plt 
import seaborn as sns
import mysql.connector 
import numpy as np

connection = mysql.connector.connect(
    host='localhost',
    user='skalda',
    password='Jul2807$',
    database='WorldBank_data'
)

query = """
SELECT id, country, country_code, year, population, gdp, co2_emissions, life_expectancy, 
       school_enrollment, unemployment, energy_use, fdi, internet_users, maternal_mortality, 
       health_expenditure, forest_area, gdp_per_capita, population_growth, manufacturing
FROM indicators_copy
"""
data = pd.read_sql(query, connection)

connection.close()


data['co2_emissions'] = pd.to_numeric(data['co2_emissions'], errors='coerce')
data['gdp'] = data['gdp'].astype(float)
data['population'] = data['population'].astype(int)
data['gdp_per_capita'] = data['gdp_per_capita'].astype(float)
data['population_growth'] = data['population_growth'].astype(float)
data['unemployment'] = data['unemployment'].astype(float)
data['country'] = data['country'].astype('string')
data['country_code'] = data['country_code'].astype('string')


# Q1
country_classification = {
    "Argentina": "Developing",
    "Australia": "Developed",
    "Brazil": "Developing",
    "Canada": "Developed",
    "China": "Developing",
    "European Union": "Developed",
    "France": "Developed",
    "Germany": "Developed",
    "India": "Developing",
    "Indonesia": "Developing",
    "Italy": "Developed",
    "Japan": "Developed",
    "Korea, Rep.": "Developed",
    "Mexico": "Developing",
    "Russian Federation": "Developing",
    "Saudi Arabia": "Developing",
    "South Africa": "Developing",
    "Turkiye": "Developing",
    "United Kingdom": "Developed",
    "United States": "Developed",
    "Bangladesh" : "Underdeveloped", 
    "Chad" : "Underdeveloped",
    "Ethiopia" : "Underdeveloped",
    "Haiti" : "Underdeveloped",
    "Nepal" : "Underdeveloped",
    "Niger" : "Underdeveloped",
    "Somalia" : "Underdeveloped",
    "Sudan": "Underdeveloped",
    "Zimbabwe": "Underdeveloped"
}

data['Country category'] = data['country'].map(country_classification)

unique_categories = data['Country category'].unique()
markers = ['o', 's', 'D'][:len(unique_categories)]  

# Scatter plot with regression lines
sns.lmplot(
    data=data,
    x='population_growth',
    y='unemployment',
    hue='Country category',
    palette='Set2',
    height=6,
    aspect=1.5,
    markers=markers  # Use the correct number of markers
)


plt.title("Unemployment vs Population Growth by Country Category", fontsize=20)
plt.xlabel("Population Growth (%)", fontsize=12)
plt.ylabel("Unemployment (%)", fontsize=12)
plt.tight_layout()
# plt.legend(title="Category")


plt.savefig("q1_1_v1.pdf", dpi=1200)



# Q2
country_classification = {
    "Argentina": "Developing", "Australia": "Developed", "Brazil": "Developing", 
    "Canada": "Developed", "China": "Developing", "European Union": "Developed", 
    "France": "Developed", "Germany": "Developed", "India": "Developing", 
    "Indonesia": "Developing", "Italy": "Developed", "Japan": "Developed", 
    "Korea, Rep.": "Developed", "Mexico": "Developing", "Russian Federation": "Developing", 
    "Saudi Arabia": "Developing", "South Africa": "Developing", "Turkiye": "Developing", 
    "United Kingdom": "Developed", "United States": "Developed"
}


data['country_category'] = data['country'].map(country_classification)


markers_developed = 'o'  
markers_developing = 's'  
markers_underdeveloped = 'D' 


unique_categories = data['country_category'].unique()


plt.figure(figsize=(14, 8))


sns.lineplot(data=data, x='year', y='gdp', hue='country_category', style='country_category', 
              markers={'Developed': markers_developed, 'Developing': markers_developing,'Underdeveloped':markers_underdeveloped}, palette='tab10')


plt.title("GDP Trends by Country Category", fontsize=25)
plt.xlabel("Year", fontsize=15)
plt.ylabel("GDP ($)", fontsize=15)
plt.legend(title="Country Category", loc='upper left',fontsize=15)
# plt.tight_layout()
plt.savefig("q2_1_v1.pdf", dpi=1200)
plt.show()

plt.figure()
plt.bar(data[data["country"] == "China"]["year"], data[data["country"] == "China"]["gdp"], color="lightskyblue"); plt.xlabel("Year");plt.ylabel("GDP ($)");plt.title("GDP of China",fontsize=14)
plt.savefig("q2_2_v2.pdf", dpi=1200)


top_5_countries = data.groupby('country')['gdp'].sum().nlargest(5).index

# Step 2: Filter the data for the top 5 countries
top_5_data = data[data['country'].isin(top_5_countries)]

# Step 3: Create a line plot
plt.figure(figsize=(14, 8))
sns.lineplot(data=top_5_data, x='year', y='gdp', hue='country', marker='o', palette='tab10')

# Adding titles and labels
plt.title("GDP Trends of Top 5 Countries Over Years", fontsize=25
)
plt.xlabel("Year", fontsize=22)
plt.ylabel("GDP ($)", fontsize=22)
plt.xticks(fontsize=17)
plt.yticks(fontsize=17)

# Save and show the chart
plt.legend(title="Country", fontsize=12)
plt.tight_layout()
# plt.savefig("top_5_countries_gdp_trends_line_chart.pdf", dpi=1200)
plt.show()


# Q3
import numpy

g20_list = list(country_classification.keys())

x2 = {ii1: data[data["country"] == ii1][["health_expenditure", "life_expectancy"]].corr()["health_expenditure"]["life_expectancy"] for ii1 in list(numpy.unique(data["country"])) if ii1 in g20_list}

def plot_sideways_bar_chart(data):
    """
    Create a sideways bar chart from a dictionary with country names as keys
    and values between -1 and 1.
    
    Parameters:
        data (dict): Dictionary with country names as keys and floats as values.
    """
    # Sort the dictionary by values (optional)
    data = dict(sorted(data.items(), key=lambda item: item[1]))

    # Extract keys and values
    countries = list(data.keys())
    values = list(data.values())

    # Create the bar chart
    plt.figure(figsize=(10, 6))
    plt.barh(countries, values, color=['green' if v >= 0 else 'red' for v in values])
    plt.xlabel('Correlation Coefficient (r)',fontsize=14)
    plt.ylabel('Country',fontsize=14)
    plt.title('Correlation Coefficient (Life Expectancy vs. Health Expenditure)',fontsize=18)
    plt.axvline(0, color='black', linewidth=0.8)  # Add a vertical line at 0 for reference
    plt.grid(axis='x', linestyle='--', linewidth=0.5, alpha=0.7)

    # Show the plot
    plt.tight_layout()
    plt.savefig("q3_1_v1.pdf", dpi=1200)
    # plt.show()

plot_sideways_bar_chart(x2)



grouped_by_country = {ii1: data[data["country"]==ii1] for ii1 in list(numpy.unique(data["country"])) if ii1 in g20_list}


plt.figure()
plt.gca().set_facecolor('white')
for key_1 in grouped_by_country:
    
    current_country = key_1
    if current_country in ["Indonesia", "Saudi Arabia"]:
        # plt.figure()
        # plt.bar(grouped_by_country[key_1]["year"], grouped_by_country[key_1]["life_expectancy"])
        # plt.title(key_1 + " Life Expectancy")
        
        # plt.axvline(2007.5, color="r");plt.axvline(2008.5, color="r")
        
        df_now = grouped_by_country[key_1][grouped_by_country[key_1]["year"]<=2021]
        
        # plt.figure()
        plt.plot(df_now["year"], df_now["health_expenditure"])
        
        # plt.title(key_1 + " Health Expenditure")
        
plt.legend(["Indonesia", "Saudi Arabia"])

plt.axvline(2007.5, color="grey");plt.axvline(2008.5, color="grey")
plt.xlabel("Year")
plt.ylabel("Health expenditure ($, in billions)")
plt.title("Health expenditure during 2008 recession")
plt.ylim([1.3,7.1])

#plt.gca().set_facecolor('lightblue')

ax = plt.gca()
for spine in ax.spines.values():
    spine.set_edgecolor('black')  # Set border color
    spine.set_linewidth(1.5)      # Set border thickness
    
plt.savefig("q3_2_v2.pdf", dpi=1200)

def get_health_exp_life_exp_charts(data, countries):
    # Create a single figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))  # 2x2 grid of subplots
    axes = axes.flatten()  # Flatten the axes array for easier indexing
    
    for i, country_name in enumerate(countries):
        ax = axes[i]
        grouped_by_country = {
            ii1: data[data["country"] == ii1][data["year"] < 2022]
            for ii1 in np.unique(data["country"])
            if ii1 in [country_name]
        }
        recollect = pd.concat([grouped_by_country[key_1] for key_1 in grouped_by_country])
        sns.scatterplot(
            x='health_expenditure', 
            y='life_expectancy', 
            data=recollect, 
            hue='year', 
            palette='rainbow', 
            ax=ax
        )
        ax.set_title(country_name)
    
    # Adjust layout to prevent overlap
    plt.tight_layout()
    plt.show()

# Example usage
countries = ["United States", "European Union", "China", "Russian Federation"]
get_health_exp_life_exp_charts(data, countries)

plt.savefig("q3_3_v1.pdf", dpi=1200)


# Q4

data["energy_use_total_ton_oil_eqv"] = data["energy_use"] * data["population"]/1000

# data["energy_use_times_100000"] = data["energy_use"] * 100000

grouped_by_country = {ii1: data[data["country"]==ii1][data["year"]<2015] for ii1 in list(numpy.unique(data["country"])) if ii1 in g20_list}


recollect = pd.concat([grouped_by_country[key_1] for key_1 in grouped_by_country])


plt.figure(figsize=(10, 6))

# Line plot for total energy use vs year
sns.lineplot(x='year', y='energy_use_total_ton_oil_eqv', data=recollect, label='Total Energy Use (ton oil eqv.)', marker='o')

# Line plot for population vs year
sns.lineplot(x='year', y='population', data=recollect, label='Population', marker='o', linestyle='--')

plt.title('Total Energy Use and Population Over Time', fontsize=18)
plt.xlabel('Year', fontsize=12)
plt.ylabel('', fontsize=12)
# plt.ylim([0.8*100000000,4.8*100000000])
# plt.xlim([1999.8,2014.1])

plt.legend(["Total Energy Use (ton oil eq.)", "Total Energy Use CI (ton oil eq.)", "Population", "Population CI"])
# plt.show()
plt.savefig("q4_1_v1.pdf", dpi=1200)

# -------------------------------------------------------------------------


grouped_by_country = {ii1: data[data["country"]==ii1] for ii1 in list(numpy.unique(data["country"])) if ii1 in g20_list}
recollect = pd.concat([grouped_by_country[key_1] for key_1 in grouped_by_country])


def plot_cool_chart(xlim=[-10000, 1500000000], ylim=[100,9000], title="G20"):
    
    # Plotting total energy use vs population over time
    plt.figure()
    
    # Scatter plot with Seaborn
    sns.scatterplot(x='population', y='energy_use', data=recollect, hue='year', palette='viridis')
    
    # Adding titles and labels
    plt.title(f'Total Energy Use (per capita) vs. Population: {title}', fontsize=16)
    plt.xlabel('Population', fontsize=12)
    plt.ylabel('Energy (ton oil eq., per capita)', fontsize=12)
    plt.legend(title='Year')
    
    # plt.xlim([-1000, 0.5*1000000000])
    plt.xlim(xlim)
    plt.ylim(ylim)
    # plt.show()
    plt.savefig(title+".pdf", dpi=1200)

"""
plot_cool_chart()
"""


grouped_by_country = {ii1: data[data["country"]==ii1] for ii1 in list(numpy.unique(data["country"])) if ii1 in ["United States"]}
recollect = pd.concat([grouped_by_country[key_1] for key_1 in grouped_by_country])
plot_cool_chart(xlim=[250000000, 350000000], ylim=[6400,8500], title="US")



grouped_by_country = {ii1: data[data["country"]==ii1] for ii1 in list(numpy.unique(data["country"])) if ii1 in ["European Union"]}
recollect = pd.concat([grouped_by_country[key_1] for key_1 in grouped_by_country])
plot_cool_chart(xlim=[410000000, 460000000], ylim=[3000,4000], title="EU")
# plt.savefig("EU", dpi=600)

grouped_by_country = {ii1: data[data["country"]==ii1] for ii1 in list(numpy.unique(data["country"])) if ii1 in ["China"]}
recollect = pd.concat([grouped_by_country[key_1] for key_1 in grouped_by_country])
plot_cool_chart(xlim=[1210000000, 1470000000], ylim=[500,2500], title="China")
# plt.savefig("China", dpi=600)


# plot_cool_chart(xlim=[200000000, 400000000], ylim=[6000,8500], title="United States")
plt.figure()
plt.ylabel("Energy Use (per capita, ton oil eq.)")
plt.title("G20 Energy Use (per capita)")
grouped_by_country = {ii1: data[data["country"]==ii1] for ii1 in list(numpy.unique(data["country"])) if ii1 in g20_list}
recollect = pd.concat([grouped_by_country[key_1] for key_1 in grouped_by_country])
recollect = recollect[recollect["year"]<2015]
sns.lineplot(x='year', y='energy_use', data=recollect, label=None, marker="o", linestyle="--")
plt.savefig("q4_2_v1.pdf")
# plt.savefig("percap", dpi=600)






# # Q5


country_classification = {
    "Argentina": "Developing",
    "Australia": "Developed",
    "Brazil": "Developing",
    "Canada": "Developed",
    "China": "Developing",
    "European Union": "Developed",
    "France": "Developed",
    "Germany": "Developed",

    "India": "Developing",
    "Indonesia": "Developing",
    "Italy": "Developed",
    "Japan": "Developed",
    "Korea, Rep.": "Developed",
    "Mexico": "Developing",
    "Russian Federation": "Developing",
    "Saudi Arabia": "Developing",
    "South Africa": "Developing",
    "Turkiye": "Developing",
    "United Kingdom": "Developed",
    "United States": "Developed",
    "Bangladesh" : "Underdeveloped", 
    "Chad" : "Underdeveloped",
    "Ethiopia" : "Underdeveloped",
    "Haiti" : "Underdeveloped",
    "Nepal" : "Underdeveloped",
    "Niger" : "Underdeveloped",
    "Somalia" : "Underdeveloped",
    "Sudan": "Underdeveloped",
    "Zimbabwe": "Underdeveloped"
}

# data['country_category'] = data['country'].map(country_classification)

data['Classification'] = data['country'].map(country_classification)

plt.figure(figsize=(10, 6))

# Create a scatter plot using Seaborn, color by Classification
sns.scatterplot(data=data, x='fdi', y='unemployment', hue='Classification', style='Classification', s=100, palette="deep")

# Add titles and labels
plt.title('Impact of FDI on Unemployment Rates by Country Classification', fontsize=16)
plt.xlabel('Foreign Direct Investment (FDI)', fontsize=14)
plt.ylabel('Unemployment Rate (%)', fontsize=14)

# Show the plot
plt.legend(title='Country Classification')

markers = ['o', 'X', 'D'][:len(unique_categories)]  # Ensure correct length


# plt.figure()
sns.lmplot(
    data=data,
    x='fdi',
    y='unemployment',
    hue="Classification",
    palette="deep",
    height=6,
    aspect=1.4,
    markers=markers, # Use the correct number of markers
    scatter=True,
)
plt.title("Impact of FDI on Unemployment Rates by Country Classification",fontsize=20)
plt.xlabel("Foreign Direct Investment (% of GDP)")
plt.ylabel("Unemployment %")
# sns.scatterplot(data=data, x='fdi', y='unemployment', hue='Classification', style='Classification', s=100, palette="deep")
plt.tight_layout()



# plt.show()
plt.savefig("q5_2_v1.pdf", dpi=1200)


plt.figure()


# plt.figure()
sns.lmplot(data=data,
           x='fdi',
           y='unemployment',
           hue="Classification",
           palette='deep',
           height=6,
           aspect=1.5,
           markers=markers, # Use the correct number of markers
           scatter=False)

plt.title("Increased Variance in Unemployment", fontsize=24)
plt.xlabel("Foreign Direct Investment (% of GDP)")
plt.ylabel("Unemployment %")
plt.ylim([0,14])
#;sns.scatterplot(data=data, x='fdi', y='unemployment', hue='Classification', style='Classification', s=100, palette="deep")

# plt.show()
plt.tight_layout()


plt.savefig("q5_3_v1.pdf", dpi=1200)