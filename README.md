Welcome to the Sum Up Revenue operation analytics dbt project!

### Data Model Design:
Staging Layer: Loads raw data from CSV files: Financials, FUnnel, Sales and Store
Intermediate Layer: Aggregates and transforms data into data marts: Customer Financials, Funnel Performance, Sales reps performance
Analysis Layer: SQL queries to answer business questions: Best channel, Worst channel, Top Sales Rep, Lead time

![alt text](<dbt model dag.png>)

### Solution implementation Step by step guide to run the project

1. Setting up the environment
    Create virtual environment: python -m venv dbt-env  
    Activate virtual environment : source dbt-env/bin/activate
    Install dbt and duckdb adapter: python -m pip install dbt-core dbt-duckdb 
    Install duckdb: pip install duckdb: brew install duckdb

2. Setup new DBT project via: dbt init sumup_revops

3. Load CSV files into DuckDB and create staging data models - 1 data model per file

4. Setup config files: dbt_project.yml and profiles.yml

5. Try running the dbt run command and verify if staging data models are loaded correctly.

6. Build tests and create model_properties.yml file to bind test cases and manage documentaion.

7. Create intermediate layer with relevant data models to include dimensions and facts which can be used for analysis tables later. Repeat step 4 - 6 till desired models are ready.

8. Build data models for tasks in analysis - one table per question.

9. Plug duckdb into any visualization tool such as Tableau and use prepared data models to find relevant insights.

10. Generate documentation based on model_properties.yml: dbt docs generate
    
Try running the following commands:
- dbt run
- dbt test

### Assumption
1. All the records in the financials file are assumed to be converted customers as all of them have acquisition month present in sales.csv.
2. In the financials file, there are customers present more than once. Assuming CAC and LTV are calculated separately when several devices are sold to customers and there are multiple rows per customer for different device IDs.
3. All ltv/cac calculations are considered in euros irrespective of different regions.
4. Date in the funnel dataset looks fishy as only dates between 1-12 are present for the month of January if the d/m/y format is considered. Assume that data is correct as per d/m/y format for now.
5. The timestamp column in the store file has a few rows with different formats such as mm/dd/yy with AM. Relevant data quality test cases are built to pick a different format and for now, data with different formats is excluded and not loaded.
6. Use min the acquisition month to join with the customer financials table as sales data does not contain the device Id to uniquely identify the record.


### Executive Summary from the analysis
The sales lead and funnel data for customers who purchased Sumup devices in 2023 were analysed to uncover top insights and identify areas for performance improvement. The objective is to understand the best and worst-performing acquisition channels, evaluate the sales funnel, and identify potential bottlenecks impacting lead times. 

Key Insights
1. The direct sales channel consistently shows high customer acquisition and higher lifetime value (LTV) across all markets. Notably, the UK and Germany exhibit strong performance in this channel.
2. The web channel is the worst performer, with a low LTV to customer acquisition cost (CAC) ratio of 2.66 and an average LTV per customer of only 245 euros, just 3% of that achieved through direct sales.
3. Hotels exhibit the highest total LTV, LTV per customer, and LTV to CAC ratio, marking them as a premium segment requiring exceptional sales and onboarding experiences.
4. Food trucks and restaurants also show high LTV per customer with comparatively lower CAC, making them valuable segments to focus on.
5. The overall average lead to live time is 164 days, skewed by outliers exceeding 350 days. Excluding outliers reduces this to 4.8 days.
6. Top categories with the lowest lead times (excluding outliers) are hotels, beauty, and restaurants.

Based on the insights, we can align with the GTM strategy team to come up with an action plan on improving the sales performance and reducing lead time. 




