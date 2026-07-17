# PySpark Mini Project: Indian Takeaway Orders Analysis

## Overview
This project involves loading a dataset of Indian takeaway orders (~33,000 records) into Databricks and using PySpark to clean the data and extract business insights. The goal is to perform Exploratory Data Analysis (EDA) and answer key business questions to be presented to stakeholders.

## Implementation Plan & Tasks

- [x] **Phase 1: Ingestion & Setup**
  - Create a new Python notebook in Databricks (`takeaway-project.ipynb`).
  - Upload raw CSV files (`restaurant_2_orders`) to Databricks DBFS.
  - Load the data into a PySpark DataFrame.
- [x] **Phase 2: Data Cleaning**
  - Standardize column names (replace spaces with underscores).
  - Cast `Order Date` from string to `Timestamp` format using the UK date format `dd/MM/yyyy HH:mm`.
  - Extract useful features for analysis: `Order_Hour` and `Order_DayOfWeek`.
- [ ] **Phase 3: Data Analysis & Visualization**
  - **Question 1:** Temporal Trends (What are the busiest days/hours?)
  - **Question 2:** Product Popularity (What are the top 10 items & Revenue?)
  - **Question 3:** Restaurant Comparison (Volume & Average Order Value)
- [ ] **Phase 4: Bonus**
  - Save cleaned DataFrame as a Delta Table.
  - Connect a 3rd party tool to the Databricks SQL endpoint.

---

## Project Execution & Code

### Phase 1 & 2: Ingestion and Cleaning
```python
import pyspark.sql.functions as F

# 1. Load the table you just created
df_rest2 = spark.table("workspace.default.restaurant_2_orders")

# 2. Clean Column Names (replace spaces with underscores automatically)
for col_name in df_rest2.columns:
    df_rest2 = df_rest2.withColumnRenamed(col_name, col_name.replace(" ", "_"))

# 3. Convert 'Order_Date' from string to an actual Timestamp using the UK format
df_clean = df_rest2.withColumn("Order_Date", F.to_timestamp("Order_Date", "dd/MM/yyyy HH:mm")) \
                   .withColumn("Order_Hour", F.hour("Order_Date")) \
                   .withColumn("Order_DayOfWeek", F.date_format("Order_Date", "EEEE"))

# 4. Show the cleaned data
df_clean.printSchema()
display(df_clean)
```

### Phase 2 Proof: Schema Output Screenshot
*(You can save your schema success screenshots in this folder and link them here!)*
> **Screenshot Placeholder:** `![Schema Output Screenshot](schema_success.png)`

---

### Phase 3: Data Analysis (Next Steps)
Use the following code blocks in your Databricks Notebook to answer the business questions.

#### Q1: Temporal Trends (Busiest Days and Hours)
**Findings:** As expected for a takeaway restaurant, the weekend completely dominates! **Saturday** is the busiest day by a wide margin (59,019 orders), followed by Friday and Sunday. Tuesday is the slowest day. 

*(Save your screenshot as `busiest_days.png` in this folder to display it below)*
![Busiest Days Chart](busiest_days.png)
```python
# Group by Day of Week to find busiest days
busiest_days = df_clean.groupBy("Order_DayOfWeek").count().orderBy("count", ascending=False)
display(busiest_days) 
# Tip: Click the '+' next to the table output in Databricks to create a Bar Chart!

# Group by Hour to find peak ordering times
peak_hours = df_clean.groupBy("Order_Hour").count().orderBy("Order_Hour")
display(peak_hours) 
# Tip: Create a Line Chart for this one!
```

#### Q2: Product Popularity (Top Items & Revenue)
**Findings:** The most frequently ordered item is by far the **Plain Papadum** (28,704 orders), followed by Pilau Rice and Naan. However, when looking at *total revenue*, the highest earning dish is **Chicken Tikka Masala** (£57,684), followed by Pilau Rice and Bombay Aloo. This shows that cheap side dishes drive volume, but main courses drive the actual revenue!

*(Save your screenshot as `product_popularity.png` in this folder to display it below)*
![Product Popularity Chart](product_popularity.png)
```python
# Top 10 Most Ordered Items
top_items = df_clean.groupBy("Item_Name") \
                    .sum("Quantity") \
                    .withColumnRenamed("sum(Quantity)", "Total_Ordered") \
                    .orderBy("Total_Ordered", ascending=False) \
                    .limit(10)
display(top_items)

# Revenue per Item (Quantity * Price)
# First we need to calculate total revenue per row
df_revenue = df_clean.withColumn("Row_Revenue", F.col("Quantity") * F.col("Product_Price"))

top_revenue_items = df_revenue.groupBy("Item_Name") \
                              .sum("Row_Revenue") \
                              .withColumnRenamed("sum(Row_Revenue)", "Total_Revenue") \
                              .orderBy("Total_Revenue", ascending=False) \
                              .limit(10)
display(top_revenue_items)
```

#### Q3: Overall Performance (Total Revenue & Average Order Value)
**Findings:** The restaurant processed **13,397 unique orders**, generating a massive total revenue of **£1,133,616.30**. The Average Order Value (AOV) is a very healthy **£84.62**, indicating that customers tend to order large meals (likely feeding families or groups).

*(Save your screenshot as `overall_performance.png` in this folder to display it below)*
![Overall Performance Chart](overall_performance.png)

```python
# Calculate Total Orders, Total Revenue, and Average Order Value (AOV)
from pyspark.sql.functions import countDistinct, sum, round

performance_df = df_revenue.agg(
    countDistinct("Order_Number").alias("Total_Unique_Orders"),
    round(sum("Row_Revenue"), 2).alias("Total_Revenue")
).withColumn(
    "Average_Order_Value", 
    round(F.col("Total_Revenue") / F.col("Total_Unique_Orders"), 2)
)

display(performance_df)
```

---

### Phase 4: Bonus (Delta Table & 3rd Party Connection)

#### 1. Save Cleaned Data as a Delta Table
To make the cleaned data permanently available in the Databricks Catalog for other tools to query, we saved the DataFrame as a Delta Table.

*(Save your screenshot as `delta_table_save.png` in this folder to display it below)*
![Delta Table Save Confirmation](delta_table_save.png)

```python
# Save our cleaned, revenue-calculating DataFrame as a permanent Delta Table
df_revenue.write.format("delta").mode("overwrite").saveAsTable("workspace.default.restaurant_2_cleaned")
print("Cleaned data successfully saved to Catalog!")
```

#### 2. Connect a 3rd Party Tool (Tableau / PowerBI)
To connect external Business Intelligence software to this Databricks table:
1. Navigate to **Compute** in the Databricks sidebar.
2. Select the active cluster.
3. Scroll to **Advanced options** -> **JDBC/ODBC**.
4. Use the provided Server Hostname, Port, and HTTP Path in Tableau/PowerBI, authenticating with a Personal Access Token.
