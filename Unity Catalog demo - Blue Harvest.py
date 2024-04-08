# Databricks notebook source
# MAGIC %md
# MAGIC #### 
# MAGIC ## 💻 Unity Catalog quick demo to Blue Harvest data domain
# MAGIC 	
# MAGIC   Repository with step by step to create a demo for [Databricks Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/)
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 📝   Details:
# MAGIC
# MAGIC Dataset is online retail platform that captures customer information, customer orders and customer events. We will create a catalog, then create tables and learn how Unity Catalog allows you to securely discover, access and collaborate on trusted data and AI assets

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📌 Ingestion and transformation

# COMMAND ----------

# MAGIC %md
# MAGIC #### list databricks datasets demo - on retail org

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/retail-org/
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### get user id 

# COMMAND ----------

user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
user_id = ''.join(filter(str.isdigit, user_id))
print(user_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### print read me to understand better the retail org dataset

# COMMAND ----------

f = open('/dbfs/databricks-datasets/retail-org/README.md', 'r')
print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create and ingest Active Promotions at bronze layer

# COMMAND ----------

active_promo = spark.read.parquet('/databricks-datasets/retail-org/active_promotions/')

active_promo.write.format("delta").mode("overwrite").saveAsTable(f"unity_catalog_blue_harvest.default.active_promotions_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create and ingest Customers at bronze layer

# COMMAND ----------

customers = spark.read.csv('dbfs:/databricks-datasets/retail-org/customers/', header = True)

customers.write.format("delta").mode("overwrite").saveAsTable(f"unity_catalog_blue_harvest.default.customers_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create and ingest Suppliers at bronze layer

# COMMAND ----------

suppliers = spark.read.csv('dbfs:/databricks-datasets/retail-org/suppliers/', header = True)

suppliers.write.format("delta").mode("overwrite").saveAsTable(f"unity_catalog_blue_harvest.default.suppliers_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create and ingest Loyalty segment at bronze layer

# COMMAND ----------

loyalty_segment = spark.read.csv('dbfs:/databricks-datasets/retail-org/loyalty_segments/', header = True)

loyalty_segment.write.format("delta").mode("overwrite").saveAsTable(f"unity_catalog_blue_harvest.default.loyalty_segment_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create and ingest Sales Orders at bronze layer

# COMMAND ----------

sales_orders = spark.read.json('/databricks-datasets/retail-org/sales_orders/')

sales_orders.write.format("delta").mode("overwrite").saveAsTable(f"unity_catalog_blue_harvest.default.sales_orders_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create and ingest Retail at bronze layer

# COMMAND ----------

products = spark.read.option("delimiter", ";").csv('/databricks-datasets/retail-org/products/', header = True)

products.write.format("delta").mode("overwrite").saveAsTable(f"unity_catalog_blue_harvest.default.products_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE unity_catalog_blue_harvest.default.products_silver AS
# MAGIC SELECT product_category, SUM(sales_price) total_sales
# MAGIC FROM unity_catalog_blue_harvest.default.products_bronze
# MAGIC GROUP BY product_category;
# MAGIC
# MAGIC SELECT * FROM unity_catalog_blue_harvest.default.products_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE unity_catalog_blue_harvest.default.top10_customer_gold AS
# MAGIC
# MAGIC SELECT  t2.customer_name, count(t1.number_of_line_items) as sum_of_items
# MAGIC FROM unity_catalog_blue_harvest.default.sales_orders_bronze t1
# MAGIC inner join unity_catalog_blue_harvest.default.customers_bronze t2
# MAGIC on
# MAGIC t1.customer_id = t2.customer_id
# MAGIC GROUP BY 1
# MAGIC order by 2 desc
# MAGIC limit 10;
# MAGIC
# MAGIC SELECT * FROM unity_catalog_blue_harvest.default.top10_customer_gold
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📌   Now start grant permission and the whole magic of unity catalog
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lineage
# MAGIC ![Lineage](https://github.com/QuentinAmbard/databricks-demo/blob/main/product_demos/uc/lineage/uc-lineage-slide.png?raw=true "Lineage")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Column Lineage
# MAGIC ![Lineage](https://github.com/QuentinAmbard/databricks-demo/blob/main/product_demos/uc/lineage/lineage-column.gif?raw=true "Lineage")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table Lineage
# MAGIC ![Lineage](https://github.com/QuentinAmbard/databricks-demo/blob/main/product_demos/uc/lineage/lineage-table.gif?raw=true "Lineage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Details

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🛠  Research and Demo
# MAGIC Table ACL & Row and Column Level Security With Unity Catalog
# MAGIC https://notebooks.databricks.com/demos/uc-01-acl/index.html
# MAGIC
# MAGIC Access Data on External Locations
# MAGIC https://notebooks.databricks.com/demos/uc-02-external-location/index.html
# MAGIC
# MAGIC Data Lineage With Unity Catalog
# MAGIC https://notebooks.databricks.com/demos/uc-03-data-lineage/00-UC-lineage.html
# MAGIC
# MAGIC System Tables: Billing Forecast, Usage Analytics, and Access Auditing With Databricks Unity Catalog
# MAGIC https://notebooks.databricks.com/demos/uc-04-system-tables/index.html
# MAGIC
# MAGIC Upgrade Table to Unity Catalog
# MAGIC https://notebooks.databricks.com/demos/uc-05-upgrade/index.html
# MAGIC
# MAGIC Azure Databricks Unity catalog
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-lineage
# MAGIC
# MAGIC Lineage Demo
# MAGIC https://app.getreprise.com/launch/MnqjQDX/
# MAGIC
# MAGIC https://github.com/databricks-demos/dbdemos
# MAGIC
# MAGIC Ebook Governance and Unity catalog
# MAGIC https://www.databricks.com/sites/default/files/2023-10/final_data-and-ai-governance.6sept2023.pdf
# MAGIC
# MAGIC https://www.databricks.com/resources/demos/tutorials/lakehouse-platform/dbdemos-hls-patient-readmission-health?itm_data=demo_center
# MAGIC https://www.databricks.com/resources/demos/tutorials/governance/table-acl-and-dynamic-views-with-uc?itm_data=demo_center
# MAGIC https://www.databricks.com/resources/demos/tours/governance/query-federation-product-tour?itm_data=demo_center
# MAGIC https://www.databricks.com/resources/demos/videos/governance/unity-catalog-demo?itm_data=demo_center
# MAGIC
# MAGIC
# MAGIC How to use demo databricks datasets
# MAGIC https://www.databricks.com/resources/demos/library?itm_data=demo_center
# MAGIC Demo
# MAGIC https://app.getreprise.com/launch/Q6ojw2y/
# MAGIC
# MAGIC
# MAGIC
# MAGIC Quick step
# MAGIC %pip install dbdemos
# MAGIC
# MAGIC import dbdemos
# MAGIC dbdemos.install('uc-03-data-lineage')
# MAGIC
# MAGIC -----
# MAGIC
# MAGIC Going further with Data governance & security
# MAGIC By bringing all your data assets together, Unity Catalog let you build a complete and simple governance to help you scale your teams.
# MAGIC
# MAGIC Unity Catalog can be leveraged from simple GRANT to building a complete datamesh organization.
# MAGIC
# MAGIC
# MAGIC Fine-grained ACL: row/column level access
# MAGIC Need more advanced control? You can chose to dynamically change your table output based on the user permissions: dbdemos.intall('uc-01-acl')
# MAGIC
# MAGIC Secure external location (S3/ADLS/GCS)
# MAGIC Unity Catatalog let you secure your managed table but also your external locations: dbdemos.intall('uc-02-external-location')
# MAGIC
# MAGIC Lineage
# MAGIC UC automatically captures table dependencies and let you track how your data is used, including at a row level: dbdemos.intall('uc-03-data-lineage')
# MAGIC
# MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
# MAGIC
# MAGIC Audit log
# MAGIC UC captures all events. Need to know who is accessing which data? Query your audit log: dbdemos.intall('uc-04-audit-log')
# MAGIC
# MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
# MAGIC
# MAGIC Upgrading to UC
# MAGIC Already using Databricks without UC? Upgrading your tables to benefit from Unity Catalog is simple: dbdemos.intall('uc-05-upgrade')
# MAGIC
# MAGIC Sharing data with external organization
# MAGIC Sharing your data outside of your Databricks users is simple with Delta Sharing, and doesn't require your data consumers to use Databricks: dbdemos.intall('delta-sharing-airlines')
# MAGIC ----
# MAGIC Research
# MAGIC
# MAGIC https://www.databricks.com/resources/demos/tutorials/lakehouse-platform/dbdemos-hls-patient-readmission-health?itm_data=demo_center
# MAGIC https://www.databricks.com/resources/demos/tutorials/governance/table-acl-and-dynamic-views-with-uc?itm_data=demo_center
# MAGIC https://www.databricks.com/resources/demos/tours/governance/query-federation-product-tour?itm_data=demo_center
# MAGIC https://www.databricks.com/resources/demos/videos/governance/unity-catalog-demo?itm_data=demo_center
# MAGIC
# MAGIC #  📚 Ebook
# MAGIC https://www.databricks.com/sites/default/files/2023-10/final_data-and-ai-governance.6sept2023.pdf
# MAGIC
# MAGIC ## 👨🏻‍🔧 Details
# MAGIC https://www.databricks.com/blog/2022/04/20/announcing-gated-public-preview-of-unity-catalog-on-aws-and-azure.html
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Audit

# COMMAND ----------

## Links

https://medium.com/@24chynoweth/databricks-system-tables-an-introduction-e11a06872405
https://notebooks.databricks.com/demos/uc-01-acl/index.html
https://notebooks.databricks.com/demos/uc-04-system-tables/index.html#

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 👨🏽‍🏫 Created by [Diego Lopes](mailto:lopesdiego12@gmail.com)
