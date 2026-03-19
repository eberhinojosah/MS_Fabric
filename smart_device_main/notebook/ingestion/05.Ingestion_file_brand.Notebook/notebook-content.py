# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7ef17035-eddb-4f03-bdd9-a978e296af24",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "44d09e17-8f0e-43a0-bbb6-a600b3cbd2bb",
# META       "known_lakehouses": [
# META         {
# META           "id": "d9c9b849-d2cb-47c2-9a34-aeefde497789"
# META         },
# META         {
# META           "id": "7ef17035-eddb-4f03-bdd9-a978e296af24"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### **Ingestion del archivo "brand.json"**

# MARKDOWN ********************

# #### Paso 1 - Leer el archivo JSON usando "DataFrameReader" de Spark

# CELL ********************

#p_file_date = "2025-07-07"
#p_environment = "Development"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run common_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

brand_schema = "BrandID INT, Brand STRING, HardwareDesigner STRING, Manufacturer STRING"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

brand_df = spark.read \
            .schema(brand_schema) \
            .json(f"{bronze_folder_path}/{p_file_date}/brand.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 2 - Añadir columnas de "ingestion_date" y "environment"

# CELL ********************

from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

brand_final_df = add_ingestion_date(brand_df) \
                    .withColumn("environment", lit(p_environment)) \
                    .withColumn("file_date", lit(p_file_date)) \
                    .withColumnRenamed("BrandID", "brand_id") \
                    .withColumnRenamed("Brand", "brand") \
                    .withColumnRenamed("HardwareDesigner", "hardware_designer") \
                    .withColumnRenamed("Manufacturer", "manufacturer")        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 3 - Escribir la salida en un formato "Delta

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS brand;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

brand_final_df.write.format("delta").mode("overwrite").saveAsTable("brand")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Select * from brand limit 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
