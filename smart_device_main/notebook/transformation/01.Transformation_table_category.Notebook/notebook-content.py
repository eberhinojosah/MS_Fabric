# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0441ae0c-df2d-46f2-99f9-aa3e4e65098b",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "44d09e17-8f0e-43a0-bbb6-a600b3cbd2bb",
# META       "known_lakehouses": [
# META         {
# META           "id": "0441ae0c-df2d-46f2-99f9-aa3e4e65098b"
# META         },
# META         {
# META           "id": "7ef17035-eddb-4f03-bdd9-a978e296af24"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### **Transformación de la tabla "category"**

# MARKDOWN ********************

# ##### Paso 1 - Leer la tabla "category" usando spark.sql

# CELL ********************

category_df= spark.sql("SELECT * FROM lh_silver.dbo.category")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 2 - Eliminar la columna "environment" del data frame

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

category_final_df = category_df.drop(col("environment"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 3 - Escribir datos en el "lh_gold"

# CELL ********************

category_final_df.write.format("delta").mode("overwrite").saveAsTable("dim_category")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Select * from dim_category Limit 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
