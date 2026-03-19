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

# ### **Transformacion de la tabla "model"**

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

# MARKDOWN ********************

# ##### Paso 1 - Leer la tabla "model" usando "Spark.sql"

# CELL ********************

model_df = spark.sql("SELECT * FROM lh_silver.dbo.model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 2 - Eliminar la columna "environment"

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_final_df = model_df.drop(col("environment"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 3 Escribir en el "lh_gold"

# CELL ********************

merge_condition = "tgt.model_id = src.model_id AND tgt.file_date = src.file_date"
merge_delta_lake(model_final_df, "lh_gold", "dbo","dim_model", gold_folder_path, merge_condition, "file_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC SELECT file_date, count(1) FROM dim_model
# MAGIC GROUP By file_date;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
