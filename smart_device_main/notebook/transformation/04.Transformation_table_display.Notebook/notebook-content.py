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

# ### **Transformacion de la tabla "display"**

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

# ##### Paso 1 - Leer la tabla "display" usando "Spark.sql"

# CELL ********************

display_df = spark.sql("SELECT * FROM lh_silver.dbo.display")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 2 - Eliminar la columna "environment" del DataFrame

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display_drop_column_df = display_df.drop(col("environment"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 3 - Transformar la columna "refresh_rate"

# CELL ********************

display_final_df = display_drop_column_df.na.fill({"refresh_rate": "uknown"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 4 - Escribir datos en el "lh_gold"

# CELL ********************

merge_condition = 'tgt.display_id = src.display_id AND tgt.file_date = src.file_date'


#Llamamos a la funcion
merge_delta_lake(display_final_df, "lh_gold", "dbo","dim_display", gold_folder_path, merge_condition, "file_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT file_date, count(1) from dim_display
# MAGIC GROUP By file_date;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
