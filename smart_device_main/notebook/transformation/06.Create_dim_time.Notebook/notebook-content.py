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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### **Crear la dimension "Time"**

# MARKDOWN ********************

# ##### Paso 1 - Leer la tabla de hechos "fact_device" usando "spark.sql"

# CELL ********************

time_df = spark.sql("SELECT * FROM lh_gold.dbo.fact_device")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(time_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 2 - Seleccionar la columna "released_announces"

# CELL ********************

from pyspark.sql.functions import col
time_selected_df = time_df.select(col("released_announced"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 3 - Eliminar duplicados y hacer un "orden ascendente"

# CELL ********************

time_dup_and_order = time_selected_df.dropDuplicates(["released_announced"]) \
                                        .sort("released_announced", ascending = True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 4 - Cambiar el nombre de la columna "released_announced a "date"

# CELL ********************

time_renamed = time_dup_and_order.withColumnRenamed("released_announced", "date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 5 -Agregar Columnas de "TimeIntelligence" a la Dimension "Time"

# CELL ********************

from pyspark.sql.functions import year, month, dayofmonth, date_format,quarter,concat, lit 

time_final_df = time_renamed.withColumn("year", year("date")) \
                            .withColumn("month", month("date")) \
                            .withColumn("day", dayofmonth("date")) \
                            .withColumn("month_name", date_format("date","MMMM")) \
                            .withColumn("day_name", date_format("date", "EEEE")) \
                            .withColumn("quarter", concat(lit("Q"), quarter("date")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 6 - Escribir datos en el "lh_gold"

# CELL ********************

time_final_df.write.format("delta") \
                    .mode("overwrite") \
                    .saveAsTable("dim_time")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Select * from dim_time limit 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
