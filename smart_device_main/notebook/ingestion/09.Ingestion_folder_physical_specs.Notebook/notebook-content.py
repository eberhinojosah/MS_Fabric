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

# ### **Ingestion de la carpeta "physical_specs**

# MARKDOWN ********************

# ##### Paso 1 - Leer los archivos JSON usando "DataframeReader" de Spark

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

phys_spec_schema = StructType ( fields = [
                    StructField("PhysicalSpecID", IntegerType(), False),
                    StructField("Width", StringType(), True),
                    StructField("Height", StringType(), True),
                    StructField("Depth", StringType(), True),
                    StructField("BoundingVolume", StringType(), True),
                    StructField("Mass", StringType(), True),
                    StructField("RAMCapacity", StringType(), True),
                    StructField("NonVolatileMemoryCapacity", StringType(), True),
                    StructField("CellularController", StringType(), True),
                    StructField("NominaBatteryCapacity", StringType(), True),
                    StructField("EstimateBatteryLife", StringType(), True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

phys_spec_df = spark.read \
            .schema(phys_spec_schema) \
            .option("multiline", True) \
            .json(f"{bronze_folder_path}/{p_file_date}/physical_specs")

            #abfss://Smart_Device_Analytics_WS@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/smart-device/bronze-data/2025-07-07/physical_specs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# - 1.- "PhysicalSpecID" renombrar a "physical_spec_id"
# - 2.- "Width" renombrar a "width"
# - 3.- "Height" renombrar a "height"
# - 4.- "Depth" renombrar a "depth"
# - 5.- "Mass" renombrar a "mass"
# - 6.- "RAMCapacity" renombrar a "ram_capacity"
# - 7.- "CellularController" renombrar a "controller"
# - 8.- "NominalBAtteryCapacity" renombrar a "battery_capacity"
# - 9.- Agregar las columnas "ingestion_date" y "environment"


# CELL ********************

from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

phys_spec_with_columns_df = add_ingestion_date(phys_spec_df) \
                            .withColumn("environment", lit(p_environment)) \
                            .withColumn("file_date", lit(p_file_date)) \
                            .withColumnRenamed("PhysicalSpecID", "physical_spec_id") \
                            .withColumnRenamed("Width", "width") \
                            .withColumnRenamed("Height", "height") \
                            .withColumnRenamed("Depth", "depth") \
                            .withColumnRenamed("Mass", "mass") \
                            .withColumnRenamed("RAMCapacity", "ram_capacity") \
                            .withColumnRenamed("CellularController", "controller") \
                            .withColumnRenamed("NominalBAtteryCapacity", "battery_capacity") \
                           


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 3 - Eliminar las columnas no deseadas del DataFrame ###
# 
# - 1.- "BoundingVolume"
# - 2.- "NonVolatileMemoryCapacity"
# - 3.- "EstimatedBatteryLife"

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

phys_spec_final_df = phys_spec_with_columns_df.drop(col("BoundingVolume"),
                                                    col("NonVolatileMemoryCapacity"),
                                                    col("EstimatedBatteryLife"))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 4 - Escribir datos en el "lakehouse" en formato "Delta"

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS physical_specs;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

phys_spec_final_df.write.format("delta").mode("overwrite").saveAsTable("physical_specs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM physical_specs LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
