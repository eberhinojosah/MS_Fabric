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

# ### **Ingestion del archivo "operation_system.json" json multilinea**

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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

op_system_schema = StructType(fields = [
            StructField("OSID", IntegerType(), False),
            StructField("Platform", StringType(), True),
            StructField("OperatingSystem", StringType(), True),
            StructField("SoftwareExtras", StringType(), True)            
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

op_system_df = spark.read \
                .schema(op_system_schema) \
                .option("multiLine", True) \
                .json(f"{bronze_folder_path}/{p_file_date}/operationg_system.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# - 1.-"OSID" renombrar a "os_id"
# - 2.-"Platform" renombrar a "platform"
# - 3.- Agregar las columnas "ingestion_date" y "environment"

# CELL ********************

from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

op_system_with_column_df = add_ingestion_date(op_system_df) \
                            .withColumn("environment", lit(p_environment)) \
                            .withColumn("file_date", lit(p_file_date)) \
                            .withColumnRenamed("OSID", "os_id") \
                            .withColumnRenamed("Platform", "platform")
                           
                   
                

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(op_system_with_column_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 3 - Eliminar las columnas no deseadas del DataFrame

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

op_system_final_df= op_system_with_column_df.drop(col("OperatingSystem"), col("SoftwareExtras"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(op_system_final_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 4 - Escribir la salida en formato "Delta"

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS operating_system;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

op_system_final_df.write.format("delta").mode("overwrite").saveAsTable("operating_system")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT* from operating_system limit 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
