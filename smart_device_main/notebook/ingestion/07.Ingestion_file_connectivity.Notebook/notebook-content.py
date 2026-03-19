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

# ### **Ingestion del archivo "connectivity.json" json multilinea**

# CELL ********************

#p_file_date = "2025-07-07"
#p_environment = "Development"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Paso 1 - Leer el archivo JSON usando "DataFrameReader" de Spark

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

conn_schema = StructType(fields = [
            StructField("ConnectivityID", IntegerType(), False),
            StructField("USB", StringType(), True),
            StructField("USBConnector", StringType(), True),
            StructField("Bluetooth", StringType(), True),  
            StructField("NFC", StringType(), True),
            StructField("SIMCardSlot", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

conn_df = spark.read \
                .schema(conn_schema) \
                .option("multiLine", True) \
                .json(f"{bronze_folder_path}/{p_file_date}/connectivity.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# - 1.-"ConnectivityID" renombrar a "connectivity_id"
# - 2.-"USB" renombrar a "usb"
# - 3.- "USBConnector" renombrar a "usb_connector"
# - 4.- "Bluetooth" renombrar a "bluetooth"
# - 5.- "NFC" renombrar a "nfc"
# - 6.- "SIMCardSlot" renombrar a "sim_card_slot"
# - 7.- Agregar las columnas "ingestion_date" y "environment"

# CELL ********************

from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

conn_final_df =  add_ingestion_date(conn_df) \
            .withColumn("environment", lit(p_environment)) \
            .withColumn("file_date", lit(p_file_date)) \
            .withColumnRenamed("ConnectivityID", "connectivity_id") \
            .withColumnRenamed("USB", "usb") \
            .withColumnRenamed("USBConnector", "usb_connector") \
            .withColumnRenamed("Bluetooth", "bluetooth") \
            .withColumnRenamed("NFC", "nfc") \
            .withColumnRenamed("SIMCardSlot", "sim_card_slot") \
            

                   
                

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 4 - Escribir la salida en formato "Delta"

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE if EXISTS connectivity;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

conn_final_df.write.format("delta").mode("overwrite").saveAsTable("connectivity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT* from connectivity limit 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
