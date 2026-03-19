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

# ### **Ingestion del archivo "device.csv"**

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

from pyspark.sql.types import StructType, StructField, IntegerType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

device_schema = StructType ( fields = [
    StructField("DeviceID", IntegerType(), False),
    StructField("BrandID", IntegerType(), True),
    StructField("ModelID", IntegerType(), True),
    StructField("DisplayId", IntegerType(), True),
    StructField("CameraID", IntegerType(), True),
    StructField("ConnectivityID", IntegerType(), True),
    StructField("OSID", IntegerType(), True),
    StructField("PhysicalSpecID", IntegerType(), True),
    StructField("ReleasedYear", IntegerType(), True),
    StructField("ReleasedAnnounced", DateType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

device_df = spark.read \
            .option("header", True) \
            .schema(device_schema) \
            .csv(f"{bronze_folder_path}/{p_file_date}/device.csv")

            #La variable "bronze_folder_path" viene del notebook configuration

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(device_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sin esquema definido tarda mas en leerlo porque recorre todo el archivo
# 
# device_df = spark.read \
#            .option("header", True) \
#            .option("inferSchema", True) \
#            .csv("abfss://Smart_Device_Analytics_WS@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/smart-device/bronze-data/2025-07-07/device.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Paso 2 - Seleccionar solo las columnas "requeridas"**

# CELL ********************

# Primera forma de hacer un select
device_selected_df = device_df.select("DeviceID","BrandID","ModelID","DisplayId","CameraID",
                                      "ConnectivityID","OSID","PhysicalSpecID","ReleasedAnnounced")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Segunda forma de hacer un select
device_selected_df = device_df.select(device_df.DeviceID,device_df.BrandID,device_df.ModelID,device_df.DisplayId,device_df.CameraID,
                                      device_df.ConnectivityID,device_df.OSID,device_df.PhysicalSpecID,device_df.ReleasedAnnounced)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tercera forma de hacer un select
device_selected_df = device_df.select(device_df["DeviceID"],device_df["BrandID"],device_df["ModelID"],device_df["DisplayId"],device_df["CameraID"],
                                      device_df["ConnectivityID"],device_df["OSID"],device_df["PhysicalSpecID"],device_df["ReleasedAnnounced"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cuarta forma de hacer un select
device_selected_df = device_df.select(col("DeviceID"),col("BrandID"),col("ModelID"),col("DisplayId"),col("CameraID"),
                                      col("ConnectivityID"),col("OSID"),col("PhysicalSpecID"),col("ReleasedAnnounced"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(device_selected_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

device_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 3 - Cambiar el nombre de las columnas segun lo "requerido"

# CELL ********************

# Con el primer metodo podemos concatenar mas funciones 

device_renamed_df = device_selected_df \
                    .withColumnRenamed("DeviceID", "device_id") \
                    .withColumnRenamed("BrandID", "brand_id") \
                    .withColumnRenamed("ModelID", "model_id") \
                    .withColumnRenamed("DisplayID", "display_id") \
                    .withColumnRenamed("CameraID", "camera_id") \
                    .withColumnRenamed("ConnectivityID", "connectivity_id") \
                    .withColumnRenamed("OSID", "os_id") \
                    .withColumnRenamed("PhysicalSpecID", "physical_spec_id") \
                    .withColumnRenamed("ReleasedAnnounced", "released_announced")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Con el segundo metodo es mas facil pero no concatenamos nada mas
# device_renamed_df = device_selected_df \
#                    .withColumnsRenamed({"DeviceID":"device_id","BrandID":"brand_id", "ModelID":"model_id","DisplayID":"display_id",
#                                         "CameraID":"camera_id","ConnectivityID":"connectivity_id","OSID":"os_id","PhysicalSpecID":"physical_spec_id",
#                                         "ReleasedAnnouncedID":"released_announced"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(device_renamed_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 4 - Agregar las columnas "ingestion_date" y "enviroment"

# CELL ********************

#Ahora se utilizara la funcion que esta en el notebook de common_functions llamada 'add_ingestion_date'
#from pyspark.sql.functions import current_timestamp, lit  
from pyspark.sql.functions import  lit


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Primera opcion con withColum
device_final_df = add_ingestion_date(device_renamed_df) \
                .withColumn("environment", lit(p_environment)) \
                .withColumn("file_date", lit(p_file_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Segunda opcion con withColums
# device_final_df = device_renamed_df.withColumns({"ingestion_date": current_timestamp(),
#                                    "environment":lit("Development")})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(device_final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 5 - Escribir datos en el "lakehouse" en formato "Delta"

# CELL ********************

# MAGIC %%sql   
# MAGIC DROP TABLE IF EXISTS device;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

device_final_df.write.format("delta").mode("overwrite").saveAsTable("device")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Select * from device
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
