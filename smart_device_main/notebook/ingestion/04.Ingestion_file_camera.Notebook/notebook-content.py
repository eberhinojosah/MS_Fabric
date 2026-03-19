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

# ### **Ingestion del archivo "camera.json"**

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

camera_schema = "CameraID INT, CameraResolution STRING, NumberOfEffectivePixel STRING, \
                Aperture STRING, Zoom STRING, RecordableImageFormat STRING, \
                VideoRecording STRING, Flash STRING"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

camera_df = spark.read \
            .schema(camera_schema) \
            .json(f"{bronze_folder_path}/{p_file_date}/camera.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#camera_selected_df = camera_df.select("CameraID", "CameraResolution", "NumberOfEffectivePixel","Aperture", "Zoom", "RecordableImageFormat","VideoRecording" , "Flash")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 2 Eliminar columnas no deseadas del DataFrame

# CELL ********************

camera_dropped_df = camera_df.drop("Aperture", "Zoom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 3 - Cambiar el nombre de las columnas y añadir "ingestion_date" y "environment"

# CELL ********************

from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

camera_final_df = add_ingestion_date(camera_dropped_df) \
                    .withColumn("environment", lit(p_environment)) \
                    .withColumn("file_date", lit(p_file_date)) \
                    .withColumnRenamed("CameraID", "camera_id") \
                    .withColumnRenamed("CameraResolution", "camera_resolution") \
                    .withColumnRenamed("NumberOfEffectivePixel", "effective_pixel") \
                    .withColumnRenamed("RecordableImageFormat", "image_format") \
                    .withColumnRenamed("VideoRecording", "video_recording") \
                    .withColumnRenamed("Flash", "flash") \
                                    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(camera_final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 4 - Escribir la salida en un formato "Delta

# CELL ********************

# MAGIC %%sql 
# MAGIC DROP TABLE IF EXISTS camera;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

camera_final_df.write.format("delta").mode("overwrite").saveAsTable("camera")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Select * from camera limit 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
