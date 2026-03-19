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

# ### Ingestion de la carpeta "display"

# MARKDOWN ********************

# ##### Parte 1 - Leer los archivos CSV usando "DataframeReader" de Spark

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

display_schema = StructType(fields=[
                StructField("DisplayID", IntegerType(), False),
                StructField("DisplayHole", StringType(), True),
                StructField("DisplayDiagonal", StringType(), True),
                StructField("Resolution", StringType(), True),
                StructField("DisplayType", StringType(), True),
                StructField("DisplayColorDepth", StringType(), True),
                StructField("DisplayIllumination", StringType(), True),
                StructField("DisplayRefreshRate", StringType(), True)])

#DisplayID;DisplayHole;DisplayDiagonal;Resolution;DisplayType;DisplayColorDepth;DisplayIllumination;DisplayRefreshRate

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display_df = spark.read \
            .option("header", True) \
            .option("delimiter", ";") \
            .schema(display_schema) \
            .csv(f"{bronze_folder_path}/{p_file_date}/display/display_*.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(display_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 2 - Renombrar las columnas y añadir nuevas columnas
# - 1.- "DisplayId" renombrar a "display_id"
# - 2.- "Resolution" renombrar a "resolution"
# - 3.- "DisplayColorDepth" renombrar a "color_depth"
# - 4.- "DisplayIllumination" renombrar a "illumination"
# - 5.- "DisplayRefreshRate" renombrar a "refresh_rate"
# - 6.- Agregar las columnas "ingestion_date" y "environment"

# CELL ********************

from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display_with_columns_df = add_ingestion_date(display_df) \
                        .withColumn("environment", lit(p_environment)) \
                        .withColumn("file_date", lit(p_file_date)) \
                        .withColumnRenamed("DisplayId", "display_id") \
                        .withColumnRenamed("Resolution", "resolution") \
                        .withColumnRenamed("DisplayColorDepth", "color_depth") \
                        .withColumnRenamed("DisplayIllumination", "illumination") \
                        .withColumnRenamed("DisplayRefreshRate", "refresh_rate")
                        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 3 - Eliminar las columnas no deseadas del DataFrame
# - 1.- DisplayHole
# - 2.- DisplayDiagonal
# - 3.- DisplayType

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display_final_df = display_with_columns_df.drop(col("DisplayHole"),
                                                col("DisplayDiagonal"),
                                                col("DisplayType"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(display_final_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Paso 4 - Escribir los datos en el "lakehouse" en formato "Delta

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS display;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display_final_df.write.format("delta").mode("overwrite").saveAsTable("display")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Select * from display limit 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
