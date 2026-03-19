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

# ##### **Ingestion del archivo "model.csv"**

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

# MARKDOWN ********************

# ### Paso 1 - Leer el archivo CSV usadno "DataframeReader" de Spark

# CELL ********************

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_schema = StructType(fields= [
    StructField("ModelID", IntegerType(), False),
    StructField("CategoryID", IntegerType(), True),  
    StructField("Model", StringType(), True),  
    StructField("Codename", StringType(), True),  
    StructField("GeneralExtras", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_df = spark.read \
            .option("header", True) \
            .option("delimiter", ";") \
            .schema(model_schema) \
            .csv(f"{bronze_folder_path}/{p_file_date}/model.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Paso 2 Seleccionar solo las columnas requeridas 

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_selected_df = model_df.select(col("ModelID"), col("CategoryID"), col("Model"), col("Codename"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 3 Cambiar el nombre de las columnas segun lo requerido

# CELL ********************

model_renamed_df = model_selected_df.withColumnRenamed("ModelID", "model_id") \
                                    .withColumnRenamed("CategoryID", "category_id") \
                                    .withColumnRenamed("Model", "model") \
                                    .withColumnRenamed("Codename", "code_name") 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 4 Agregar las columnas "Ingestion date" y "environmet"

# CELL ********************

from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_final_df = add_ingestion_date(model_renamed_df) \
                 .withColumn("environment", lit(p_environment)) \
                 .withColumn("file_date", lit(p_file_date))
                                     

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Paso 5 Escribir los datos en el "lakehouse" en formato delta

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS model;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_final_df.write.format("delta").mode("overwrite").saveAsTable("model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM lh_silver.dbo.model LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
