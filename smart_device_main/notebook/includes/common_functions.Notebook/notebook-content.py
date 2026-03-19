# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def merge_delta_lake(input_df, lh_gold_name, schema_name, table_name,gold_folder_path, merge_condition, partition_column):

    from delta.tables import DeltaTable

    if (spark._jsparkSession.catalog().tableExists(f"{lh_gold_name}.{schema_name}.{table_name}")):

        deltaTablePeople = DeltaTable.forPath(spark, f'{gold_folder_path}/{schema_name}/{table_name}')

        deltaTablePeople.alias('tgt') \
        .merge(
            input_df.alias('src'),
            merge_condition
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    else:
        input_df.write.format("delta") \
                        .mode("overwrite") \
                        .partitionBy(partition_column) \
                        .saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
