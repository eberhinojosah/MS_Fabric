# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d9c9b849-d2cb-47c2-9a34-aeefde497789",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "44d09e17-8f0e-43a0-bbb6-a600b3cbd2bb",
# META       "known_lakehouses": [
# META         {
# META           "id": "d9c9b849-d2cb-47c2-9a34-aeefde497789"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

bronze_folder_path = "abfss://Smart_Device_Analytics_WS@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/smart-device/bronze-data"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_folder_path = "abfss://Smart_Device_Analytics_WS@onelake.dfs.fabric.microsoft.com/lh_gold.Lakehouse/Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
