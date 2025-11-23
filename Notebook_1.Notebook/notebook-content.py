# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8cbdebf1-a82c-4e1d-a1e4-1c5aa01f7440",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "189641ee-9dee-49bf-a77f-366a38d83a82",
# META       "known_lakehouses": [
# META         {
# META           "id": "8cbdebf1-a82c-4e1d-a1e4-1c5aa01f7440"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze.dbo.taxi_zone_lookup LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
