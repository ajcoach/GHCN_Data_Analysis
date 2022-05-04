# -*- coding: utf-8 -*-
"""
Created on Sat Sep 1 16:34:03 2021

@author: ajc364
"""

### Question 1 a ###

# How many stations are there in total?

stations_inventory.count()

""" Output 
118493
"""

# How many stations were active in 2020?

inventory_data.filter((F.col('FIRSTYEAR') <= 2020) & (F.col('LASTYEAR') >= 2020)).select('ID').distinct().count()

""" Output
41310
"""

# Using the enriched dataset instead

stations_inventory.filter((F.col('FIRSTYEAR') <= 2020) & (F.col('LASTYEAR') >= 2020)).select('ID').distinct().count()


""" Output
41311
"""


# How many stations are in each of the GCOS Surface Network (GSN), the US Historical Climatology Network (HCN), and the US Climate Reference Network (CRN)? 

stations_inventory.groupBy('GSN_FLAG', 'HCN/CRN_FLAG').count().show()

""" Output
+--------+------------+------+
|GSN_FLAG|HCN/CRN_FLAG| count|
+--------+------------+------+
|     GSN|            |   977|
|     GSN|         HCN|    14|
|        |            |116298|
|        |         HCN|  1204|
+--------+------------+------+
"""

### Question 1 b ###

# Count the total number of stations in each country

country_data = countries_data.selectExpr('CODE', 'NAME as COUNTRYNAME')
stations_country = stations_with_countryid.join(country_data, on = 'CODE', how = "left").groupBy('CODE').count()
countries_data_with_stations_count = country_data.join(stations_country, on='CODE', how = 'left')\
.withColumnRenamed('count', 'NUMBEROFSTATIONS')
countries_data_with_stations_count.sort('CODE').show(truncate = False)

""" Output
+----+------------------------------+----------------+
|CODE|COUNTRYNAME                   |NUMBEROFSTATIONS|
+----+------------------------------+----------------+
|AC  |Antigua and Barbuda           |2               |
|AE  |United Arab Emirates          |4               |
|AF  |Afghanistan                   |4               |
|AG  |Algeria                       |87              |
|AJ  |Azerbaijan                    |66              |
|AL  |Albania                       |3               |
|AM  |Armenia                       |53              |
|AO  |Angola                        |6               |
|AQ  |American Samoa [United States]|21              |
|AR  |Argentina                     |101             |
|AS  |Australia                     |17088           |
|AU  |Austria                       |13              |
|AY  |Antarctica                    |102             |
|BA  |Bahrain                       |1               |
|BB  |Barbados                      |1               |
|BC  |Botswana                      |21              |
|BD  |Bermuda [United Kingdom]      |2               |
|BE  |Belgium                       |1               |
|BF  |Bahamas, The                  |40              |
|BG  |Bangladesh                    |10              |
+----+------------------------------+----------------+
only showing top 20 rows
"""


# Store the output in countries using the withColumnRenamed command.

countries_data_with_stations_count.write.mode("overwrite").parquet('./Assignment1/countries_data_with_stations_count.parquet')


## In HDFS
hdfs dfs -ls /user/ajc364/Assignment1
##

""" Output
Found 2 items
drwxr-xr-x   - ajc364 ajc364          0 2021-09-11 16:45 /user/ajc364/Assignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - ajc364 ajc364          0 2021-09-11 15:38 /user/ajc364/Assignment1/stations_inventory.parquet
"""
##


df = spark.read.load('./Assignment1/countries_data_with_stations_count.parquet')
df.show()

""" Output
+----+--------------------+----------------+
|CODE|         COUNTRYNAME|NUMBEROFSTATIONS|
+----+--------------------+----------------+
|  AL|             Albania|               3|
|  AR|           Argentina|             101|
|  AS|           Australia|           17088|
|  BE|             Belgium|               1|
|  BN|               Benin|               9|
|  BU|            Bulgaria|               4|
|  CE|           Sri Lanka|               6|
|  CF| Congo (Brazzaville)|              17|
|  CG|    Congo (Kinshasa)|              13|
|  CH|               China|             228|
|  CI|               Chile|              21|
|  CO|            Colombia|              27|
|  CS|          Costa Rica|               6|
|  CV|          Cape Verde|               3|
|  EI|             Ireland|              14|
|  GB|               Gabon|              20|
|  GG|             Georgia|             103|
|  GI|Gibraltar [United...|               1|
|  GL| Greenland [Denmark]|              69|
|  GV|              Guinea|              11|
+----+--------------------+----------------+
only showing top 20 rows
"""


# Same for the States

states_names = states_data.selectExpr('CODE', 'NAME as STATENAME')
stations_states = stations_inventory.join(states_names, stations_data.STATE == states_names.CODE, how = 'left')
states_data_with_stations_count = stations_states.groupBy('STATE', 'STATENAME').count().withColumnRenamed('count', 'NUMBEROFSTATIONS')
states_data_with_stations_count.sort('STATE').show(truncate = False)


""" Output
+-----+--------------------+----------------+
|STATE|STATENAME           |NUMBEROFSTATIONS|
+-----+--------------------+----------------+
|     |null                |43969           |
|AB   |ALBERTA             |1428            |
|AK   |ALASKA              |1003            |
|AL   |ALABAMA             |1012            |
|AR   |ARKANSAS            |885             |
|AS   |AMERICAN SAMOA      |21              |
|AZ   |ARIZONA             |1534            |
|BC   |BRITISH COLUMBIA    |1688            |
|BH   |null                |34              |
|CA   |CALIFORNIA          |2879            |
|CO   |COLORADO            |4310            |
|CT   |CONNECTICUT         |350             |
|DC   |DISTRICT OF COLUMBIA|15              |
|DE   |DELAWARE            |124             |
|FL   |FLORIDA             |1877            |
|FM   |MICRONESIA          |38              |
|GA   |GEORGIA             |1261            |
|GU   |GUAM                |21              |
|HI   |HAWAII              |752             |
|IA   |IOWA                |905             |
+-----+--------------------+----------------+
only showing top 20 rows
"""


# Save a copy of each table

states_data_with_stations_count.write.mode("overwrite").parquet('./Assignment1/states_data_with_stations_count.parquet')

## In HDFS
hdfs dfs -ls /user/ajc364/Assignment1
##

""" Output
Found 3 items
drwxr-xr-x   - ajc364 ajc364          0 2021-09-11 16:45 /user/ajc364/Assignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - ajc364 ajc364          0 2021-09-11 16:59 /user/ajc364/Assignment1/states_data_with_stations_count.parquet
drwxr-xr-x   - ajc364 ajc364          0 2021-09-11 15:38 /user/ajc364/Assignment1/stations_inventory.parquet
"""
##


df = spark.read.load('./Assignment1/states_data_with_stations_count.parquet')
df.show()

""" Output
+-----+--------------------+----------------+
|STATE|           STATENAME|NUMBEROFSTATIONS|
+-----+--------------------+----------------+
|   AS|      AMERICAN SAMOA|              21|
|   NU|             NUNAVUT|             146|
|   NS|         NOVA SCOTIA|             365|
|   PE|PRINCE EDWARD ISLAND|              84|
|   FM|          MICRONESIA|              38|
|   GU|                GUAM|              21|
|   NE|            NEBRASKA|            2259|
|   FL|             FLORIDA|            1877|
|   HI|              HAWAII|             752|
|   IA|                IOWA|             905|
|   KS|              KANSAS|            1994|
|   MO|            MISSOURI|            1471|
|   NJ|          NEW JERSEY|             739|
|   NV|              NEVADA|             684|
|   OK|            OKLAHOMA|            1018|
|     |                null|           43969|
|   SA|                null|               1|
|   VT|             VERMONT|             333|
|   WA|          WASHINGTON|            1506|
|   WI|           WISCONSIN|            1102|
+-----+--------------------+----------------+
only showing top 20 rows
"""


### Question 1 c ###

# How many stations are there in the Southern Hemisphere only?

stations_inventory.filter(F.col('LATITUDE') < 0).count()

""" Output
Out[62]: 25296
"""


# How many stations are there in total in the territories of the United States around the world, excluding the United States itself?

countries_data_with_stations_count.filter(F.col('COUNTRYNAME').contains('[United States]')).agg(F.sum('NUMBEROFSTATIONS')).show()

""" Output
+---------------------+
|sum(NUMBEROFSTATIONS)|
+---------------------+
|                  337|
+---------------------+
"""


# Displays the territories from above

countries_data_with_stations_count.filter(F.col('COUNTRYNAME').contains('[United States]')).show(truncate = False)

""" Output
+----+----------------------------------------+----------------+
|CODE|COUNTRYNAME                             |NUMBEROFSTATIONS|
+----+----------------------------------------+----------------+
|AQ  |American Samoa [United States]          |21              |
|CQ  |Northern Mariana Islands [United States]|11              |
|LQ  |Palmyra Atoll [United States]           |3               |
|GQ  |Guam [United States]                    |21              |
|WQ  |Wake Island [United States]             |1               |
|JQ  |Johnston Atoll [United States]          |4               |
|VQ  |Virgin Islands [United States]          |54              |
|RQ  |Puerto Rico [United States]             |222             |
+----+----------------------------------------+----------------+
"""

