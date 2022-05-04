# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 15:05:28 2021

@author: ajc364
"""

# Increasing executers and cores

start_pyspark_shell -e 2 -c 1 -w 2 -m 2
start_pyspark_shell -e 4 -c 2 -w 4 -m 4
start_pyspark_shell -e 8 -c 4 -w 8 -m 8


spark.conf.set("spark.executor.instances", 4)
spark.conf.set("spark.executor.cores", 4)

start_pyspark_shell(4, 2, 4, 4)



### Question 4 a ###

# Count the number of rows in daily

daily_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/*.csv.gz")
)
daily_all.count()

""" Output
2978405055
"""


# Question 4 b ##

# How many observations are there for each of the five core elements? 

observations_for_core_elements = daily_all.filter(F.col('ELEMENT').\
isin(['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD'])).groupBy('ELEMENT').agg(F.count('ID'))
observations_for_core_elements.show()

""" Output
+-------+----------+
|ELEMENT| count(ID)|
+-------+----------+
|   TMAX| 442969361|
|   SNOW| 338791588|
|   SNWD| 287930120|
|   PRCP|1037260588|
|   TMIN| 441606080|
+-------+----------+

"""


## Question 4 c ##

# Determine how many observations of TMIN do not have a corresponding observation of TMAX

# Function to check if station ID collected TMIN
def stationTMIN(elementset):
    if(len(elementset) == 1 and elementset[0] == 'TMIN'):
        return True
    return False

udf_stationTMIN = F.udf(stationTMIN)


# Filter all daily observations for TMIN & TMAX in ELEMENT

daily_all_tmin_tmax = daily_all.filter(F.col('ELEMENT').isin({'TMIN', 'TMAX'}))


# Collects all the elements per stations ID in a day

daily_all_t_elements = daily_all_tmin_tmax.groupBy('ID', 'DATE').agg(F.collect_set('ELEMENT').alias('ELEMENTSET'))


# Finds if an observation collected TMIN

daily_all_tmin = daily_all_t_elements.withColumn('stationTMIN', udf_stationTMIN('ELEMENTSET'))


# Filter to get the ones that collects TMIN, but not TMAX

daily_all_tmin.filter(F.col('stationTMIN') == True).count()


""" Output
8689146
"""


# How many different stations contributed to these observations?

daily_all_tmin.filter(F.col('stationTMIN') == True).select('ID').distinct().count()

"""Output
27610
"""



## Question 4 d ##

daily_all_nz = daily_all.withColumn("COUNTRYCODE", daily_all.ID.substr(1,2))\
.withColumn('YEAR', daily_all.DATE.substr(1,4)).filter(F.col('COUNTRYCODE') == 'NZ')



daily_all_nz_T_elements = daily_all_nz.filter(F.col('ELEMENT').isin(['TMIN', 'TMAX']))



# LEFT Join with stations_nz data for station name

daily_all_nz_T_stations = daily_all_nz_T_elements.join(NZ_stations_data, on = "ID", how = "left")


daily_all_nz_T_stations.write.mode("overwrite").option('header', True).csv('./Assignment1/daily_all_nz_T_elements.csv')


# In HDFS
hdfs dfs -ls user/ajc364/Assignment1
#

"""Output
Found 7 items
drwxr-xr-x   - ajc364 ajc364          0 2021-09-16 13:25 /user/ajc364/Assignment1/NZ_station_distance.parquet
drwxr-xr-x   - ajc364 ajc364          0 2021-09-16 14:18 /user/ajc364/Assignment1/average_rainfall.csv
drwxr-xr-x   - ajc364 ajc364          0 2021-09-17 09:51 /user/ajc364/Assignment1/average_rainfall_by_country.csv
drwxr-xr-x   - ajc364 ajc364          0 2021-09-14 13:34 /user/ajc364/Assignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - ajc364 ajc364          0 2021-09-17 12:12 /user/ajc364/Assignment1/daily_all_nz_T_elements.csv
drwxr-xr-x   - ajc364 ajc364          0 2021-09-14 14:07 /user/ajc364/Assignment1/states_data_with_stations_count.parquet
drwxr-xr-x   - ajc364 ajc364          0 2021-09-14 13:16 /user/ajc364/Assignment1/stations_inventory.parquet
"""


df = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", True)
    .load('./Assignment1/daily_all_nz_T_elements.csv')
)
df.show()

""" Output
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----------+----+-------------------+--------+---------+
|         ID|    DATE|ELEMENT|VALUE|MEASUREMENT_FLAG|QUALITY_FLAG|SOURCE_FLAG|OBSERVATION_TIME|COUNTRYCODE|YEAR|               NAME|LATITUDE|LONGITUDE|
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----------+----+-------------------+--------+---------+
|NZ000936150|20030101|   TMAX|183.0|            null|        null|          G|            null|         NZ|2003| HOKITIKA AERODROME| -42.717|  170.983|
|NZ000936150|20030101|   TMIN|124.0|            null|        null|          G|            null|         NZ|2003| HOKITIKA AERODROME| -42.717|  170.983|
|NZ000093012|20030101|   TMAX|247.0|            null|        null|          G|            null|         NZ|2003|            KAITAIA|   -35.1|  173.267|
|NZ000093012|20030101|   TMIN|142.0|            null|        null|          G|            null|         NZ|2003|            KAITAIA|   -35.1|  173.267|
|NZ000939870|20030101|   TMAX|163.0|            null|        null|          G|            null|         NZ|2003|CHATHAM ISLANDS AWS|  -43.95| -176.567|
|NZ000939870|20030101|   TMIN|127.0|            null|        null|          G|            null|         NZ|2003|CHATHAM ISLANDS AWS|  -43.95| -176.567|
|NZM00093110|20030101|   TMAX|259.0|            null|        null|          S|            null|         NZ|2003|  AUCKLAND AERO AWS|   -37.0|    174.8|
|NZM00093110|20030101|   TMIN|127.0|            null|        null|          S|            null|         NZ|2003|  AUCKLAND AERO AWS|   -37.0|    174.8|
|NZM00093678|20030101|   TMAX|192.0|            null|        null|          S|            null|         NZ|2003|           KAIKOURA| -42.417|    173.7|
|NZ000093292|20030101|   TMAX|252.0|            null|        null|          G|            null|         NZ|2003| GISBORNE AERODROME|  -38.65|  177.983|
|NZ000093292|20030101|   TMIN|105.0|            null|        null|          G|            null|         NZ|2003| GISBORNE AERODROME|  -38.65|  177.983|
|NZ000093994|20030101|   TMAX|242.0|            null|        null|          G|            null|         NZ|2003| RAOUL ISL/KERMADEC|  -29.25| -177.917|
|NZ000093994|20030101|   TMIN|210.0|            null|        null|          G|            null|         NZ|2003| RAOUL ISL/KERMADEC|  -29.25| -177.917|
|NZ000937470|20030101|   TMAX|319.0|            null|        null|          G|            null|         NZ|2003|         TARA HILLS| -44.517|    169.9|
|NZ000937470|20030101|   TMIN| 93.0|            null|        null|          G|            null|         NZ|2003|         TARA HILLS| -44.517|    169.9|
|NZ000939450|20030101|   TMAX|111.0|            null|        null|          G|            null|         NZ|2003|CAMPBELL ISLAND AWS|  -52.55|  169.167|
|NZ000939450|20030101|   TMIN| 84.0|            null|        null|          G|            null|         NZ|2003|CAMPBELL ISLAND AWS|  -52.55|  169.167|
|NZM00093781|20030101|   TMAX|218.0|            null|        null|          S|            null|         NZ|2003|  CHRISTCHURCH INTL| -43.489|  172.532|
|NZM00093439|20030101|   TMAX|228.0|            null|        null|          S|            null|         NZ|2003|WELLINGTON AERO AWS| -41.333|    174.8|
|NZM00093439|20030101|   TMIN|134.0|            null|        null|          S|            null|         NZ|2003|WELLINGTON AERO AWS| -41.333|    174.8|
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+-----------+----+-------------------+--------+---------+
only showing top 20 rows
"""


# How many observations are there?

daily_all_nz_T_elements.count()

""" Output
468192
"""

# How many years are covered by the observations?

daily_all_nz_T_elements.select("YEAR").distinct().count()

""" Output
82
"""

# Copy file to local

hdfs dfs -copyToLocal ./Assignment1/daily_all_nz_T_elements.csv ./

color=['coral', 'lightgreen']

# Count num of rows in part files

cat  ./daily_all_nz_T_elements.csv/part* | wc -l

""" Output
468274
"""


### Question 4 e ###

# Add country code and year columns to the daily_ all df

daily_all_with_year = daily_all.withColumn("CODE", daily_all.ID.substr(1,2))\
.withColumn('YEAR', daily_all.DATE.substr(1,4))


# Join with country table for country name

daily_all_with_year_countryname = daily_all_with_year.join(countries_data, on = 'CODE', how = 'left')


# Filter precipitation records and group by year and country

average_rainfall = daily_all_with_year_countryname.filter(F.col('ELEMENT') == 'PRCP')\
                  .groupBy('YEAR', 'CODE', 'NAME').agg(F.avg('VALUE').alias('Average_Rainfall'))
    


# Save results as parquet

average_rainfall.write.mode("overwrite").option('header', True).csv('./Assignment1/average_rainfall.csv')


# In HDFS
hdfs dfs -ls /user/ajc364/Assignment1
#

""" Output
Found 6 items
drwxr-xr-x   - ajc364 ajc364          0 2021-09-16 13:25 /user/ajc364/Assignment1/NZ_station_distance.parquet
drwxr-xr-x   - ajc364 ajc364          0 2021-09-16 13:03 /user/ajc364/Assignment1/average_rainfall.csv
drwxr-xr-x   - ajc364 ajc364          0 2021-09-14 13:34 /user/ajc364/Assignment1/countries_data_with_stations_count.parquet
drwxr-xr-x   - ajc364 ajc364          0 2021-09-15 17:02 /user/ajc364/Assignment1/daily_all_nz_T_elements.csv
drwxr-xr-x   - ajc364 ajc364          0 2021-09-14 14:07 /user/ajc364/Assignment1/states_data_with_stations_count.parquet
drwxr-xr-x   - ajc364 ajc364          0 2021-09-14 13:16 /user/ajc364/Assignment1/stations_inventory.parquet
"""


# Check file

df = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", True)
    .load('./Assignment1/average_rainfall.csv')
)

df.sort('Average_rainfall').show()
    
    
""" Output
+----+----+--------------------+-------------------+
|YEAR|CODE|                NAME|   Average_Rainfall|
+----+----+--------------------+-------------------+
|1874|  UK|      United Kingdom|-1.0767123287671232|
|1899|  MI|              Malawi|                0.0|
|1846|  NL|         Netherlands|                0.0|
|1996|  MC|         Macau S.A.R|                0.0|
|2002|  MV|            Maldives|                0.0|
|2013|  TI|          Tajikistan|                0.0|
|1840|  US|       United States|                0.0|
|2005|  WQ|Wake Island [Unit...|                0.0|
|1985|  KU|              Kuwait|                0.0|
|1981|  AE|United Arab Emirates|                0.0|
|1982|  CV|          Cape Verde|                0.0|
|1974|  CK|Cocos (Keeling) I...|                0.0|
|1991|  SL|        Sierra Leone|                0.0|
|1976|  CK|Cocos (Keeling) I...|                0.0|
|1985|  MC|         Macau S.A.R|                0.0|
|2003|  RW|              Rwanda|                0.0|
|1984|  WI|      Western Sahara|                0.0|
|1999|  JO|              Jordan|                0.0|
|1987|  AF|         Afghanistan|                0.0|
|1999|  KU|              Kuwait|                0.0|
+----+----+--------------------+-------------------+
only showing top 20 rows
"""


# Check which country has highest rainfall in a year

average_rainfall.sort(F.desc('Average_rainfall')).show(truncate = False)

""" Output
+----+----+-------------------+------------------+
|YEAR|CODE|NAME               |Average_Rainfall  |
+----+----+-------------------+------------------+
|2000|EK  |Equatorial Guinea  |4361.0            |
|1975|DR  |Dominican Republic |3414.0            |
|1974|LA  |Laos               |2480.5            |
|1978|BH  |Belize             |2244.714285714286 |
|1979|NN  |Sint Maarten       |1967.0            |
|1974|CS  |Costa Rica         |1820.0            |
|1979|BH  |Belize             |1755.5454545454545|
|1973|NS  |Suriname           |1710.0            |
|1978|UC  |Curacao            |1675.0384615384614|
|1977|BH  |Belize             |1541.7142857142858|
|1978|HO  |Honduras           |1469.6122448979593|
|1977|UC  |Curacao            |1442.5384615384614|
|1978|NN  |Sint Maarten       |1292.8695652173913|
|1977|HO  |Honduras           |1284.138888888889 |
|1978|TD  |Trinidad and Tobago|1265.0            |
|1976|GY  |Guyana             |1213.3333333333333|
|1979|UC  |Curacao            |1168.2            |
|1973|TS  |Tunisia            |1162.0            |
|2006|BM  |Burma              |1152.0            |
|2001|EK  |Equatorial Guinea  |1100.0            |
+----+----+-------------------+------------------+
only showing top 20 rows
"""


# Average rainfall per country in 2021 needed for plotting

avg_rainfall_2021 = average_rainfall.filter(F.col('YEAR') == '2021').sort('CODE')


# Shows created data

avg_rainfall_2021.show()

""" Output
+----+----+--------------------+------------------+
|YEAR|CODE|                NAME|  Average_Rainfall|
+----+----+--------------------+------------------+
|2021|  AE|United Arab Emirates|               0.0|
|2021|  AF|         Afghanistan|              33.2|
|2021|  AG|             Algeria| 8.560311856428362|
|2021|  AJ|          Azerbaijan|113.14343928280358|
|2021|  AM|             Armenia|30.640529531568227|
|2021|  AO|              Angola|            124.04|
|2021|  AQ|American Samoa [U...|104.43429487179488|
|2021|  AR|           Argentina| 65.11223344556677|
|2021|  AS|           Australia|26.269825840856242|
|2021|  AU|             Austria| 89.35034013605443|
|2021|  AY|          Antarctica|19.531794248683678|
|2021|  BA|             Bahrain|0.7272727272727273|
|2021|  BC|            Botswana|59.254980079681275|
|2021|  BD|Bermuda [United K...| 49.02717391304348|
|2021|  BE|             Belgium|31.514150943396228|
|2021|  BF|        Bahamas, The|21.066941297631306|
|2021|  BG|          Bangladesh|               0.0|
|2021|  BH|              Belize|31.844660194174757|
|2021|  BK|Bosnia and Herzeg...| 30.82608695652174|
|2021|  BL|             Bolivia| 91.34436008676789|
+----+----+--------------------+------------------+
only showing top 20 rows
"""


# Write to file 

avg_rainfall_2021.write.mode("overwrite").option('header', True).csv('./Assignment1/average_rainfall_2021.csv')


# Read file

avg_rainfall_by_country = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", True)
    .load('./Assignment1/average_rainfall_2021.csv')
)
avg_rainfall_by_country.show()


# Copy to local

hdfs dfs -copyToLocal ./Assignment1/average_rainfall_2021.csv ./







