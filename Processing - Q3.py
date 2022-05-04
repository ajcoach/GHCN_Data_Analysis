# -*- coding: utf-8 -*-
"""
Created on Sat Sep 1 14:45:58 2021

@author: ajc364
"""

### Question 3 a ###

# Extracts country code from each station code in stations nad stores output as new column

stations_with_countryid = stations_data.withColumn("CODE", stations_data.ID.substr(1,2))
stations_with_countryid.show(20, False)
stations_with_countryid.cache()

""" Output
+-----------+--------+---------+---------+-----+------------------------+--------+------------+------+----+
|ID         |LATITUDE|LONGITUDE|ELEVATION|STATE|NAME                    |GSN_FLAG|HCN/CRN_FLAG|WMO_ID|CODE|
+-----------+--------+---------+---------+-----+------------------------+--------+------------+------+----+
|ACW00011604|17.1167 |-61.7833 |10.1     |     |ST JOHNS COOLIDGE FLD   |        |            |      |AC  |
|ACW00011647|17.1333 |-61.7833 |19.2     |     |ST JOHNS                |        |            |      |AC  |
|AE000041196|25.3330 |55.5170  |34.0     |     |SHARJAH INTER. AIRP     |GSN     |            |41196 |AE  |
|AEM00041194|25.2550 |55.3640  |10.4     |     |DUBAI INTL              |        |            |41194 |AE  |
|AEM00041217|24.4330 |54.6510  |26.8     |     |ABU DHABI INTL          |        |            |41217 |AE  |
|AEM00041218|24.2620 |55.6090  |264.9    |     |AL AIN INTL             |        |            |41218 |AE  |
|AF000040930|35.3170 |69.0170  |3366.0   |     |NORTH-SALANG            |GSN     |            |40930 |AF  |
|AFM00040938|34.2100 |62.2280  |977.2    |     |HERAT                   |        |            |40938 |AF  |
|AFM00040948|34.5660 |69.2120  |1791.3   |     |KABUL INTL              |        |            |40948 |AF  |
|AFM00040990|31.5000 |65.8500  |1010.0   |     |KANDAHAR AIRPORT        |        |            |40990 |AF  |
|AG000060390|36.7167 |3.2500   |24.0     |     |ALGER-DAR EL BEIDA      |GSN     |            |60390 |AG  |
|AG000060590|30.5667 |2.8667   |397.0    |     |EL-GOLEA                |GSN     |            |60590 |AG  |
|AG000060611|28.0500 |9.6331   |561.0    |     |IN-AMENAS               |GSN     |            |60611 |AG  |
|AG000060680|22.8000 |5.4331   |1362.0   |     |TAMANRASSET             |GSN     |            |60680 |AG  |
|AGE00135039|35.7297 |0.6500   |50.0     |     |ORAN-HOPITAL MILITAIRE  |        |            |      |AG  |
|AGE00147704|36.9700 |7.7900   |161.0    |     |ANNABA-CAP DE GARDE     |        |            |      |AG  |
|AGE00147705|36.7800 |3.0700   |59.0     |     |ALGIERS-VILLE/UNIVERSITE|        |            |      |AG  |
|AGE00147706|36.8000 |3.0300   |344.0    |     |ALGIERS-BOUZAREAH       |        |            |      |AG  |
|AGE00147707|36.8000 |3.0400   |38.0     |     |ALGIERS-CAP CAXINE      |        |            |      |AG  |
|AGE00147708|36.7200 |4.0500   |222.0    |     |TIZI OUZOU              |        |            |60395 |AG  |
+-----------+--------+---------+---------+-----+------------------------+--------+------------+------+----+
only showing top 20 rows
"""


### Question 3 b ###

# Left join stations with countries using output from part a)

stations_countries = stations_with_countryid.join(countries_data, on = 'CODE', how = "left")
stations_countries.show()

""" Output
+----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
|CODE|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|                NAME|
+----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
|  AC|ACW00011604| 17.1167| -61.7833|     10.1|     |ST JOHNS COOLIDGE...|        |            |      | Antigua and Barbuda|
|  AC|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      | Antigua and Barbuda|
|  AE|AE000041196| 25.3330|  55.5170|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|United Arab Emirates|
|  AE|AEM00041194| 25.2550|  55.3640|     10.4|     |          DUBAI INTL|        |            | 41194|United Arab Emirates|
|  AE|AEM00041217| 24.4330|  54.6510|     26.8|     |      ABU DHABI INTL|        |            | 41217|United Arab Emirates|
|  AE|AEM00041218| 24.2620|  55.6090|    264.9|     |         AL AIN INTL|        |            | 41218|United Arab Emirates|
|  AF|AF000040930| 35.3170|  69.0170|   3366.0|     |        NORTH-SALANG|     GSN|            | 40930|         Afghanistan|
|  AF|AFM00040938| 34.2100|  62.2280|    977.2|     |               HERAT|        |            | 40938|         Afghanistan|
|  AF|AFM00040948| 34.5660|  69.2120|   1791.3|     |          KABUL INTL|        |            | 40948|         Afghanistan|
|  AF|AFM00040990| 31.5000|  65.8500|   1010.0|     |    KANDAHAR AIRPORT|        |            | 40990|         Afghanistan|
|  AG|AG000060390| 36.7167|   3.2500|     24.0|     |  ALGER-DAR EL BEIDA|     GSN|            | 60390|             Algeria|
|  AG|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|             Algeria|
|  AG|AG000060611| 28.0500|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|             Algeria|
|  AG|AG000060680| 22.8000|   5.4331|   1362.0|     |         TAMANRASSET|     GSN|            | 60680|             Algeria|
|  AG|AGE00135039| 35.7297|   0.6500|     50.0|     |ORAN-HOPITAL MILI...|        |            |      |             Algeria|
|  AG|AGE00147704| 36.9700|   7.7900|    161.0|     | ANNABA-CAP DE GARDE|        |            |      |             Algeria|
|  AG|AGE00147705| 36.7800|   3.0700|     59.0|     |ALGIERS-VILLE/UNI...|        |            |      |             Algeria|
|  AG|AGE00147706| 36.8000|   3.0300|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |             Algeria|
|  AG|AGE00147707| 36.8000|   3.0400|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |             Algeria|
|  AG|AGE00147708| 36.7200|   4.0500|    222.0|     |          TIZI OUZOU|        |            | 60395|             Algeria|
+----+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+--------------------+
only showing top 20 rows
"""


### Question 3 c ###

# Left join stations and states

stations_states = stations_data.join(states_data, stations_data.STATE == states_data.CODE, how="left")
stations_states.sort(F.desc('STATE')).show(truncate = False)

""" Output
+-----------+--------+---------+---------+-----+-------------------+--------+------------+------+----+---------------+
|ID         |LATITUDE|LONGITUDE|ELEVATION|STATE|NAME               |GSN_FLAG|HCN/CRN_FLAG|WMO_ID|CODE|NAME           |
+-----------+--------+---------+---------+-----+-------------------+--------+------------+------+----+---------------+
|CA002100100|61.6500 |-137.4833|966.0    |YT   |AISHIHIK A         |        |            |      |YT  |YUKON TERRITORY|
|CA002100366|64.4667 |-140.7333|576.0    |YT   |CLINTON CREEK      |        |            |      |YT  |YUKON TERRITORY|
|CA002100115|60.4667 |-134.8333|820.0    |YT   |ANNIE LAKE ROBINSON|        |            |      |YT  |YUKON TERRITORY|
|CA002100120|62.3667 |-133.3833|1158.0   |YT   |ANVIL              |        |            |      |YT  |YUKON TERRITORY|
|CA002100160|62.4167 |-140.8667|649.0    |YT   |BEAVER CREEK A     |        |            |      |YT  |YUKON TERRITORY|
|CA002100161|62.3667 |-140.8667|663.0    |YT   |BEAVER CREEK YTG   |        |            |      |YT  |YUKON TERRITORY|
|CA002100163|60.0000 |-136.7667|836.0    |YT   |BLANCHARD RIVER    |        |            |      |YT  |YUKON TERRITORY|
|CA002100164|63.9667 |-139.3500|396.0    |YT   |BONANZA CREEK      |        |            |      |YT  |YUKON TERRITORY|
|CA002100165|64.2333 |-140.3500|1036.0   |YT   |BOUNDARY           |        |            |      |YT  |YUKON TERRITORY|
|CA002100167|61.4667 |-135.7833|716.0    |YT   |BRAEBURN           |        |            |      |YT  |YUKON TERRITORY|
|CA002100168|63.0500 |-140.9333|1128.0   |YT   |BRANDT PEAK        |        |            |      |YT  |YUKON TERRITORY|
|CA002100174|60.8667 |-135.3833|686.0    |YT   |BRYN MYRDDIN FARM  |        |            |      |YT  |YUKON TERRITORY|
|CA002100179|61.3667 |-139.0333|807.0    |YT   |BURWASH            |        |            |      |YT  |YUKON TERRITORY|
|CA002100181|61.3667 |-139.0333|805.0    |YT   |BURWASH            |        |            |71001 |YT  |YUKON TERRITORY|
|CA002100182|61.3667 |-139.0500|806.0    |YT   |BURWASH A          |        |            |      |YT  |YUKON TERRITORY|
|CA002100200|60.1667 |-134.7000|660.0    |YT   |CARCROSS           |        |            |      |YT  |YUKON TERRITORY|
|CA002100300|62.1000 |-136.3000|525.0    |YT   |CARMACKS           |        |            |      |YT  |YUKON TERRITORY|
|CA002100301|62.1167 |-136.2000|543.0    |YT   |CARMACKS CS        |        |            |71039 |YT  |YUKON TERRITORY|
|CA002100302|62.1833 |-136.4833|1234.0   |YT   |CARMACKS TOWER     |        |            |      |YT  |YUKON TERRITORY|
|CA002100310|62.7333 |-138.8333|1100.0   |YT   |CASINO CREEK       |        |            |      |YT  |YUKON TERRITORY|
+-----------+--------+---------+---------+-----+-------------------+--------+------------+------+----+---------------+
only showing top 20 rows
"""


### Question 3 d ###


schema = StructType([
    StructField('ID', StringType(), True),
    StructField('FIRSTYEAR', IntegerType(), True),
    StructField('LASTYEAR', IntegerType(), True),
    StructField('ELEMENTSET', ArrayType(StringType()), True)
])



rdd = sc.parallelize(inventory_data.groupBy('ID').agg(F.min(F.col('FIRSTYEAR')),F.max(F.col('LASTYEAR')),F.collect_set('ELEMENT')).collect())


# Create Inventory dataframe

inventory  = spark.createDataFrame(rdd, schema)
inventory.show()

""" Output
+-----------+---------+--------+--------------------+
|         ID|FIRSTYEAR|LASTYEAR|          ELEMENTSET|
+-----------+---------+--------+--------------------+
|ACW00011647|     1957|    1970|[TMAX, TMIN, PRCP...|
|AEM00041217|     1983|    2021|[TMAX, TMIN, PRCP...|
|AG000060590|     1892|    2021|[TMAX, TMIN, PRCP...|
|AGE00147706|     1893|    1920|  [TMAX, TMIN, PRCP]|
|AGE00147708|     1879|    2021|[TMAX, TMIN, PRCP...|
|AGE00147709|     1879|    1938|  [TMAX, TMIN, PRCP]|
|AGE00147710|     1909|    2009|[TMAX, TMIN, PRCP...|
|AGE00147711|     1880|    1938|  [TMAX, TMIN, PRCP]|
|AGE00147714|     1896|    1938|  [TMAX, TMIN, PRCP]|
|AGE00147719|     1888|    2021|[TMAX, TMIN, PRCP...|
|AGM00060351|     1981|    2021|[TMAX, TMIN, PRCP...|
|AGM00060353|     1996|    2019|[TMAX, TMIN, PRCP...|
|AGM00060360|     1945|    2021|[TMAX, TMIN, PRCP...|
|AGM00060387|     1995|    2004|[TMAX, TMIN, PRCP...|
|AGM00060445|     1957|    2021|[TMAX, TMIN, PRCP...|
|AGM00060452|     1985|    2021|[TMAX, TMIN, PRCP...|
|AGM00060467|     1981|    2019|[TMAX, TMIN, PRCP...|
|AGM00060468|     1973|    2021|[TMAX, TMIN, PRCP...|
|AGM00060507|     1943|    2021|[TMAX, TMIN, PRCP...|
|AGM00060511|     1983|    2021|[TMAX, TMIN, PRCP...|
+-----------+---------+--------+--------------------+
only showing top 20 rows
"""


# How many different elements has each station collected overall?

inventory_data.groupBy('ID').agg(F.countDistinct('ELEMENT').alias('Count_of_Elements')).sort(F.desc('Count_of_Elements')).show()

""" Output
+-----------+-----------------+
|         ID|Count_of_Elements|
+-----------+-----------------+
|USW00014607|               62|
|USW00013880|               61|
|USW00023066|               60|
|USW00013958|               59|
|USW00093817|               58|
|USW00024121|               58|
|USW00093058|               57|
|USW00014944|               57|
|USW00024157|               56|
|USW00024127|               56|
|USW00024156|               56|
|USW00025309|               54|
|USW00094849|               54|
|USW00014914|               54|
|USW00093822|               54|
|USW00026510|               54|
|USW00094908|               54|
|USW00003813|               53|
|USW00013722|               53|
|USW00003856|               52|
+-----------+-----------------+
only showing top 20 rows
"""


# Count the number of core elements that each station has collected overall

countOnConditon = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

inventory_data.filter(F.col('ELEMENT').isin(['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']))\
         .groupBy('ID').agg(F.count('ELEMENT')).show()
         
""" Output
+-----------+--------------+
|         ID|count(ELEMENT)|
+-----------+--------------+
|USC00046975|             5|
|USC00047000|             3|
|USC00047011|             5|
|USC00047016|             5|
|USC00047024|             5|
|USC00047070|             5|
|USC00047109|             5|
|USC00047150|             5|
|USC00047228|             5|
|USC00047244|             3|
|USC00047248|             3|
|USC00047273|             3|
|USC00047293|             3|
|USC00047314|             3|
|USC00047351|             3|
|USC00047370|             5|
|USC00047473|             5|
|USC00047600|             3|
|USC00047606|             5|
|USC00047643|             5|
+-----------+--------------+
only showing top 20 rows
"""


# Count the number of other (non-core) elements that each station has collected overall

inventory_data.filter(~F.col('ELEMENT').isin(['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']))\
         .groupBy('ID').agg(F.count('ELEMENT')).show()
         
""" Output
+-----------+--------------+
|         ID|count(ELEMENT)|
+-----------+--------------+
|ACW00011647|             2|
|AEM00041217|             1|
|AG000060590|             1|
|AGE00147708|             1|
|AGE00147710|             1|
|AGE00147719|             1|
|AGM00060351|             1|
|AGM00060353|             1|
|AGM00060360|             1|
|AGM00060387|             1|
|AGM00060445|             1|
|AGM00060452|             1|
|AGM00060467|             1|
|AGM00060468|             1|
|AGM00060507|             1|
|AGM00060511|             1|
|AGM00060535|             1|
|AGM00060540|             1|
|AGM00060602|             1|
|AGM00060603|             1|
+-----------+--------------+
only showing top 20 rows
"""


# How many stations collect all five core elements?

inventory_data.filter(F.col('ELEMENT').isin(['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']))\
         .groupBy('ID').agg(F.count('ELEMENT').alias('CORECOUNT')).filter(F.col('CORECOUNT') == 5).count()
         
""" Output
20289
"""


# How many only collect precipitation?

inventory_single_element = inventory_data.groupBy('ID').count().filter(F.col('count') == 1)
inventory_single_element.join(inventory_data, on = 'ID', how= 'left').filter(F.col('ELEMENT') == 'PRCP').sort('ELEMENT').show()
inventory_single_element.count()

""" Output
+-----------+-----+--------+---------+-------+---------+--------+
|         ID|count|LATITUDE|LONGITUDE|ELEMENT|FIRSTYEAR|LASTYEAR|
+-----------+-----+--------+---------+-------+---------+--------+
|AJ000037636|    1|    41.3|     45.5|   PRCP|     1977|    1991|
|AJ000037729|    1|    40.7|     45.8|   PRCP|     1936|    1987|
|AJ000037835|    1|    40.2|     47.7|   PRCP|     1955|    1992|
|AJ000037861|    1|    40.2|     50.9|   PRCP|     1955|    1991|
|AJ000037883|    1|    39.9|     45.9|   PRCP|     1955|    1987|
|AJ000037888|    1|    39.5|     45.8|   PRCP|     1955|    1988|
|AJ000037893|    1|    39.9|     46.9|   PRCP|     1936|    1987|
|AJ000037946|    1|    39.1|     46.0|   PRCP|     1977|    1991|
|AM000037608|    1|    41.1|     43.7|   PRCP|     1936|    1990|
|AM000037694|    1|    40.9|     44.4|   PRCP|     1963|    1992|
|AM000037772|    1|    40.4|     43.9|   PRCP|     1966|    1992|
|AM000037782|    1|    40.4|     44.3|   PRCP|     1919|    1992|
|AM000037878|    1|    39.8|     45.0|   PRCP|     1960|    1992|
|AM000037887|    1|    39.6|     45.5|   PRCP|     1911|    1976|
|AR000000007|    1|  -30.58|   -58.45|   PRCP|     1981|    2000|
|AR000000010|    1|  -30.68|   -57.82|   PRCP|     1981|    2000|
|AR000000015|    1|  -30.03|    -58.3|   PRCP|     1981|    2000|
|AR000000016|    1|  -29.37|   -58.18|   PRCP|     1981|    2000|
|ASN00002036|    1|-17.3922| 126.2306|   PRCP|     2002|    2021|
|ASN00002043|    1|-18.3368| 126.1243|   PRCP|     2000|    2021|
+-----------+-----+--------+---------+-------+---------+--------+
only showing top 20 rows

Out[46]: 16217
"""


### Question 3 e ###

# Left join stations and output from part d)

stations_inventory = stations_data.join(inventory, on = 'ID', how = "left")#\
stations_inventory.show()

""" Output
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|FIRSTYEAR|LASTYEAR|          ELEMENTSET|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
|ACW00011647| 17.1333| -61.7833|     19.2|     |            ST JOHNS|        |            |      |     1957|    1970|[TMAX, TMIN, PRCP...|
|AEM00041217| 24.4330|  54.6510|     26.8|     |      ABU DHABI INTL|        |            | 41217|     1983|    2021|[TMAX, TMIN, PRCP...|
|AG000060590| 30.5667|   2.8667|    397.0|     |            EL-GOLEA|     GSN|            | 60590|     1892|    2021|[TMAX, TMIN, PRCP...|
|AGE00147706| 36.8000|   3.0300|    344.0|     |   ALGIERS-BOUZAREAH|        |            |      |     1893|    1920|  [TMAX, TMIN, PRCP]|
|AGE00147708| 36.7200|   4.0500|    222.0|     |          TIZI OUZOU|        |            | 60395|     1879|    2021|[TMAX, TMIN, PRCP...|
|AGE00147709| 36.6300|   4.2000|    942.0|     |       FORT NATIONAL|        |            |      |     1879|    1938|  [TMAX, TMIN, PRCP]|
|AGE00147710| 36.7500|   5.1000|      9.0|     |BEJAIA-BOUGIE (PORT)|        |            | 60401|     1909|    2009|[TMAX, TMIN, PRCP...|
|AGE00147711| 36.3697|   6.6200|    660.0|     |         CONSTANTINE|        |            |      |     1880|    1938|  [TMAX, TMIN, PRCP]|
|AGE00147714| 35.7700|   0.8000|     78.0|     |     ORAN-CAP FALCON|        |            |      |     1896|    1938|  [TMAX, TMIN, PRCP]|
|AGE00147719| 33.7997|   2.8900|    767.0|     |            LAGHOUAT|        |            | 60545|     1888|    2021|[TMAX, TMIN, PRCP...|
|AGM00060351| 36.7950|   5.8740|     11.0|     |               JIJEL|        |            | 60351|     1981|    2021|[TMAX, TMIN, PRCP...|
|AGM00060353| 36.8170|   5.8830|      6.0|     |          JIJEL-PORT|        |            | 60353|     1996|    2019|[TMAX, TMIN, PRCP...|
|AGM00060360| 36.8220|   7.8090|      4.9|     |              ANNABA|        |            | 60360|     1945|    2021|[TMAX, TMIN, PRCP...|
|AGM00060387| 36.9170|   3.9500|      8.0|     |              DELLYS|        |            | 60387|     1995|    2004|[TMAX, TMIN, PRCP...|
|AGM00060445| 36.1780|   5.3240|   1050.0|     |     SETIF AIN ARNAT|        |            | 60445|     1957|    2021|[TMAX, TMIN, PRCP...|
|AGM00060452| 35.8170|  -0.2670|      4.0|     |               ARZEW|        |            | 60452|     1985|    2021|[TMAX, TMIN, PRCP...|
|AGM00060467| 35.6670|   4.5000|    442.0|     |              M'SILA|        |            | 60467|     1981|    2019|[TMAX, TMIN, PRCP...|
|AGM00060468| 35.5500|   6.1830|   1052.0|     |               BATNA|        |            | 60468|     1973|    2021|[TMAX, TMIN, PRCP...|
|AGM00060507| 35.2080|   0.1470|    513.9|     |              GHRISS|        |            | 60507|     1943|    2021|[TMAX, TMIN, PRCP...|
|AGM00060511| 35.3410|   1.4630|    989.1|     |          BOU CHEKIF|        |            | 60511|     1983|    2021|[TMAX, TMIN, PRCP...|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
only showing top 20 rows
"""


## In HDFS
hdfs dfs -ls /user/ajc364
hdfs dfs -mkdir ./Assignment1
##


# Write to file in just created folder

stations_inventory.write.mode("overwrite").parquet('./Assignment1/stations_inventory.parquet')


## In HDFS
hdfs dfs -ls /user/ajc364/Assignment1

""" Output
Found 1 items
drwxr-xr-x   - ajc364 ajc364          0 2021-09-11 15:38 /user/ajc364/Assignment1/stations_inventory.parquet
"""
##


df = spark.read.load('./Assignment1/stations_inventory.parquet')
df.show()

""" Output
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
|         ID|LATITUDE|LONGITUDE|ELEVATION|STATE|                NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|FIRSTYEAR|LASTYEAR|          ELEMENTSET|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
|AE000041196| 25.3330|  55.5170|     34.0|     | SHARJAH INTER. AIRP|     GSN|            | 41196|     1944|    2021|[TMAX, TMIN, PRCP...|
|AEM00041218| 24.2620|  55.6090|    264.9|     |         AL AIN INTL|        |            | 41218|     1994|    2021|[TMAX, TMIN, PRCP...|
|AFM00040938| 34.2100|  62.2280|    977.2|     |               HERAT|        |            | 40938|     1973|    2021|[TMAX, TMIN, PRCP...|
|AG000060611| 28.0500|   9.6331|    561.0|     |           IN-AMENAS|     GSN|            | 60611|     1958|    2021|[TMAX, TMIN, PRCP...|
|AGE00147707| 36.8000|   3.0400|     38.0|     |  ALGIERS-CAP CAXINE|        |            |      |     1878|    1879|  [TMAX, TMIN, PRCP]|
|AGE00147715| 35.4200|   8.1197|    863.0|     |             TEBESSA|        |            |      |     1879|    1938|  [TMAX, TMIN, PRCP]|
|AGE00147780| 37.0800|   6.4700|    195.0|     |SKIKDA-CAP BOUGAR...|        |            |      |     1931|    1938|        [TMAX, TMIN]|
|AGE00147794| 36.7800|   5.1000|    225.0|     |   BEJAIA-CAP CARBON|        |            |      |     1926|    1938|        [TMAX, TMIN]|
|AGM00060402| 36.7120|   5.0700|      6.1|     |             SOUMMAM|        |            | 60402|     1973|    2021|[TMAX, TMIN, PRCP...|
|AGM00060405| 36.5000|   7.7170|    111.0|     |          BOUCHEGOUF|        |            | 60405|     2002|    2004|[TMAX, TMIN, PRCP...|
|AGM00060415| 36.3170|   3.5330|    748.0|     |          AIN-BESSAM|        |            | 60415|     2003|    2021|[TMAX, TMIN, PRCP...|
|AGM00060430| 36.3000|   2.2330|    721.0|     |             MILIANA|        |            | 60430|     1957|    2021|[TMAX, TMIN, PRCP...|
|AGM00060437| 36.2830|   2.7330|   1036.0|     |               MEDEA|        |            | 60437|     1995|    2021|[TMAX, TMIN, PRCP...|
|AGM00060461| 35.7000|  -0.6500|     22.0|     |           ORAN-PORT|        |            | 60461|     1995|    2017|[TMAX, TMIN, PRCP...|
|AGM00060475| 35.4320|   8.1210|    811.1|     |CHEIKH LARBI TEBESSI|        |            | 60475|     1958|    2021|[TMAX, TMIN, PRCP...|
|AGM00060514| 35.1670|   2.3170|    801.0|     |       KSAR CHELLALA|        |            | 60514|     1995|    2021|[TMAX, TMIN, PRCP...|
|AGM00060515| 35.3330|   4.2060|    459.0|     |           BOU SAADA|        |            | 60515|     1984|    2021|[TMAX, TMIN, PRCP...|
|AGM00060518| 35.3000|  -1.3500|     70.0|     |            BENI-SAF|        |            | 60518|     1976|    2021|[TMAX, TMIN, PRCP...|
|AGM00060520| 35.2000|  -0.6170|    476.0|     |      SIDI-BEL-ABBES|        |            | 60520|     1995|    2021|[TMAX, TMIN, PRCP...|
|AGM00060549| 33.5360|  -0.2420|   1175.0|     |            MECHERIA|        |            | 60549|     1980|    2021|[TMAX, TMIN, PRCP...|
+-----------+--------+---------+---------+-----+--------------------+--------+------------+------+---------+--------+--------------------+
only showing top 20 rows
"""


### Question 3 f ###

# Left join 1000 rows subset of daily and output from part e)

daily_stations_inventory = daily.join(stations_inventory, on = 'ID', how = "left")
daily_stations_inventory.show()

""" Output
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+--------+---------+---------+-----+---------------+--------+------------+------+---------+--------+--------------------+
|         ID|    DATE|ELEMENT|VALUE|MEASUREMENT_FLAG|QUALITY_FLAG|SOURCE_FLAG|OBSERVATION_TIME|LATITUDE|LONGITUDE|ELEVATION|STATE|           NAME|GSN_FLAG|HCN/CRN_FLAG|WMO_ID|FIRSTYEAR|LASTYEAR|          ELEMENTSET|
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+--------+---------+---------+-----+---------------+--------+------------+------+---------+--------+--------------------+
|AEM00041217|20200101|   TAVG|205.0|               H|        null|          S|            null| 24.4330|  54.6510|     26.8|     | ABU DHABI INTL|        |            | 41217|     1983|    2021|[TMAX, TMIN, PRCP...|
|AG000060590|20200101|   TMIN|-20.0|            null|        null|          S|            null| 30.5667|   2.8667|    397.0|     |       EL-GOLEA|     GSN|            | 60590|     1892|    2021|[TMAX, TMIN, PRCP...|
|AG000060590|20200101|   TAVG| 55.0|               H|        null|          S|            null| 30.5667|   2.8667|    397.0|     |       EL-GOLEA|     GSN|            | 60590|     1892|    2021|[TMAX, TMIN, PRCP...|
|AGE00147708|20200101|   TMIN| 44.0|            null|        null|          S|            null| 36.7200|   4.0500|    222.0|     |     TIZI OUZOU|        |            | 60395|     1879|    2021|[TMAX, TMIN, PRCP...|
|AGE00147708|20200101|   PRCP|  0.0|            null|        null|          S|            null| 36.7200|   4.0500|    222.0|     |     TIZI OUZOU|        |            | 60395|     1879|    2021|[TMAX, TMIN, PRCP...|
|AGE00147708|20200101|   TAVG| 99.0|               H|        null|          S|            null| 36.7200|   4.0500|    222.0|     |     TIZI OUZOU|        |            | 60395|     1879|    2021|[TMAX, TMIN, PRCP...|
|AGE00147719|20200101|   TMAX|142.0|            null|        null|          S|            null| 33.7997|   2.8900|    767.0|     |       LAGHOUAT|        |            | 60545|     1888|    2021|[TMAX, TMIN, PRCP...|
|AGE00147719|20200101|   TMIN|-23.0|            null|        null|          S|            null| 33.7997|   2.8900|    767.0|     |       LAGHOUAT|        |            | 60545|     1888|    2021|[TMAX, TMIN, PRCP...|
|AGE00147719|20200101|   PRCP|  0.0|            null|        null|          S|            null| 33.7997|   2.8900|    767.0|     |       LAGHOUAT|        |            | 60545|     1888|    2021|[TMAX, TMIN, PRCP...|
|AGE00147719|20200101|   TAVG| 61.0|               H|        null|          S|            null| 33.7997|   2.8900|    767.0|     |       LAGHOUAT|        |            | 60545|     1888|    2021|[TMAX, TMIN, PRCP...|
|AGM00060351|20200101|   TMIN| 57.0|            null|        null|          S|            null| 36.7950|   5.8740|     11.0|     |          JIJEL|        |            | 60351|     1981|    2021|[TMAX, TMIN, PRCP...|
|AGM00060351|20200101|   PRCP|  0.0|            null|        null|          S|            null| 36.7950|   5.8740|     11.0|     |          JIJEL|        |            | 60351|     1981|    2021|[TMAX, TMIN, PRCP...|
|AGM00060351|20200101|   TAVG| 94.0|               H|        null|          S|            null| 36.7950|   5.8740|     11.0|     |          JIJEL|        |            | 60351|     1981|    2021|[TMAX, TMIN, PRCP...|
|AGM00060360|20200101|   PRCP|  0.0|            null|        null|          S|            null| 36.8220|   7.8090|      4.9|     |         ANNABA|        |            | 60360|     1945|    2021|[TMAX, TMIN, PRCP...|
|AGM00060360|20200101|   TAVG| 96.0|               H|        null|          S|            null| 36.8220|   7.8090|      4.9|     |         ANNABA|        |            | 60360|     1945|    2021|[TMAX, TMIN, PRCP...|
|AGM00060445|20200101|   TMIN|-33.0|            null|        null|          S|            null| 36.1780|   5.3240|   1050.0|     |SETIF AIN ARNAT|        |            | 60445|     1957|    2021|[TMAX, TMIN, PRCP...|
|AGM00060445|20200101|   TAVG| 23.0|               H|        null|          S|            null| 36.1780|   5.3240|   1050.0|     |SETIF AIN ARNAT|        |            | 60445|     1957|    2021|[TMAX, TMIN, PRCP...|
|AGM00060452|20200101|   TMIN| 81.0|            null|        null|          S|            null| 35.8170|  -0.2670|      4.0|     |          ARZEW|        |            | 60452|     1985|    2021|[TMAX, TMIN, PRCP...|
|AGM00060452|20200101|   PRCP|  0.0|            null|        null|          S|            null| 35.8170|  -0.2670|      4.0|     |          ARZEW|        |            | 60452|     1985|    2021|[TMAX, TMIN, PRCP...|
|AGM00060452|20200101|   TAVG|116.0|               H|        null|          S|            null| 35.8170|  -0.2670|      4.0|     |          ARZEW|        |            | 60452|     1985|    2021|[TMAX, TMIN, PRCP...|
+-----------+--------+-------+-----+----------------+------------+-----------+----------------+--------+---------+---------+-----+---------------+--------+------------+------+---------+--------+--------------------+
only showing top 20 rows
"""


## Are there any stations in your subset of daily that are not in stations at all?

# Left join 1000 observations of daily 2021 with the stations-inventory enriched dataset

daily_1000_stations_inventory = daily.join(stations_inventory, on = 'ID', how = 'left')


# Get distinct station codes

distinct_daily_1000 = daily_1000_stations_inventory.select('ID').distinct()


# Get distinct station codes in the stations data too

distinct_stations = stations_data.select('ID').distinct()


# Subtract distinct daily stations codes from distinct station codes from stations dataset

distinct_daily_1000.subtract(distinct_stations).count()

""" Output
0
"""


## Are there any stations in subset of daily that are not in stations at all?

daily_2021_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/2021.csv.gz")
) 


# Left joining daily_2021_all with stations_inventory

daily_2021_all_stations_inventory = daily_2021_all.join(stations_inventory, on = 'ID', how = 'left')


# Get distinct station codes

distinct_daily_2021 = daily_2021_all_stations_inventory.select('ID').distinct()


# Subtract distinct daily stations codes from the distinct station codes from the stations dataset

distinct_daily_2021.subtract(distinct_stations).count()

""" Output
1
"""


## Could you determine if there are any stations in daily that arenot in stations without using Left join?

daily_2021_all_stations = daily_2021_all.select('ID').distinct()
stations_data_IDs = stations_data.select('ID')
daily_2021_all_stations.subtract(stations_data_IDs).count()

""" Output
1
"""












