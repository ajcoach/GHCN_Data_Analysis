# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 11:52:47 2021

@author: ajc364
"""


### Question 3 a ###

# Determine the default blocksize of HDFS

# In HDFS
hdfs getconf -confKey 'dfs.blocksize'
#

""" Output
134217728
"""


# How many blocks are required for the daily climate summaries for the year 2021

# In HDFS
hdfs fsck /data/ghcnd/daily/2021.csv.gz -files -blocks
#

""" Output
/data/ghcnd/daily/2021.csv.gz 81744280 bytes, replicated: replication=8, 1 block(s):  OK
0. BP-700027894-132.181.129.68-1626517177804:blk_1073744891_4067 len=81744280 Live_repl=8


Status: HEALTHY
 Number of data-nodes:  32
 Number of racks:               1
 Total dirs:                    0
 Total symlinks:                0

Replicated Blocks:
 Total size:    81744280 B
 Total files:   1
 Total blocks (validated):      1 (avg. block size 81744280 B)
 Minimally replicated blocks:   1 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    4
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
 Blocks queued for replication: 0

Erasure Coded Block Groups:
 Total size:    0 B
 Total files:   0
 Total block groups (validated):        0
 Minimally erasure-coded block groups:  0
 Over-erasure-coded block groups:       0
 Under-erasure-coded block groups:      0
 Unsatisfactory placement block groups: 0
 Average block group size:      0.0
 Missing block groups:          0
 Corrupt block groups:          0
 Missing internal blocks:       0
 Blocks queued for replication: 0
FSCK ended at Wed Sep 15 12:01:45 NZST 2021 in 1 milliseconds


The filesystem under path '/data/ghcnd/daily/2021.csv.gz' is HEALTHY
"""


# What about the year 2015? 

# In HDFS
hdfs fsck /data/ghcnd/daily/2015.csv.gz -files -blocks
#

""" Output
/data/ghcnd/daily/2015.csv.gz 207618101 bytes, replicated: replication=8, 2 block(s):  OK
0. BP-700027894-132.181.129.68-1626517177804:blk_1073744657_3833 len=134217728 Live_repl=8
1. BP-700027894-132.181.129.68-1626517177804:blk_1073744658_3834 len=73400373 Live_repl=8


Status: HEALTHY
 Number of data-nodes:  32
 Number of racks:               1
 Total dirs:                    0
 Total symlinks:                0

Replicated Blocks:
 Total size:    207618101 B
 Total files:   1
 Total blocks (validated):      2 (avg. block size 103809050 B)
 Minimally replicated blocks:   2 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    4
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
 Blocks queued for replication: 0

Erasure Coded Block Groups:
 Total size:    0 B
 Total files:   0
 Total block groups (validated):        0
 Minimally erasure-coded block groups:  0
 Over-erasure-coded block groups:       0
 Under-erasure-coded block groups:      0
 Unsatisfactory placement block groups: 0
 Average block group size:      0.0
 Missing block groups:          0
 Corrupt block groups:          0
 Missing internal blocks:       0
 Blocks queued for replication: 0
FSCK ended at Wed Sep 15 12:07:40 NZST 2021 in 1 milliseconds


The filesystem under path '/data/ghcnd/daily/2015.csv.gz' is HEALTHY
"""


### Question 3 b ###

# Load and count the number of observations in 2015 

daily_2015_obsv = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/2015.csv.gz")
)
daily_2015_obsv.count()

""" Output
34899014
"""


# Then separately in 2021

daily_2021_obsv = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false") 
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/2021.csv.gz")
)
daily_2021_obsv.count()

""" Output
19099479
"""


# How many tasks were executed by each stage of each job?


### Question 3 c ###

# Load and count the number of observations from 2015 to 2021 (inclusive)

daily_2015_to_2021 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(daily_schema)
    .load("hdfs:////data/ghcnd/daily/20{15,16,17,18,19,20,21}.csv.gz")
)
daily_2015_to_2021.count()

""" Output
228659433
"""

