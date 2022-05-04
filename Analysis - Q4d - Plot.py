# -*- coding: utf-8 -*-
"""
Created on Fri Sep 17 12:16:48 2021

@author: ajc364
"""

### Question 4 d Plot ###

import pandas as pd
import glob
import os
import matplotlib.pylab as plt

 

path = r'C:\Users\coach\Documents\Master of Applied Data Science\DATA 420\Tests\Assignment 1\daily_all_nz_T_elements.csv'
all_files = glob.glob(os.path.join(path, "*.csv"))     # advisable to use os.path.join as this makes concatenation OS independent

li = []


# Integrating all files

for file in all_files:

    df = pd.read_csv(file)
    li.append(df)


totalData = pd.concat(li, axis = 0, ignore_index = True)
totalData['DATE'] =  pd.to_datetime(totalData['DATE'], format = '%Y%m%d')
totalData['YEAR'] = pd.DatetimeIndex(totalData['DATE']).year
totalData['DEGREES'] = totalData.VALUE * 0.1


# Plotting for each station of New Zealand

for key, grp in totalData.groupby(['NAME']):
    grp = grp.pivot(index = 'DATE', columns = 'ELEMENT', values = 'DEGREES')
    grp.plot(color=['coral', 'lightgreen'])
    plt.title(key)
    plt.ylabel('Temperature (in Degrees Celsius)')
    plt.legend(loc='upper right', bbox_to_anchor=(1, 1))
    plt.tight_layout()
    plt.show()

   
# Average of TMIN and TMAX for whole data plot

averageData = totalData.groupby(['YEAR', 'ELEMENT']).mean().reset_index()

plt_avg = averageData.pivot(index='YEAR', columns='ELEMENT', values='VALUE')
plt_avg.plot(color=['coral', 'lightgreen'])
plt.title("New Zealands Average Temperatures Over the Years")
plt.ylabel('Temperature (in Degrees Celsius)')
plt.legend(loc='upper right', bbox_to_anchor=(1, 1))
plt.tight_layout()
plt.show()