# -*- coding: utf-8 -*-
"""
Created on Sun Sep  6 19:35:54 2020

@author: Dsk
"""


# import datetime
from datetime import datetime, timedelta
import time
import datetime,pandas as pd
from datetime import date
from time import strptime
import calendar

start_date = datetime.datetime.now()-datetime.timedelta(days=10)
end_date = datetime.datetime.now()


# get year from date
year = start_date.strftime("%Y")
print("Year:", year)

# get month from date
month = start_date.strftime("%m")
print("Month;", month)

# get day from date
day = start_date.strftime("%d")
print("Day:", day)

Dt_prcs=datetime.datetime(int(year), int(month), int(day), 0, 0)
calc=calendar.timegm(Dt_prcs.timetuple())

print("Time since epoch for start_date :"+str(start_date)+" is "+str(calc*1000))
dt_object1 = datetime.datetime.fromtimestamp(calc)
print(" Date of dt_object1 :"+str(dt_object1))

print("Time +180000 since epoch for start_date :"+str(start_date)+" is "+str((calc*1000)+1800000))
print("calc in secs "+str(round(time.time() * 1000)))

expirationTime=int(round(pd.to_datetime(start_date).value / 1000000))-18000000
print(" time in code " +str(expirationTime))
