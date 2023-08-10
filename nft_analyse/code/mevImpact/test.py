import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

import datetime
import time
from datetime import timedelta

def date_2_timestamp(year,mon,day):
    tempString=str(year)+"-"+str(mon)+"-"+str(day)
    tempTime=time.strptime(tempString, "%Y-%m-%d")
    return time.mktime(tempTime)

def timestamp_2_date(un_time):
    return datetime.datetime.fromtimestamp(un_time)


def timestamp_removeDay_reduce(temp_timestamp):
    temp_date=timestamp_2_date(temp_timestamp)
    temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,1)
    return temp_timestamp

def timestamp_removeHour_reduce(temp_timestamp):
    temp_date=timestamp_2_date(temp_timestamp)
    temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,temp_date.day)
    return temp_timestamp

def timestamp_removeDay_add(temp_timestamp):
    temp_date=timestamp_2_date(temp_timestamp)
    if temp_date.month==12:
        return date_2_timestamp(temp_date.year+1,1,1)
    else:
        return date_2_timestamp(temp_date.year,temp_date.month+1,1)
    
def date_removeDay_reduce(temp_date):
    temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,1)
    return timestamp_2_date(temp_timestamp)

def date_removeHour_reduce(temp_date):
    temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,temp_date.day)
    return timestamp_2_date(temp_timestamp)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# 返回从最小到最大之间经过的时间戳，间隔为一个月
def max_min_2_timestampList(minDate,maxDate):    
    resList=[]
    resMap={}
    for single_date in daterange(minDate, maxDate):
        temp_date=date_removeDay_reduce(single_date)
        temp_timestamp=date_2_timestamp(temp_date.year,temp_date.month,temp_date.day)
        resMap[temp_timestamp]=1 

    for key in resMap.keys():
        resList.append(key)
    
    # print(resList)
    return sorted(resList)

# 返回从最小到最大之间经过的时间戳，间隔为一天

def max_min_2_timestampList_day(minDate,maxDate):    
    resList=[]
    resMap={}
    for single_date in daterange(minDate, maxDate):
        # temp_date=date_removeHour_reduce(single_date)
        temp_timestamp=date_2_timestamp(single_date.year,single_date.month,single_date.day)
        resMap[temp_timestamp]=1 

    for key in resMap.keys():
        resList.append(key)
    
    # print(resList)
    return sorted(resList)


def timestamp_2_string(temp_timestamp):
    temp_date=datetime.datetime.fromtimestamp(temp_timestamp)
    if temp_date.month<10:
        tempMonth="0"+str(temp_date.month)
    else:
        tempMonth=str(temp_date.month)
    return str(temp_date.year)+"-"+tempMonth

import calendar
def timestamp_2_string_ymd(temp_timestamp):
    temp_date=datetime.datetime.fromtimestamp(temp_timestamp)
    # if temp_date.month<10:
    #     tempMonth="0"+str(temp_date.month)
    # else:
    #     tempMonth=str(temp_date.month)
        
    tempMonth=calendar.month_abbr[int(temp_date.month)]
        
    if temp_date.day<10:
        tempDay="0"+str(temp_date.day)
    else:
        tempDay=str(temp_date.day)
    return tempMonth+"-"+tempDay+"-"+str(temp_date.year)


df=pd.read_csv("/mnt/sde1/geth/nft_analyse_v1/data/mevImpact/csv/combineAll.csv")
df=df[df["coinbase_transfer_usd"]>0]
df=df[df["pureRevenue_usd"]>0]
df=df[df["revenue_dollar"]>0]

minDate=datetime.datetime.fromtimestamp( min(df["timestamp"]) )
maxDate=datetime.datetime.fromtimestamp( max(df["timestamp"]) )
print("max timestamp",max(df["timestamp"]))

timestampMap={}
for index,row in df.iterrows():
    transactionHash=row["transactionHash"]
    timestamp=int(row["timestamp"])
    timestamp=timestamp_removeHour_reduce(timestamp)
    
    coinbase_transfer_usd=float(row["coinbase_transfer_usd"])
    pureRevenue_usd=float(row["pureRevenue_usd"])    
    proportion=(coinbase_transfer_usd/pureRevenue_usd)

    
    if timestamp not in timestampMap:
        timestampMap_value=[]
    else:
        timestampMap_value=timestampMap[timestamp]
    timestampMap_value.append(proportion)
    timestampMap[timestamp]=timestampMap_value
    
list_key=[]
list_key_quchong=[]
list_value=[]
for tempTimestamp in max_min_2_timestampList_day(minDate,maxDate):
    dateString=timestamp_2_string_ymd(tempTimestamp)
    list_key_quchong.append(tempTimestamp)
    
    if tempTimestamp not in timestampMap:
        timestampMap[tempTimestamp]=[]
        list_key.append(tempTimestamp)
        list_value.append(-100)
    else:
        length=len(timestampMap[tempTimestamp])
        list_key.extend( length*[tempTimestamp] )
        list_value.extend(timestampMap[tempTimestamp])
    
for i in range(len(list_key)):
    timestamp=list_key[i]
    value=list_value[i]
    print(timestamp," ",value)