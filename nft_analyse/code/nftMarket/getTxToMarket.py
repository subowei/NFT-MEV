import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os


            
def getData_fromZip(txMap):
    dir= "/mnt/sde1/peilin_defi/nft_1650w/"
    for fileName in os.listdir(dir):
        marketType=fileName.split(".")[0]
        filePath=dir+fileName
        if ".zip" not in filePath:
            continue
        
        getTxs_fromZip(txMap,filePath,marketType)
        
        
def getTxs_fromZip(txMap,filePath_zip,marketType):
    print(filePath_zip)
    theZIP = zipfile.ZipFile(filePath_zip, 'r')
    for fileName in theZIP.namelist():
        if "Transaction" not in fileName:
            continue
        
        theCSV = theZIP.open(fileName);

        head = theCSV.readline().decode("utf-8").strip();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            
            if transactionHash not in txMap:
                txMap_value=[]
            else:
                txMap_value=txMap[transactionHash]
                
            txMap_value.append(marketType)
            
            txMap[transactionHash]=txMap_value
                
            oneLine = theCSV.readline().decode("utf-8").strip();
            
            
def getData_fromSeaport(txMap,inputCsv):
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            transactionHash=row[1]
            if transactionHash not in txMap:
                txMap_value=[]
            else:
                txMap_value=txMap[transactionHash]
                
            txMap_value.append("OpenSea")
            
            txMap[transactionHash]=txMap_value
            
def outputMap(txMap,outputPath_dict): 
    resMap={}
    
    for key,value in txMap.items():
        marketTypeString="-".join(value)
        resMap[key]=marketTypeString

    with open(outputPath_dict, "wb") as tf:
        pickle.dump(resMap,tf)

        
if __name__ == '__main__':
    txMap={}
    outputPath_dict="/mnt/sde1/geth/nft_analyse_v1/data/nftMarket/map/txMarket.map"
    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/csv/nftTradefromsSeaport_13_15.csv"
    getData_fromSeaport(txMap,inputCsv)
    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/csv/nftTradefromsSeaport_15_16.csv"
    getData_fromSeaport(txMap,inputCsv)
    
    
    outputMap(txMap,outputPath_dict)
    
    getData_fromZip(txMap)
    
    outputMap(txMap,outputPath_dict)
    

    

    
        
        
        
