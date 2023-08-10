import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os

            
def getData_fromPunk(txMap,inputCsv):
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
                
            if type(txMap_value)==str:
                txMap_value=txMap_value.split("-")
                
            txMap_value.append("CryptoPunk")
            
            txMap[transactionHash]=txMap_value
            
def outputMap(txMap,outputPath_dict): 
    resMap={}
    
    for key,value in txMap.items():
        if type(value)==list:
            marketTypeString="-".join(value)
            resMap[key]=marketTypeString
        else:
            resMap[key]=value

    with open(outputPath_dict, "wb") as tf:
        pickle.dump(resMap,tf)
        
        
def getData_fromRarible(txMap):
    with open("/mnt/sde1/geth/nft_analyse_v1/data/rarible/map/raribleTxs.map", "rb") as tf:
        tempTxMap=pickle.load(tf)
        
    for transactionHash,value in tempTxMap.items():
        if transactionHash not in txMap:
            txMap_value=[]
        else:
            txMap_value=txMap[transactionHash]
            
        if type(txMap_value)==str:
            txMap_value=txMap_value.split("-")
            
        txMap_value.append("Rarible")
        
        txMap[transactionHash]=txMap_value
        

        
if __name__ == '__main__':
    outputPath_dict="/mnt/sde1/geth/nft_analyse_v1/data/nftMarket/map/txMarket_all.map"
    
    with open("/mnt/sde1/geth/nft_analyse_v1/data/nftMarket/map/txMarket.map", "rb") as tf:
        txMap=pickle.load(tf)
    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBought_repaired.csv"
    getData_fromPunk(txMap,inputCsv)
    
    getData_fromRarible(txMap)

    
    outputMap(txMap,outputPath_dict)
    

    

    
        
        
        
