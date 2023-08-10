import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import pandas as pd


def readPunkBidEntered(inputCsv,punkBidEnteredList):    
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1
            
            punkBidEnteredList.append(row)
            
def findPrice(punkBidEnteredList,punkBoughtRow):
    targetBlockNumber=punkBoughtRow[0]
    targetTransactionIndex=punkBoughtRow[2]
    targetPunkIndex=punkBoughtRow[3]
    
    lastPrice="0"
    lastBlockNumber=0
    for row in punkBidEnteredList:
        blockNumber=row[0]
        transactionIndex=row[2]
        punkIndex=row[3]
        price=row[5]
        
        if blockNumber>targetBlockNumber:
            continue
        
        if blockNumber==targetBlockNumber and transactionIndex>targetTransactionIndex:
            continue
        
        if punkIndex!=targetPunkIndex:
            continue
        
        if lastBlockNumber==0:
            lastBlockNumber=blockNumber
            lastPrice=price
            continue
        
        if blockNumber<lastBlockNumber:
            continue
        
        lastBlockNumber=blockNumber
        lastPrice=price
        
    return lastPrice
        
    
            
def repairePunkBought(punkBidEnteredList):
    nftMap={}
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBought.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBought_repaired.csv"
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1
            
            blockNumber=int(row[0])
            transactionHash=row[1]
            transactionIndex=row[2]
            punkIndex=row[3]
            fromAddress=row[4]
            toAddress=row[5]
            price=row[6]
            
            if price=="0":
                price=findPrice(punkBidEnteredList,row)
                row[6]=price
            writer.writerow(row)
            
            
            CryptoPunksMarket="0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
            if CryptoPunksMarket not in nftMap:
                nftMap[CryptoPunksMarket]={}
            
            if punkIndex not in nftMap[CryptoPunksMarket]:
                nftMap[CryptoPunksMarket][punkIndex]={}
            
            nftMap[CryptoPunksMarket][punkIndex][blockNumber]=float(price)/1e18
            
            
    output_dict="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/map/PunkPrice.map"
    with open(output_dict, "wb") as tf:
        pickle.dump(nftMap,tf)
            
            
def main():
    # df0=pd.read_csv("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBidEntered_13_15.csv")
    # df1=pd.read_csv("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBidEntered_15_16.csv")
    # df=pd.concat([df0, df1])
    # df.to_csv("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBidEntered.csv",index=0)
    
    # df0=pd.read_csv("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBought_13_15.csv")
    # df1=pd.read_csv("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBought_15_16.csv")
    # df=pd.concat([df0, df1])
    # df.to_csv("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBought.csv",index=0)
    
    punkBidEnteredList=[]
    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBidEntered.csv"
    readPunkBidEntered(inputCsv,punkBidEnteredList)
    
    repairePunkBought(punkBidEnteredList)



if __name__ == '__main__':
    main()
