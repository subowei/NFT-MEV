import pandas as pd
import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle

def getLiquidator(txMap,flashbotsTxMap):
    df=pd.read_csv("/mnt/sde1/peilin_defi/nft_1650w/BendDAO_Transaction.csv")

    bidderMap={}

    liquidateDf=df[df["type"]=="liquidate"]

    for _, row0 in liquidateDf.iterrows():
        loanId=row0["loanId"]
        transactionHash_liquidate=row0["transactionHash"]
        
        # deal auction
        auctionDf=df[(df["loanId"]==loanId) & (df["type"]=="auction")]
        i=0
        for _, row1 in auctionDf.iterrows():
            transactionHash=row1["transactionHash"]
            bidder=row1["onBehalfOf"]
            bidPrice=row1["bidPrice"]
            nftAsset=row1["nftAsset"]
            nftTokenId=row1["nftTokenId"]
            
            newKey=bidder+"_"+transactionHash_liquidate
            
            if newKey not in bidderMap:
                bidderMap_value={"transactions":[],"bidPrice":None,"nftAsset":None,"nftTokenId":None,"isLast":False}
            else:
                bidderMap_value=bidderMap[newKey]
                
            bidderMap_value["transactions"].append(transactionHash)
            bidderMap_value["bidPrice"]=bidPrice
            bidderMap_value["nftAsset"]=nftAsset
            bidderMap_value["nftTokenId"]=nftTokenId
                
            if i==len(auctionDf)-1:
                isLast=True
                bidderMap_value["isLast"]=isLast
                bidderMap_value["transactions"].append(transactionHash_liquidate)
                
            bidderMap[newKey]=bidderMap_value
            
            i+=1

    # output

    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/liquidate/csv/liquidate.csv"
    f = open(outputCsv,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","blockNumber","timestamp","position","transactionFee","coinbase_transfer","flashbots","liquidator","bidPrice","nftAsset","nftTokenId"])
            
    for key, value in bidderMap.items():
        liquidator=key.split("_")[0]
        if value["isLast"]!=True:
            continue
        
        transactionHash=value["transactions"][-1]
        oneArray=txMap[transactionHash]
        blockNumber=oneArray[0]
        timestamp=oneArray[1]
        position=oneArray[-1]
        
        totalFee,totoalMiner,flashbots=dealMultipleTx(value["transactions"],txMap,flashbotsTxMap)
       
        # transactionsStr="_".join(value["transactions"])
        writer.writerow([transactionHash,blockNumber,timestamp,position,totalFee,totoalMiner,flashbots,
                         liquidator,value["bidPrice"],value["nftAsset"],value["nftTokenId"]])
        
    return bidderMap


def dealMultipleTx(txList,txMap,flashbotsTxMap):
    totalFee=0
    totoalMiner=0
    for transactionHash in txList:
        oneArray=txMap[transactionHash]
        blockNumber=oneArray[0]
        timestamp=oneArray[1]
        gasPrice=oneArray[10]
        gasUsed=oneArray[11]
        isError=oneArray[13]
        transactionFee=int(gasPrice)*int(gasUsed)
        flashbots="False"
        totalFee+=transactionFee
                
        if transactionHash in flashbotsTxMap:
            coinbase_transfer=flashbotsTxMap[transactionHash]["coinbase_transfer"]
            totoalMiner+=int(coinbase_transfer)
            
            
            if transactionHash==txList[-1]:
                flashbots="liquidate"
            else:
                flashbots="bid"
            
    return totalFee,totoalMiner,flashbots


def main():
    with open("/mnt/sde1/geth/nft_analyse_v1/data/liquidate/map/txData.map", "rb") as tf:
        txMap=pickle.load(tf)
        
    with open("/mnt/sda1/xblock/flashbots/flashbots_13_16.map", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
        
    getLiquidator(txMap,flashbotsTxMap)
    

if __name__ == '__main__':
    main()
    
    