import pandas as pd
import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle


def getFirstBid(txMap,flashbotsTxMap):
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/liquidate/csv/firstBid.csv"
    f = open(outputCsv,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","blockNumber","timestamp","position",
                     "transactionFee","coinbase_transfer","flashbots","bidder","penalty"])
            
    df=pd.read_csv("/mnt/sde1/peilin_defi/nft_1650w/BendDAO_Transaction.csv")
    repayDf=df[df["type"]=="repay"]
    for _, row0 in repayDf.iterrows():
        timestamp_repay=int(row0["timestamp"])
        loanId=row0["loanId"]
        transactionHash_repay=row0["transactionHash"]
        
        # deal auction
        auctionDf=df[(df["loanId"]==loanId) & (df["type"]=="auction")]
        redeemDf=df[(df["loanId"]==loanId) & (df["type"]=="redeem")]
        
        if len(auctionDf)==1:
            timestamp_auction=int(auctionDf.iloc[0]["timestamp"])
            if timestamp_repay>=timestamp_auction:
                penalty=redeemDf.iloc[0]["fineAmount"]
                transactionHash_auction=auctionDf.iloc[0]["transactionHash"]
                onBehalfOf=auctionDf.iloc[0]["onBehalfOf"]
                oneArray=txMap[transactionHash_auction]
                blockNumber=oneArray[0]
                timestamp=oneArray[1]
                gasPrice=oneArray[10]
                gasUsed=oneArray[11]
                isError=oneArray[13]
                position=oneArray[-1]
                coinbase_transfer=0
                transactionFee=int(gasPrice)*int(gasUsed)
                flashbots=False
                
                if transactionHash_auction in flashbotsTxMap:
                    flashbots=True
                    coinbase_transfer=flashbotsTxMap[transactionHash_auction]["coinbase_transfer"]
                    
                writer.writerow([transactionHash_auction,blockNumber,timestamp,position,
                                transactionFee,coinbase_transfer,flashbots,onBehalfOf,penalty])

            
            
def main():
    with open("/mnt/sde1/geth/nft_analyse_v1/data/liquidate/map/txData.map", "rb") as tf:
        txMap=pickle.load(tf)
        
    with open("/mnt/sda1/xblock/flashbots/flashbots_13_16.map", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
        
    getFirstBid(txMap,flashbotsTxMap)
    

if __name__ == '__main__':
    main()
    