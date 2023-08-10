import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import pandas as pd


# blockNumber,timestamp,transactionHash,from,to,toCreate,fromIsContract,toIsContract,value,gasLimit,gasPrice,
# gasUsed,callingFunction,isError,eip2718type,baseFeePerGas,maxFeePerGas,maxPriorityFeePerGas
def addStatus():
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/nftFlow.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/nftFlow_addData.csv"
    
    with open("/mnt/sde1/geth/nft_analyse_v1/data/liquidate/map/txData.map", "rb") as tf:
        txMap=pickle.load(tf)
    
    
    with open("/mnt/sda1/xblock/flashbots/flashbots_13_16.map", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)

        header_row.append("timestamp")
        header_row.append("isError_0")
        header_row.append("transactionFee_0")
        header_row.append("from_0")
        header_row.append("to_0")
        header_row.append("flashbots_0")
        header_row.append("isError_1")
        header_row.append("transactionFee_1")
        header_row.append("from_1")
        header_row.append("to_1")
        header_row.append("flashbots_1")
        
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            transactionHash_0=row[7].lower()
            transactionHash_1=row[11].lower()
            
            timestamp=txMap[transactionHash_0][1]
            fromAddr_0=txMap[transactionHash_0][3]
            toAddr_0=txMap[transactionHash_0][4]
            gasPrice=int(txMap[transactionHash_0][10])
            gasUsed=int(txMap[transactionHash_0][11])
            isError_0=txMap[transactionHash_0][13]
            transactionFee_0=str(int(gasPrice)*int(gasUsed))
            
            if transactionHash_0 in flashbotsTxMap:
                flashbots_0="true"
            else:
                flashbots_0="false"
                
            if transactionHash_1 in flashbotsTxMap:
                flashbots_1="true"
            else:
                flashbots_1="false"
            
            
            # print(transactionHash_1)
            if transactionHash_1=="none":
                fromAddr_1="none"
                toAddr_1="none"
                gasPrice="none"
                gasUsed="none"
                isError_1="none"
                transactionFee_1="none"
            else:
                fromAddr_1=txMap[transactionHash_1][3]
                toAddr_1=txMap[transactionHash_1][4]
                gasPrice=int(txMap[transactionHash_1][10])
                gasUsed=int(txMap[transactionHash_1][11])
                isError_1=txMap[transactionHash_1][13]
                transactionFee_1=str(int(gasPrice)*int(gasUsed))
            
            row.append(timestamp)
            row.append(isError_0)
            row.append(transactionFee_0)
            row.append(fromAddr_0)
            row.append(toAddr_0)
            row.append(flashbots_0)
            row.append(isError_1)
            row.append(transactionFee_1)
            row.append(fromAddr_1)
            row.append(toAddr_1)
            row.append(flashbots_1)
            writer.writerow(row)
            
            
def dealData():
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/nftFlow_addData.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/nftFlow_temp.csv"
    
    df=pd.read_csv(inputCsv)
    df=df[df["isError_1"]!="none"]
    df=df[df["isError_1"]!="Out of gas"]
    df=df.drop_duplicates(['transactionHash_0','transactionHash_1'],keep='first')
    
    

    df['positionOriginal_0'] = df['positionOriginal_0'].astype(int)
    df['positionOriginal_1'] = df['positionOriginal_1'].astype(int)
    df['positionEdited_0'] = df['positionEdited_0'].astype(int)
    df['positionEdited_1'] = df['positionEdited_1'].astype(int)
    
    df=df[(df["positionOriginal_0"]<df["positionOriginal_1"]) & (df["positionEdited_0"]>df["positionEdited_1"])]
    
    df.to_csv(outputCsv,index=0)
    
    
import sys
sys.path.append("/mnt/sde1/geth/nft_analyse_v1/code/nftPrice")
from readNftPrice import *

sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal2 import *

# blockNum,tokenAddress,tokenId,startAddr,tokenIdOwnerAddrOriginal,tokenIdOwnerAddrEdited,endAddr_0,transactionHash_0,positionOriginal_0,positionEdited_0,
# endAddr_1,transactionHash_1,positionOriginal_1,positionEdited_1,timestamp,isError_0,transactionFee_0,from_0,to_0,flashbots_0,isError_1,transactionFee_1,from_1,to_1,flashbots_1

def addPrice():
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/nftFlow_temp.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/nftFlow_final.csv"
    
    TOKEN_PRICE_CENTER_NFT = TokenPriceCenter_NFT()
    
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        header_row.append("ethPrice")
        header_row.append("nftPrice_eth")
        header_row.append("nftPrice_usd")
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1
            
            
            
            blockNum=int(row[0])
            tokenAddress=row[1]
            tokenId=row[2]
            timestamp=int(row[14])
            nftPrice_eth=TOKEN_PRICE_CENTER_NFT.swap(tokenAddress,tokenId, blockNum)


            wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            ethPrice=TOKEN_PRICE_CENTER.swap(wethAddress, timestamp,1e18)
            nftPrice_usd=nftPrice_eth*ethPrice

            row.append(ethPrice)
            row.append(nftPrice_eth)
            row.append(nftPrice_usd)
            
            writer.writerow(row)
      
            
def main(): 
    # addStatus(txMap,inputCsv,outputCsv)
    
    dealData()
    
    addPrice()
    
if __name__ == '__main__':
    main()