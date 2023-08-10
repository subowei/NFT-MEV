import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os


def selectBuySwap():
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap.csv"
    f = open(outputCsv,'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","blockNumber","timestamp","position",
                     "transactionFee","coinbase_transfer","flashbots","toAddress","tokenAddress","tokenId","inEth","outEth"])
    
    flashbotsTxMap={}
    with open("/mnt/sda1/xblock/flashbots/flashbots_13_16.map", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
    
    with open("/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/txData.map", "rb") as tf:
        txMap=pickle.load(tf)
        
    with open("/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/ethTransfer.map", "rb") as tf:
        ethTransferMap=pickle.load(tf)
        
    with open("/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/txWithSwap.map", "rb") as tf:
        swapMap=pickle.load(tf)
        
    with open("/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/NftTransferOverOne.map", "rb") as tf:
        nftMap=pickle.load(tf)
    
    
    for transactionHash,value0 in ethTransferMap.items(): 
        # 1. 如果不存在抵押或者兑换，则跳过       
        if transactionHash not in swapMap or "mortgage" not in swapMap[transactionHash] or "reward" not in swapMap[transactionHash]:
            continue
        
        inEth=0
        outEth=0
        arbitragerTypeList=["txFrom","txTo_owner1","txTo","owner1"]
        for address,value1 in value0.items():
            if value1["addrType"] in arbitragerTypeList:
                inEth+=value1["inValue"]
                outEth+=value1["outValue"]
                
            if "txTo" in value1["addrType"]:
                agentContract=address
        
        oneArray=txMap[transactionHash]
        blockNumber=oneArray[0]
        timestamp=oneArray[1]
        fromAddr=oneArray[3]
        toAddr=oneArray[4]
        gasPrice=oneArray[10]
        gasUsed=oneArray[11]
        isError=oneArray[13]
        position=oneArray[-1]
        coinbase_transfer=0
        transactionFee=int(gasPrice)*int(gasUsed)
        flashbots=False
        
        nftMap_value=nftMap[transactionHash]
        tokenKey=list(nftMap_value.keys())[0]
        tokenAddress=tokenKey.split("_")[0]
        tokenId=tokenKey.split("_")[1]
        
        # 2. 如果转移链只有两个角色则跳过，a->b->c，如果ab都是套利者的地址（b是代理合约），或者如果bc都是套利者地址
        transferList=list(nftMap_value.values())[0]        
        if fromAddr==transferList[0] and toAddr==transferList[1]:
            continue
        if fromAddr==transferList[1] and toAddr==transferList[2]:
            continue
        
        # 3. 第一个seller如果没有收入则跳过
        if transferList[0] not in value0 or value0[transferList[0]]["inAction"]!=True:
            continue
        
        if transactionHash in flashbotsTxMap:
            flashbots=True
            coinbase_transfer=flashbotsTxMap[transactionHash]["coinbase_transfer"]
            
        writer.writerow([transactionHash,blockNumber,timestamp,position,transactionFee,coinbase_transfer,
                         flashbots,toAddr,tokenAddress,tokenId,inEth,outEth])
        
        

if __name__ == '__main__':
    selectBuySwap()