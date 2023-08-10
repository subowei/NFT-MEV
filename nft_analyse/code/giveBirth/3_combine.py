import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle


def combine():
    f = open("/mnt/sde1/geth/nft_analyse_v1/data/giveBirth/csv/givebirth.csv",'w')
    writer = csv.writer(f)
    writer.writerow(["transactionHash","blockNumber","timestamp","position","isError","callingFunction_internal","callingFunction_normal",
                     "callReward","rewardCount","transactionFee","coinbase_transfer","total_miner_reward","flashbots"])    

    flashbotsTxMap={}
    with open("/mnt/sda1/bowei/sbw/xblock/flashbots/flashbots_13_16.map", "rb") as tf:
        flashbotsTxMap=pickle.load(tf)
        
    with open("/mnt/sde1/geth/nft_analyse_v1/data/giveBirth/map/giveBirth_internal.map", "rb") as tf:
        internalTxMap=pickle.load(tf)
    
    with open("/mnt/sde1/geth/nft_analyse_v1/data/giveBirth/map/giveBirth_normal.map", "rb") as tf:
        normalTxMap=pickle.load(tf)
        
    transactions0=list(normalTxMap.keys())
    transactions1=list(internalTxMap.keys())
    
    transactions=set(transactions0+transactions1)
    
    for transactionHash in transactions:
        normalTxMap_value=normalTxMap[transactionHash]
        blockNumber=normalTxMap_value[0]
        timestamp=normalTxMap_value[1]
        gasPrice=normalTxMap_value[10]
        gasUsed=normalTxMap_value[11]
        callingFunction_normal=normalTxMap_value[12]
        isError=normalTxMap_value[13]
        position=normalTxMap_value[-1]
        transactionFee=str(int(gasPrice)*int(gasUsed))
        callingFunction_internal="none"
        flashbots=False
        coinbase_transfer=0
        total_miner_reward=0
        callReward=0
        rewardCount=0
        
        if transactionHash in internalTxMap:            
            # if "callFunction" not in internalTxMap[transactionHash] or "callReward" not in internalTxMap[transactionHash]:
            #     continue
            
            if internalTxMap[transactionHash]["callFunction"]==None or internalTxMap[transactionHash]["callReward"]==0:
                continue 
            
            callingFunction_internal=internalTxMap[transactionHash]["callFunction"][9]
            callReward=internalTxMap[transactionHash]["callReward"]
            rewardCount=internalTxMap[transactionHash]["rewardCount"]
            
        if isError!="None":
            continue
        
        if callingFunction_normal!="0x88c2a0bf" and callingFunction_internal!="0x88c2a0bf":
            continue
        
        if transactionHash in flashbotsTxMap:
            coinbase_transfer=flashbotsTxMap[transactionHash]["coinbase_transfer"]
            total_miner_reward=flashbotsTxMap[transactionHash]["total_miner_reward"]
            flashbots=True
            
        row=[transactionHash,blockNumber,timestamp,position,isError,callingFunction_internal,callingFunction_normal,callReward,rewardCount,transactionFee,coinbase_transfer,total_miner_reward,flashbots]
        writer.writerow(row)
        
def main():
    combine()
    
if __name__ == '__main__':
    main()