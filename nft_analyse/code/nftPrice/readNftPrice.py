import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os
import datetime
import time
from datetime import timedelta
import pandas as pd



class TokenPriceCenter_NFT():
    def __init__(self):
        self.nftMap={}
        with open("/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/map/Combine.map", "rb") as tf:
            self.nftMap=pickle.load(tf)
            
    def findFloorPrice(self,tokenAddress):
        priceList=[]
        for tokenId, tempMap in self.nftMap[tokenAddress].items():
            for blockNumber, price in tempMap.items():
                if price!=0:
                    priceList.append(price)
                
        priceList.sort()
                
        if len(priceList)==0:
            return 0
        if len(priceList)==1:
            return priceList[0]
        
        if len(priceList)>=2:   
            if priceList[0]!=0 and priceList[1]/priceList[0]>100:
                return priceList[1]
            if priceList[0]==0:
                return priceList[1]
            
            return priceList[0]
        

    def swap(self, tokenAddress,tokenId, targetBlockNumber):
        targetBlockNumber=int(targetBlockNumber)
        try:
            return self.nftMap[tokenAddress][tokenId][targetBlockNumber]
        except:
            pass
        
        if tokenAddress not in self.nftMap:
            return 0
        
        if tokenId not in self.nftMap[tokenAddress]:
            return self.findFloorPrice(tokenAddress)
        
        
        blockNumberList=list(self.nftMap[tokenAddress][tokenId].keys())
        blockNumberList.sort()
        
        minDis=0
        minDisBlockNumber=0
        # 优先找指定时间后面的价格，其次选择离目标最近的价格
        for blockNumber in blockNumberList:
            if blockNumber>targetBlockNumber:
                return self.nftMap[tokenAddress][tokenId][blockNumber]
            
        for blockNumber in blockNumberList:
            if minDisBlockNumber==0:
                minDisBlockNumber=blockNumber
                minDis=abs(minDisBlockNumber-targetBlockNumber)
            else:
                tempDis=abs(blockNumber-targetBlockNumber)
                if minDis>tempDis:
                    minDis=tempDis
                    minDisBlockNumber=blockNumber
        
        return self.nftMap[tokenAddress][tokenId][minDisBlockNumber]
    
    
    def findFeaturePrice(self, tokenAddress,tokenId, targetBlockNumber):
        if tokenId not in self.nftMap[tokenAddress]:
            return self.findFloorPrice(tokenAddress)
        
        blockNumberList=list(self.nftMap[tokenAddress][tokenId].keys())
        blockNumberList.sort()
        
        for blockNumber in blockNumberList:
            if blockNumber>targetBlockNumber:
                return self.nftMap[tokenAddress][tokenId][blockNumber]
            
        return 0

