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

def combineMap(resMap,tempMap):
    for tokenAddress,value in tempMap.items():
        if tokenAddress not in resMap:
            resMap[tokenAddress]=value
            continue
            
        for tokenId, timestampMap in value.items():
            if tokenId not in resMap[tokenAddress]:
                resMap[tokenAddress][tokenId]=timestampMap
                continue
                
            for timestamp,price in timestampMap.items():
                resMap[tokenAddress][tokenId][timestamp]=price


def combine():
    
    with open("/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/map/OpenSea.map", "rb") as tf:
        nftMap0=pickle.load(tf) 
        
    with open("/mnt/sde1/peilin_defi/data/nftPrice/dict/LooksRare.map", "rb") as tf:
        nftMap1=pickle.load(tf) 
        
    with open("/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/map/Seaport.map", "rb") as tf:
        nftMap2=pickle.load(tf) 
        
    with open("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/map/PunkPrice.map", "rb") as tf:
        nftMap3=pickle.load(tf) 
        
    resMap={}
    combineMap(resMap,nftMap0)
    combineMap(resMap,nftMap1)
    combineMap(resMap,nftMap2)
    combineMap(resMap,nftMap3)

    output_dict="/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/map/Combine.map"
    with open(output_dict, "wb") as tf:
        pickle.dump(resMap,tf)
            
combine()
        