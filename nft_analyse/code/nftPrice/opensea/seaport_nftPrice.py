
import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os



def getData_fromSeaport(nftMap,inputCsv):
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            blockNumber=int(row[0])
            transactionHash=row[1]
            tokenAddress=row[5]
            tokenId=row[6]
            price=float(row[7])
                
            if tokenAddress not in nftMap:
                nftMap[tokenAddress]={}
            
            if tokenId not in nftMap[tokenAddress]:
                nftMap[tokenAddress][tokenId]={}
            
            nftMap[tokenAddress][tokenId][blockNumber]=price
            
            
if __name__ == '__main__':
    nftMap={}
    outputPath_dict="/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/map/Seaport.map"
    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/csv/nftTradefromsSeaport_13_15.csv"
    getData_fromSeaport(nftMap,inputCsv)
    with open(outputPath_dict, "wb") as tf:
        pickle.dump(nftMap,tf)
    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/csv/nftTradefromsSeaport_15_16.csv"
    getData_fromSeaport(nftMap,inputCsv)
    with open(outputPath_dict, "wb") as tf:
        pickle.dump(nftMap,tf)