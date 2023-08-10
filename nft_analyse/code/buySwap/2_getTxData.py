import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os


def getFromNormal(nftMap,outputPath_dict):
    print("getNormalTransactionFromXblock")
    fileDir = "/mnt/sda1/bowei/sbw/xblock/normalTransaction/";

    files = [
		"13000000to13249999_BlockTransaction",
		"13250000to13499999_BlockTransaction",
		"13500000to13749999_BlockTransaction",
		"13750000to13999999_BlockTransaction",
		"14000000to14249999_BlockTransaction",
		"14250000to14499999_BlockTransaction",
		"14500000to14749999_BlockTransaction",
		"14750000to14999999_BlockTransaction",
        "15000000to15249999_BlockTransaction",
        "15250000to15499999_BlockTransaction",
        "15500000to15749999_BlockTransaction",
        "15750000to15999999_BlockTransaction"
    ];
    
    txMap={}
    
    for file in files:
        print("file",file)
        theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r');
        theCSV = theZIP.open(file+".csv");

        head = theCSV.readline().decode("utf-8").strip();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        curBlockNumber=0
        position=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            
            blockNumber=int(oneArray[0])
            if curBlockNumber==0:
                curBlockNumber=blockNumber
            
            if blockNumber!=curBlockNumber:
                curBlockNumber=blockNumber
                position=0
                            
            if transactionHash in nftMap:

                oneArray.append(position)
                txMap[transactionHash]=oneArray
                    
            position+=1
            oneLine = theCSV.readline().decode("utf-8").strip();
            
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(txMap,tf)
            
        theZIP.close()


def getEoa():
    with open("/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/NftTransferOverOne.map", "rb") as tf:
        nftMap=pickle.load(tf)
    
    outputPath_dict="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/txData.map"
    
    getFromNormal(nftMap,outputPath_dict)

if __name__ == '__main__':
    getEoa()