
import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os

def getFromBlock():
    blockMap={}
    
    fileDir = "/mnt/sda1/bowei/sbw/xblock/block/";

    files = [
        "13000000to13249999_Block",
        "13250000to13499999_Block",
        "13500000to13749999_Block",
        "13750000to13999999_Block",
        "14000000to14249999_Block",
        "14250000to14499999_Block",
        "14500000to14749999_Block",
        "14750000to14999999_Block",
        "15000000to15249999_Block",
        "15250000to15499999_Block",
        "15500000to15749999_Block",
        "15750000to15999999_Block"
    ];

    for file in files:
        theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r');
        
        theCSV = theZIP.open(file+"_Info.csv");
        head = theCSV.readline().decode("utf-8").strip();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            blockNumber=oneArray[0]
            timestamp=oneArray[1]
            txFees=float(oneArray[-4])
            burntFees=float(oneArray[-2])
            
            fees=txFees-burntFees

            if blockNumber not in blockMap:
                blockMap_value={"fees":0,"reward":0,"timestamp":timestamp}
            else:
                blockMap_value=blockMap[blockNumber]
                
            blockMap_value["fees"]=fees
            blockMap[blockNumber]=blockMap_value
            oneLine = theCSV.readline().decode("utf-8").strip();
            
            
        theCSV = theZIP.open(file+"_MinerReward.csv");
        head = theCSV.readline().decode("utf-8").strip();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            blockNumber=oneArray[0]
            timestamp=oneArray[1]
            reward=oneArray[3]

            if blockNumber not in blockMap:
                blockMap_value={"fees":0,"reward":0,"timestamp":timestamp}
            else:
                blockMap_value=blockMap[blockNumber]
                
            blockMap_value["reward"]=reward
            blockMap[blockNumber]=blockMap_value
            oneLine = theCSV.readline().decode("utf-8").strip();
            
            
            
        outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/block/csv/block.csv"
        
        fNew = open(outputCsv,'w')
        writer = csv.writer(fNew)
        writer.writerow(["blockNumber","timestamp","reward","fees"])
        for key,value in blockMap.items():
            writer.writerow([key, value["timestamp"], value["reward"], value["fees"]])


def main():
    getFromBlock()

    
if __name__ == '__main__':
    main()