import pandas as pd
import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle


def getTx():
    txMap={}
    df=pd.read_csv("/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/nftFlow.csv")
    for index,row in df.iterrows():
        transactionHash_0=row["transactionHash_0"]
        transactionHash_1=row["transactionHash_1"]
        txMap[transactionHash_0]=1
        txMap[transactionHash_1]=1
    return txMap

def getFromNormal(txMap):
    print("getNormalTransactionFromXblock")
    fileDir = "/mnt/sda1/xblock/normalTransaction/";

    files = [
		"13000000to13249999_BlockTransaction",
		"13250000to13499999_BlockTransaction",
		"13500000to13749999_BlockTransaction",
		"13750000to13999999_BlockTransaction",
		"14000000to14249999_BlockTransaction",
		"14250000to14499999_BlockTransaction",
		# "14500000to14749999_BlockTransaction",
		# "14750000to14999999_BlockTransaction",
        # "15000000to15249999_BlockTransaction",
        # "15250000to15499999_BlockTransaction",
        # "15500000to15749999_BlockTransaction",
        # "15750000to15999999_BlockTransaction",
        # "16000000to16249999_BlockTransaction",
        # "16250000to16499999_BlockTransaction"
    ];

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
            # gasPrice=oneArray[10]
            # gasUsed=oneArray[11]
            # isError=oneArray[13]
            # timestamp=oneArray[1]
            
            blockNumber=int(oneArray[0])
            if curBlockNumber==0:
                curBlockNumber=blockNumber
            
            if blockNumber!=curBlockNumber:
                curBlockNumber=blockNumber
                position=0
                            
            if transactionHash in txMap:
                # txMap_value=txMap[transactionHash]
                # transactionFee=str(int(gasPrice)*int(gasUsed))
                # txMap_value["transactionFee"]=transactionFee
                # txMap_value["isError"]=isError
                # txMap_value["blockNumber"]=blockNumber
                # txMap_value["position"]=position
                # txMap_value["timestamp"]=timestamp
                
                oneArray.append(position)
                txMap[transactionHash]=oneArray
                    
            position+=1
            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
        
        with open("/mnt/sde1/geth/nft_analyse_v1/data/nftflow/map/txData.map", "wb") as tf:
            pickle.dump(txMap,tf)
            

def main():
    txMap=getTx()
    getFromNormal(txMap)
            
if __name__ == '__main__':
    main()