import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle

# blockNumber,timestamp,transactionHash,typeTraceAddress,from,to,fromIsContract,toIsContract,value,callingFunction,isError
def getInterTransactionFromXblock(valid_contract_address):
    print("getInterTransactionFromXblock")
    
    txMap={}
    fileDir = "/mnt/sda1/xblock/internalTransaction/";
    files = [
		"13000000to13249999_InternalTransaction",
		"13250000to13499999_InternalTransaction",
		"13500000to13749999_InternalTransaction",
		"13750000to13999999_InternalTransaction",
        "14000000to14249999_InternalTransaction",
        "14250000to14499999_InternalTransaction",
        "14500000to14749999_InternalTransaction",
        "14750000to14999999_InternalTransaction",
        "15000000to15249999_InternalTransaction",
        "15250000to15499999_InternalTransaction",
        "15500000to15749999_InternalTransaction",
        "15750000to15999999_InternalTransaction"
    ];
    for file in files:
        print("file",file)
        theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r');
        theCSV = theZIP.open(file+".csv");

        head = theCSV.readline();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            fromAddr=oneArray[4]
            toAddr=oneArray[5]
            callingFunction=oneArray[9]
            
            if toAddr==valid_contract_address and callingFunction=="0x88c2a0bf":
                if transactionHash in txMap.keys():
                    value=txMap[transactionHash]
                else:
                    value={"callFunction":None,"callReward":0, "rewardCount":0}
                value["callFunction"]=oneArray
                txMap[transactionHash]=value
                    
            if fromAddr==valid_contract_address:
                if transactionHash in txMap.keys():
                    value=txMap[transactionHash]
                else:
                    value={"callFunction":None,"callReward":0, "rewardCount":0}
                
                if int(oneArray[8])!=0:
                    value["callReward"]+=int(oneArray[8])
                    value["rewardCount"]+=1
                    
                txMap[transactionHash]=value
                
            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
        
        with open("/mnt/sde1/geth/nft_analyse_v1/data/giveBirth/map/giveBirth_internal.map", "wb") as tf:
            pickle.dump(txMap,tf)
            
def cryptoKitty():
    valid_contract_address="0x06012c8cf97bead5deae237070f9587f8e7a266d"
        
    getInterTransactionFromXblock(valid_contract_address)
    
if __name__ == '__main__':
    cryptoKitty()