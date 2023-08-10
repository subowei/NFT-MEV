import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle

            
# 第三步骤：获取交易对应的外部to地址，交易手续费
# ['blockNumber', 'timestamp', 'transactionHash', 'from', 'to', 'toCreate', 
# 'fromIsContract', 'toIsContract', 'value', 'gasLimit', 'gasPrice', 'gasUsed', 
# 'callingFunction', 'isError', 'eip2718type', 'baseFeePerGas', 'maxFeePerGas', 'maxPriorityFeePerGas']
def getNormalTransactionFromXblock(internalTxMap,valid_contract_address):
    print("getNormalTransactionFromXblock")
    
    txMap={}
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
            toAddr=oneArray[4]
            callingFunction=oneArray[12]
            
            blockNumber=int(oneArray[0])
            if curBlockNumber==0:
                curBlockNumber=blockNumber
            
            if blockNumber!=curBlockNumber:
                curBlockNumber=blockNumber
                position=0
                
            oneArray.append(str(position))
            
            # if toAddr==valid_contract_address and callingFunction=="0x88c2a0bf":
            if toAddr==valid_contract_address:
                txMap[transactionHash]=oneArray
                
            if transactionHash in internalTxMap:
                txMap[transactionHash]=oneArray
                    
            position+=1
            oneLine = theCSV.readline().decode("utf-8").strip();
        theZIP.close()
        
        with open("/mnt/sde1/geth/nft_analyse_v1/data/giveBirth/map/giveBirth_normal.map", "wb") as tf:
            pickle.dump(txMap,tf)
        


def cryptoKitty():
    valid_contract_address="0x06012c8cf97bead5deae237070f9587f8e7a266d"

    with open("/mnt/sde1/geth/nft_analyse_v1/data/giveBirth/map/giveBirth_internal.map", "rb") as tf:
        internalTxMap=pickle.load(tf)

    getNormalTransactionFromXblock(internalTxMap,valid_contract_address)
    
    
    
if __name__ == '__main__':
    cryptoKitty()