import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os

# ·····················································································
# blockNumber,timestamp,transactionHash,typeTraceAddress,from,to,fromIsContract,toIsContract,value,callingFunction,isError
def getFromInternal(nftMap,etherTransferMap,txMap,outputPath_dict):
    
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
            typeTraceAddress=oneArray[3]
            fromAddr=oneArray[4]
            toAddr=oneArray[5]
            internalValue=float(oneArray[8])
            wethAddr="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            
            if "delegatecall" in typeTraceAddress:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue  
            
            if internalValue==0 or fromAddr==wethAddr or toAddr==wethAddr:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            if transactionHash not in nftMap:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            nftMap_value=nftMap[transactionHash]
            
            if len(nftMap_value.keys())>1:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            tokenAddress=list(nftMap_value.keys())[0]
            transferList=list(nftMap_value.values())[0]

            if len(transferList)!=3:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            if transactionHash not in etherTransferMap:
                etherTransferMap_value={}
            else:
                etherTransferMap_value=etherTransferMap[transactionHash]
            
            tx_fromAddr=txMap[transactionHash][3]
            tx_toAddr=txMap[transactionHash][4]
            
            # 忽略套利者地址之间的的转账
            arbitragerAddrList=[tx_fromAddr,tx_toAddr,transferList[1]]
            if fromAddr in arbitragerAddrList and toAddr in arbitragerAddrList:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
                                    
            if fromAddr in transferList or fromAddr in arbitragerAddrList:
                if fromAddr==tx_fromAddr:
                    addrType="txFrom"
                elif fromAddr==tx_toAddr and fromAddr==transferList[1]:
                    addrType="txTo_owner1"
                elif fromAddr==tx_toAddr and fromAddr!=transferList[1]:
                    addrType="txTo"
                elif fromAddr==transferList[0]:
                    addrType="owner0"
                elif fromAddr==transferList[1]:
                    addrType="owner1"
                elif fromAddr==transferList[2]:
                    addrType="owner2"
                    
                if fromAddr not in etherTransferMap_value.keys():
                    etherTransferMap_value[fromAddr]={"inValue":0,"inAction":None,"outValue":0,"outAction":None,"addrType":addrType}
                
                etherTransferMap_value[fromAddr]["outValue"]+=internalValue
                etherTransferMap_value[fromAddr]["outAction"]=True
                
                etherTransferMap[transactionHash]=etherTransferMap_value
            
            if toAddr in transferList or toAddr in arbitragerAddrList:
                if toAddr==tx_fromAddr:
                    addrType="txFrom"
                elif toAddr==tx_toAddr and toAddr==transferList[1]:
                    addrType="txTo_owner1"
                elif toAddr==tx_toAddr and toAddr!=transferList[1]:
                    addrType="txTo"
                elif toAddr==transferList[0]:
                    addrType="owner0"
                elif toAddr==transferList[1]:
                    addrType="owner1"
                elif toAddr==transferList[2]:
                    addrType="owner2"
                    
                if toAddr not in etherTransferMap_value.keys():
                    etherTransferMap_value[toAddr]={"inValue":0,"inAction":None,"outValue":0,"outAction":None,"addrType":addrType}
                
                etherTransferMap_value[toAddr]["inValue"]+=internalValue
                etherTransferMap_value[toAddr]["inAction"]=True

                etherTransferMap[transactionHash]=etherTransferMap_value
                
            oneLine = theCSV.readline().decode("utf-8").strip();
        
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(etherTransferMap,tf)
            
# blockNumber,timestamp,transactionHash,tokenAddress,from,to,fromIsContract,toIsContract,value
def getFromERC20(nftMap,etherTransferMap,txMap,outputPath_dict):
    fileDir = "/mnt/sda1/xblock/erc20/";

    files = [
        "13000000to13249999_ERC20Transaction",
        "13250000to13499999_ERC20Transaction",
        "13500000to13749999_ERC20Transaction",
        "13750000to13999999_ERC20Transaction",
        "14000000to14249999_ERC20Transaction",
        "14250000to14499999_ERC20Transaction",
        "14500000to14749999_ERC20Transaction",
        "14750000to14999999_ERC20Transaction",
        "15000000to15249999_ERC20Transaction",
        "15250000to15499999_ERC20Transaction",
        "15500000to15749999_ERC20Transaction",
        "15750000to15999999_ERC20Transaction"
    ];

    for file in files:
        theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r');
        theCSV = theZIP.open(file+".csv");

        head = theCSV.readline().decode("utf-8").strip();
        oneLine = theCSV.readline().decode("utf-8").strip();
        i=0
        while (oneLine!=""):
            if i%1000000==0:
                print(i)
            i+=1
            oneArray = oneLine.split(",")
            transactionHash=oneArray[2]
            tokenAddr=oneArray[3]
            fromAddr=oneArray[4]
            toAddr=oneArray[5]
            erc20Value=float(oneArray[8])
            wethAddr="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

        
            if erc20Value==0 or tokenAddr!=wethAddr or fromAddr==wethAddr or toAddr==wethAddr:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            if transactionHash not in nftMap:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            nftMap_value=nftMap[transactionHash]

            if len(nftMap_value.keys())>1:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            tokenAddress=list(nftMap_value.keys())[0]
            transferList=list(nftMap_value.values())[0]
            
            # only consider nft transfer twice
            if len(transferList)!=3:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            if transactionHash not in etherTransferMap:
                etherTransferMap_value={}
            else:
                etherTransferMap_value=etherTransferMap[transactionHash]
                
            tx_fromAddr=txMap[transactionHash][3]
            tx_toAddr=txMap[transactionHash][4]
            
            # 防止与内部交易重复计算
            # if toAddr==tx_fromAddr:
            #     oneLine = theCSV.readline().decode("utf-8").strip();
            #     continue
            
            # 忽略套利者地址之间的的转账
            arbitragerAddrList=[tx_fromAddr,tx_toAddr,transferList[1]]
            if fromAddr in arbitragerAddrList and toAddr in arbitragerAddrList:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
                   
            if fromAddr in transferList or fromAddr in arbitragerAddrList:
                if fromAddr==tx_fromAddr:
                    addrType="txFrom"
                elif fromAddr==tx_toAddr and fromAddr==transferList[1]:
                    addrType="txTo_owner1"
                elif fromAddr==tx_toAddr and fromAddr!=transferList[1]:
                    addrType="txTo"
                elif fromAddr==transferList[0]:
                    addrType="owner0"
                elif fromAddr==transferList[1]:
                    addrType="owner1"
                elif fromAddr==transferList[2]:
                    addrType="owner2"
                    
                if fromAddr not in etherTransferMap_value.keys():
                    etherTransferMap_value[fromAddr]={"inValue":0,"inAction":None,"outValue":0,"outAction":None,"addrType":addrType}
                
                etherTransferMap_value[fromAddr]["outValue"]+=erc20Value
                etherTransferMap_value[fromAddr]["outAction"]=True
                
                etherTransferMap[transactionHash]=etherTransferMap_value
            
            if toAddr in transferList or toAddr in arbitragerAddrList:
                if toAddr==tx_fromAddr:
                    addrType="txFrom"
                elif toAddr==tx_toAddr and toAddr==transferList[1]:
                    addrType="txTo_owner1"
                elif toAddr==tx_toAddr and toAddr!=transferList[1]:
                    addrType="txTo"
                elif toAddr==transferList[0]:
                    addrType="owner0"
                elif toAddr==transferList[1]:
                    addrType="owner1"
                elif toAddr==transferList[2]:
                    addrType="owner2"
                    
                if toAddr not in etherTransferMap_value.keys():
                    etherTransferMap_value[toAddr]={"inValue":0,"inAction":None,"outValue":0,"outAction":None,"addrType":addrType}
                
                etherTransferMap_value[toAddr]["inValue"]+=erc20Value
                etherTransferMap_value[toAddr]["inAction"]=True
                
                etherTransferMap[transactionHash]=etherTransferMap_value
            
            oneLine = theCSV.readline().decode("utf-8").strip();
            
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(etherTransferMap,tf)
            
            
def getEthTransfer():
    with open("/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/NftTransferOverOne.map", "rb") as tf:
        nftMap=pickle.load(tf)
    
    outputPath_dict="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/ethTransfer.map"
    
    txMap={}
    with open("/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/txData.map", "rb") as tf:
        txMap=pickle.load(tf)
        
    
    etherTransferMap={}
    getFromInternal(nftMap,etherTransferMap,txMap,outputPath_dict)
    getFromERC20(nftMap,etherTransferMap,txMap,outputPath_dict)


def main():
    getEthTransfer()
    
    

if __name__ == '__main__':
    main()