
import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os

# blockNumber,timestamp,transactionHash,tokenAddress,from,to,fromIsContract,toIsContract,tokenId        
def getFromERC721(outputPath_dict):
    nftMap={}
    nftMap_new={}
    fileDir = "/mnt/sda1/xblock/erc721/";

    files = [
        "13000000to13249999_ERC721Transaction",
        "13250000to13499999_ERC721Transaction",
        "13500000to13749999_ERC721Transaction",
        "13750000to13999999_ERC721Transaction",
        "14000000to14249999_ERC721Transaction",
        "14250000to14499999_ERC721Transaction",
        "14500000to14749999_ERC721Transaction",
        "14750000to14999999_ERC721Transaction",
        "15000000to15249999_ERC721Transaction",
        "15250000to15499999_ERC721Transaction",
        "15500000to15749999_ERC721Transaction",
        "15750000to15999999_ERC721Transaction"
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
            tokenAddress=oneArray[3]
            fromAddress=oneArray[4]
            toAdddress=oneArray[5]
            tokenId=oneArray[8]
            
            zeroAddress="0x0000000000000000000000000000000000000000"
            
            if fromAddress==zeroAddress or toAdddress==zeroAddress:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            try:
                nftMap_value=nftMap[transactionHash]
            except:
                nftMap_value={}
                
            tokenKey=tokenAddress+"_"+tokenId
            if tokenKey in nftMap_value:
                tokenValue=nftMap_value[tokenKey]
            else:
                tokenValue=[]
            
            tokenValue.append(fromAddress)
            tokenValue.append(toAdddress)
            
            nftMap_value[tokenKey]=tokenValue
            nftMap[transactionHash]=nftMap_value
                    
                
            oneLine = theCSV.readline().decode("utf-8").strip();
            
        # clear
        for key in nftMap.keys():
            value=nftMap[key]
            count=0
            for tokenKey in value.keys():
                tokenValue=deleteDuplicateInList(value[tokenKey])
                value[tokenKey]=tokenValue
                
                # nft transfered twice
                if len(tokenValue)<3:
                    # delete NFT
                    count+=1
                    
            if count==len(value):
                continue
            
            nftMap_new[key]=value
                
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(nftMap_new,tf)
            
            
def deleteDuplicateInList(tempList):
    newList=[]
    for item in tempList: 
        if item not in newList:
            newList.append(item)
    return newList


def getNftTransferOverOne():
    print("start")
    # flashbotsTxMap={}
    
    # with open("/mnt/sde1/geth/nft_analyse/data/flashbots/flashbotsTxMap.data", "rb") as tf:
    #     flashbotsTxMap=pickle.load(tf)
        
    outputPath_dict="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/NftTransferOverOne.map"
    
    print("erc721")
    
    getFromERC721(outputPath_dict)
    
    
if __name__ == '__main__':
    getNftTransferOverOne()