import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os

def getPairAddress():
    pairAddressMap={}
    pairAddress_csv="/mnt/sde1/geth/nft_analyse/data/swapDex/pairWithWeth.csv"
    with open(pairAddress_csv,'r', encoding="UTF8") as f:
        reader = csv.reader(f)
        header_row=next(reader)
        i=0
        for row in reader:
            if i%10000==0:
                print(i)
            i+=1
            pairAddress=row[0]
            pairAddressMap[pairAddress]=1
            
    return pairAddressMap


# blockNumber,timestamp,transactionHash,tokenAddress,from,to,fromIsContract,toIsContract,value
def get_mortgage_reward_fromERC20(nftMap,pairAddressMap):
    txMap={}
    fileDir = "/mnt/sda1/bowei/sbw/xblock/erc20/";

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
            tokenAddress=oneArray[3]
            fromAddress=oneArray[4]
            toAdddress=oneArray[5]
            value=oneArray[8]
            wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            zeroAddress="0x0000000000000000000000000000000000000000"

            if transactionHash not in nftMap:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue

            # mortgage: mortgage NFT into market and get erc20 token
            if fromAddress==zeroAddress and tokenAddress!=wethAddress:
                mortgage="true"
                if transactionHash not in txMap.keys():
                    txMap_value={}
                else:
                    txMap_value=txMap[transactionHash]
                txMap_value["mortgage"]=mortgage
                txMap[transactionHash]=txMap_value

            
            # reward: swap erc20 token to weth
            if tokenAddress==wethAddress and fromAddress in pairAddressMap:
                reward=value
                if transactionHash not in txMap.keys():
                    txMap_value={}
                else:
                    txMap_value=txMap[transactionHash]
                txMap_value["reward"]=reward
                txMap[transactionHash]=txMap_value
                
            oneLine = theCSV.readline().decode("utf-8").strip();
        
        outputPath_dict="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/txWithSwap.map"
        with open(outputPath_dict, "wb") as tf:
            pickle.dump(txMap,tf)
        
        
def main():
    with open("/mnt/sde1/geth/nft_analyse_v1/data/buySwap/map/NftTransferOverOne.map", "rb") as tf:
        nftMap=pickle.load(tf)
    
    pairAddressMap=getPairAddress()
    get_mortgage_reward_fromERC20(nftMap,pairAddressMap)
        
        
if __name__ == '__main__':
    main()