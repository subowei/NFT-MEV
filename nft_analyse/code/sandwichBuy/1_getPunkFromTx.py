import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle

def getPunkFromErc20():
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
    
    txMap={}

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
            
            punkTokenAddr="0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

            if tokenAddr!=punkTokenAddr:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            txMap[transactionHash]=oneArray
            
        
            oneLine = theCSV.readline().decode("utf-8").strip();
            
        with open("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/map/txWithPunkTransfer.map", "wb") as tf:
            pickle.dump(txMap,tf)

def main():        
    getPunkFromErc20()

if __name__ == '__main__':
    main()