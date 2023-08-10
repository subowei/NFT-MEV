import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os


def getFromInternal(txMap,outputPath_dict,addressMap):
    
    fileDir = "/mnt/sda1/bowei/sbw/xblock/internalTransaction/";

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
            
            if fromAddr not in addressMap and toAddr not in addressMap:
                oneLine = theCSV.readline().decode("utf-8").strip();
                continue
            
            if fromAddr in addressMap:
                marketType=addressMap[fromAddr]
            else:
                marketType=addressMap[toAddr]
            
            if transactionHash not in txMap:
                txMap_value=[]
            else:
                txMap_value=txMap[transactionHash]
            
            if marketType not in txMap_value:
                txMap_value.append(marketType)
                
            txMap[transactionHash]=txMap_value
            
            oneLine = theCSV.readline().decode("utf-8").strip();
        
        outputMap(txMap,outputPath_dict)
            
            
def outputMap(txMap,outputPath_dict): 
    resMap={}
    
    for key,value in txMap.items():
        marketTypeString=";".join(value)
        resMap[key]=marketTypeString

    with open(outputPath_dict, "wb") as tf:
        pickle.dump(resMap,tf)



def getAllTxs():
    txMap={}
    outputPath_dict="/mnt/sde1/geth/nft_analyse_v1/data/nftMarket/map/txToAllMarket.map"
    
    addressMap={
        # rarible Exchange V1
        "0x09eab21c40743b2364b94345419138ef80f39e30":"Rarible_v1",
        # rarible Exchange V2
        "0x9757f2d2b135150bbeb65308d4a91804107cd8d6":"Rarible_v2",
        # CryptoPunksMarket
        "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb":"CryptoPunks",
        # CryptoKitties: Sales Auction
        "0xb1690c08e213a35ed9bab7b318de14420fb57d8c":"CryptoKitties",
        # OpenSea Wyvern Exchange V1
        "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b":"OpenSea_wyvernV1",
        # OpenSea Wyvern Exchange V2
        "0x7f268357a8c2552623316e2562d90e642bb538e5":"OpenSea_wyvernV2",
        # OpenSea Seaport 1.0
        "0x00000000006cee72100d161c57ada5bb2be1ca79":"OpenSea_seaportV0",
        # OpenSea Seaport 1.1
        "0x00000000006c3852cbef3e08e8df289169ede581":"OpenSea_seaportV1",
        # OpenSea Seaport 1.2
        "0x00000000000006c7676171937c444f6bde3d6282":"OpenSea_seaportV2",
        # OpenSea Seaport 1.3
        "0x0000000000000ad24e80fd803c6ac37206a45f15":"OpenSea_seaportV3",
        # OpenSea Seaport 1.4
        "0x00000000000001ad428e4906ae43d8f9852d0dd6":"OpenSea_seaportV4",
        # OpenSea Seaport 1.5
        "0x00000000000000adc04c56bf30ac9d3c0aaf14dc":"OpenSea_seaportV5",
        # LooksRare: Exchange
        "0x59728544b08ab483533076417fbbb2fd0b17ce3a":"LooksRare",
        # X2Y2: Exchange
        "0x74312363e45dcaba76c59ec49a7aa8a65a67eed3":"X2Y2",
        # Sudoswap: Pair Router
        "0x2b2e8cda09bba9660dca5cb6233787738ad68329":"Sudoswap",
        # Sudoswap: Pair Factory
        "0xb16c1342e617a5b6e4b631eb114483fdb289c0a4":"Sudoswap",
        # SuperRare: Treasury
        "0x860a80d33e85e97888f1f0c75c6e5bbd60b48da9":"SuperRare",
        # Blur.io: Marketplace
        "0x000000000000ad05ccc4f10045630fb830b95127":"Blur",
        # Arcade.xyz: Origination Controller Proxy
        "0x4c52ca29388a8a854095fd2beb83191d68dc840b":"Arcade",
        # Arcade: Origination Controller
        "0x0585a675029c68a6af41ba1350bc8172d6172320":"Arcade",
        # VeryNifty: NFT20 Factory
        "0x0f4676178b5c53ae0a655f1b19a96387e4b8b5f2":"NFT20",
        # NFTX: Simple Fee Distributo
        "0xfd8a76dc204e461db5da4f38687adc9cc5ae4a86":"NFTX",
        # NFTX: DAO Treasury
        "0x40d73df4f99bae688ce3c23a01022224fe16c7b2":"NFTX",
        # WrappedG0
        "0xa10740ff9ff6852eac84cdcff9184e1d6d27c057":"WrappedG0",
        # Gem: GemSwap 2
        "0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2":"GemSwap"
    }
    
    getFromInternal(txMap,outputPath_dict,addressMap)

if __name__ == '__main__':
    getAllTxs()