
import numpy as np
import csv    #加载csv包便于读取csv文件
import zipfile
import os
import json
import pickle
import os

# deal prefix "0"
def deal_topics_item(tempStr):
    tempStr="0x"+tempStr[26:]
    return tempStr
    
# convert 10 from 16
def deal_data_item(tempStr):
    # delete prefix "0"
    if "0x" in tempStr:
        tempStr=tempStr.lstrip("0x")
        
    tempStr=tempStr.lstrip("0")
        
    if tempStr=="":
        tempStr="0"
        return tempStr
    convertion = int(tempStr, 16)
    return str(convertion)

# only deal OrdersMatched event in opensea
def dealReceiptFile(result,outputList):
    for receipt in result:
        logs=receipt["logs"]
        for log in logs:
            topics=log["topics"]
            data=log["data"]
            transactionHash=log["transactionHash"]
            blockNumber=int(log["blockNumber"],16)
            address=log["address"]
            transactionIndex=int(log["transactionIndex"],16)

            if len(topics)==3 and topics[0]==event_select and address==CryptoPunksMarket:
                punkIndex=deal_data_item(topics[1])
                fromAddress=deal_topics_item(topics[2])
                value=deal_data_item(data)
                
                tempList=[blockNumber,transactionHash,transactionIndex,punkIndex,fromAddress,value]
                outputList.append(tempList)


def dealZipFile(fileDir,outputCsv):
    outputList=[]
    
    # 1. get all receipt file name
    fileNameList=[]
    for fileName in os.listdir(fileDir):
        if ".zip" in fileName:
            fileNameList.append(fileName.split(".zip")[0])
    
    # 2. unzip and deal
    for file in fileNameList:
        print(file)
        theZIP = zipfile.ZipFile(fileDir+file+".zip", 'r')
        for receiptFileName in theZIP.namelist():
            if "txt" not in receiptFileName:
                continue
            theTxt = theZIP.open(receiptFileName,"r")
            result=json.loads(theTxt.read().decode('UTF-8'))["result"]
            if len(result)!=0:
                dealReceiptFile(result,outputList)
                
        ouputCsv(outputList,outputCsv)
        
    print("finish")

def ouputCsv(outputList,outputCsv):
    f = open(outputCsv,'w')
    writer = csv.writer(f)
    writer.writerow(["blockNumber","transactionHash","transactionIndex","punkIndex","fromAddress","price"])
    for row in outputList:
            writer.writerow(row)

def main():
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBidEntered_13_15.csv"
    dealZipFile(receipt_path,outputCsv)

CryptoPunksMarket="0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
receipt_path="/mnt/sda1/xblock/receipt/"
event_select="0x5b859394fabae0c1ba88baffe67e751ab5248d2e879028b8c8d6897b0519f56a"

# def main():
#     outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBidEntered_15_16.csv"
#     dealZipFile(receipt_path,outputCsv)


# receipt_path="/mnt/sda1/xblock/receipt_15_16/"
# event_select="0x5b859394fabae0c1ba88baffe67e751ab5248d2e879028b8c8d6897b0519f56a"


if __name__ == '__main__':
    main()