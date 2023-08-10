import sys
import csv
import pickle

sys.path.append("/mnt/sde1/geth/nft_analyse_v1/code/nftPrice")
from readNftPrice import *

sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal2 import *

def readPunkIndex(inputCsv,txMap):
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            transactionHash=row[1]
            punkIndex=row[3]
            txMap[transactionHash]=punkIndex


def addNFTIndex():
    txMap={}
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/punkBought_repaired.csv"
    readPunkIndex(inputCsv,txMap)
    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/sandwich_punk.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/sandwich_withPunkIndex.csv"
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        header_row.append("punkIndex")
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            transactionHash=row[2]
            if transactionHash in txMap:
                punkIndex=txMap[transactionHash]
            else:
                punkIndex="none"
                
            row.append(punkIndex)
            writer.writerow(row)
            
            
def addNFTPrice():    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/sandwich_withPunkIndex.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/sandwich_withPunkPrice.csv"
    
    TOKEN_PRICE_CENTER_NFT = TokenPriceCenter_NFT()
    print("start")
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        header_row.append("nftPrice")
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1
            blockNum=int(row[0])
            punkIndex=row[21]
            if punkIndex=="none":
                nftPrice=0
            else:
                punkAddress="0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
                nftPrice=TOKEN_PRICE_CENTER_NFT.findFeaturePrice(punkAddress,punkIndex, blockNum)
            row.append(nftPrice)
            writer.writerow(row)


def addEthPrice():
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/sandwich_withPunkPrice.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/sandwich_withPrice.csv"
    wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    # ethPrice=TOKEN_PRICE_CENTER.swap(wethAddress, timestamp,1)
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        header_row.append("ethPrice")
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            timestamp=int(row[1])
            ethPrice=TOKEN_PRICE_CENTER.swap(wethAddress, timestamp,1e18)
            row.append(ethPrice)
            writer.writerow(row)


def main():
    # addNFTIndex()
    addNFTPrice()
    addEthPrice()

    
if __name__ == '__main__':
    main()