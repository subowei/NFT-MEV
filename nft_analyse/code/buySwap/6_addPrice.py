import sys
import csv
import pickle

sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal2 import *
    
    
def addEthPrice():
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap_withPrice.csv"
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

            timestamp=int(row[2])
            ethPrice=TOKEN_PRICE_CENTER.swap(wethAddress, timestamp,1e18)
            row.append(ethPrice)
            writer.writerow(row)


def addRevenue():
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap_withPrice.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap_withRevenue.csv"
    wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        header_row.append("revenue_eth")
        header_row.append("revenue_dollar")
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            transactionFee=float(row[4])/1e18
            # coinbase_transfer=float(row[5])/1e18
            inEth=float(row[10])/1e18
            outEth=float(row[11])/1e18
            ethPrice=float(row[12])
            revenue_eth=inEth-outEth-transactionFee
            revenue_dollar=revenue_eth*ethPrice
    
            row.append(revenue_eth)
            row.append(revenue_dollar)
            writer.writerow(row)
            
            
def addMarket():
    with open("/mnt/sde1/geth/nft_analyse_v1/data/nftMarket/map/txToAllMarket.map", "rb") as tf:
        txMap=pickle.load(tf)
    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap_withRevenue.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap_withMarket.csv"
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        header_row.append("market")
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            transactionHash=row[0]
            if transactionHash in txMap:
                market=txMap[transactionHash]
            else:
                market="none"
            
            row.append(market)
            writer.writerow(row)
            
            
def simplyMarketString(oldStr):
    oldList=oldStr.split(";")
    temp=oldList[0]
    return temp.split("_")[0]
            
            
            
def dealMarket():    
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap_withMarket.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap_final.csv"
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            row[-1]=simplyMarketString(row[-1])
            
            writer.writerow(row)


def main():
    addEthPrice()
    addRevenue()
    addMarket()
    
    dealMarket()
    
if __name__ == '__main__':
    main()