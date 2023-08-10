import sys
import csv

sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal2 import *
    
    
def addEthPrice():
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/liquidate/csv/firstBid.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/liquidate/csv/firstBid_withPrice.csv"
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
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/liquidate/csv/firstBid_withPrice.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/liquidate/csv/firstBid_withRevenue.csv"
    
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
            coinbase_transfer=float(row[5])/1e18
            penalty=float(row[8])/1e18
            ethPrice=float(row[9])
            
            revenue_eth=penalty-coinbase_transfer-transactionFee
            revenue_dollar=revenue_eth*ethPrice
    
            row.append(revenue_eth)
            row.append(revenue_dollar)
            writer.writerow(row)


def main():
    addEthPrice()
    addRevenue()

    
if __name__ == '__main__':
    main()