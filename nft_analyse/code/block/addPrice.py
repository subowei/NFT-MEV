import sys
import csv
import pickle

sys.path.append("/mnt/sde1/peilin_defi/code/interfaceTool")
from readTotal2 import *
    
    
def addEthPrice():
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/block/csv/block.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/block/csv/block_final.csv"
    wethAddress="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    
    blockMap={}
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        header_row.append("ethPrice")
        header_row.append("totalReward_eth")
        header_row.append("totalReward_usd")
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            blockNumber=int(row[0])
            timestamp=int(row[1])
            reward=float(row[2])/1e18
            fees=float(row[3])/1e18
            
            ethPrice=TOKEN_PRICE_CENTER.swap(wethAddress, timestamp,1e18)
            
            totalReward_eth=reward+fees
            totalReward_usd=totalReward_eth*ethPrice
            
            row.append(ethPrice)
            row.append(totalReward_eth)
            row.append(totalReward_usd)
            writer.writerow(row)
            
            blockMap[blockNumber]={"timestamp":timestamp,"totalReward_usd":totalReward_usd}
            
        
    with open("/mnt/sde1/geth/nft_analyse_v1/data/block/map/block.map", "wb") as tf:
        pickle.dump(blockMap,tf)
        

def main():
    addEthPrice()

    
if __name__ == '__main__':
    main()