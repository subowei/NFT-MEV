import sys
import csv
import pickle

    
    
def addReward():
    inputCsv="/mnt/sde1/geth/nft_analyse_v1/data/mevImpact/csv/combineAll.csv"
    outputCsv="/mnt/sde1/geth/nft_analyse_v1/data/mevImpact/csv/combineAll_final.csv"
    
    with open("/mnt/sde1/geth/nft_analyse_v1/data/block/map/block.map", "rb") as tf:
        blockMap=pickle.load(tf)
    
    fNew = open(outputCsv,'w')
    writer = csv.writer(fNew)
    with open(inputCsv,'r', encoding="UTF8") as fOld:
        reader = csv.reader(fOld)
        header_row=next(reader)
        header_row.append("minerReward_usd")
        writer.writerow(header_row)
        i=0
        for row in reader:
            if i%100==0:
                print(i)
            i+=1

            blockNumber=int(row[0])
            totalReward_usd=blockMap[blockNumber]["totalReward_usd"]
            row.append(totalReward_usd)
            writer.writerow(row)        

def main():
    addReward()

    
if __name__ == '__main__':
    main()