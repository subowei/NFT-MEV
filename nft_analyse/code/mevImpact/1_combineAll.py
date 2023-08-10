import sys
import csv
import pandas as pd



def getBehavior(writer, filePath, behaviorType):
    df=pd.read_csv(filePath)
    if behaviorType=="buysell":
        df=df[df["marketEdit"]!="none"]
        df=df[df["marketEdit"]!="Arcade"]
        df=df[df["marketEdit"]!="GemSwap"]
        
    elif behaviorType=="buyswap":
        df=df[df["market"]!="none"]
        
    elif behaviorType=="sandwichbuy":
        df=df[df["blockNumber"]!=14743812]
        
    elif behaviorType=="liquidate":
        df=df[df["blockNumber"]<=16000000]
        df=df[df["revenue_dollar"]>0]
        
    elif behaviorType=="firstbid":
        df=df[df["blockNumber"]<=16000000]
        df=df[df["revenue_dollar"]>0]
        
    
    for index,row in df.iterrows():
        transactionHash=row["transactionHash"]
        blockNumber=row["blockNumber"]
        timestamp=row["timestamp"]
        ethPrice=row["ethPrice"]
        coinbase_transfer=row["coinbase_transfer"]
        revenue_dollar=row["revenue_dollar"]
        
        coinbase_transfer_usd=float(ethPrice)*float(coinbase_transfer)/1e18
        
        
        if behaviorType=="buysell":
            pureRevenue=(float(row["inEth"])-float(row["outEth"]))/1e18
            pureRevenue+= (float(coinbase_transfer)/1e18)
            
        elif behaviorType=="buyswap":
            pureRevenue=(float(row["inEth"])-float(row["outEth"]))/1e18
            pureRevenue+= (float(coinbase_transfer)/1e18)
            
        elif behaviorType=="givebirth":
            pureRevenue=float(row["callReward"])/1e18
            
        elif behaviorType=="sandwichbuy":
            pureRevenue=float(row["nftPrice"])-float(row["bidPrice"])
            
        elif behaviorType=="liquidate":
            pureRevenue=float(row["nftPrice"])-float(row["bidPrice"])/1e18
            
        elif behaviorType=="firstbid":
            pureRevenue=float(row["penalty"])/1e18
        
        pureRevenue_usd=float(ethPrice)*float(pureRevenue)
        
        writer.writerow([blockNumber,timestamp,transactionHash,pureRevenue_usd,coinbase_transfer_usd,revenue_dollar,behaviorType])


def main():
    fNew = open("/mnt/sde1/geth/nft_analyse_v1/data/mevImpact/csv/combineAll.csv",'w')
    writer = csv.writer(fNew)
    writer.writerow(["blockNumber","timestamp","transactionHash","pureRevenue_usd","coinbase_transfer_usd","revenue_dollar","behaviorType"])
    
    getBehavior(writer, "/mnt/sde1/geth/nft_analyse_v1/data/buySwap/csv/buyswap_final.csv", "buyswap")
    getBehavior(writer, "/mnt/sde1/geth/nft_analyse_v1/data/buySell/csv/buysell_final.csv", "buysell")
    getBehavior(writer, "/mnt/sde1/geth/nft_analyse_v1/data/giveBirth/csv/givebirth_withRevenue.csv", "givebirth") 
    getBehavior(writer, "/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/sandwich_withRevenue.csv", "sandwichbuy") 
    getBehavior(writer, "/mnt/sde1/geth/nft_analyse_v1/data/liquidate/csv/liquidate_withRevenue.csv", "liquidate") 
    getBehavior(writer, "/mnt/sde1/geth/nft_analyse_v1/data/liquidate/csv/firstBid_withRevenue.csv", "firstbid") 



if __name__ == '__main__':
    main()
    



