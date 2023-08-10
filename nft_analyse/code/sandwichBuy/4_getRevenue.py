import sys
import csv
import pandas as pd


def getRevenue():
    fNew = open("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/sandwich_withRevenue.csv",'w')
    writer = csv.writer(fNew)
    header_row=["blockNumber","timestamp","transactionHash","bidPrice","coinbase_transfer","nftPrice","ethPrice","revenue_eth","revenue_dollar"]
    writer.writerow(header_row)
    
    df=pd.read_csv("/mnt/sde1/geth/nft_analyse_v1/data/sandwichBuy/csv/sandwich_withPrice.csv")        
    for index in range(0,len(df),3):
        print("index",index)
        blockNumber=df.iloc[index+2]["blockNumber"]
        timestamp=df.iloc[index+2]["timestamp"]
        transactionHash=df.iloc[index+2]["transactionHash"]
        nftPrice=df.iloc[index+1]["nftPrice"]
        ethPrice=df.iloc[index+2]["ethPrice"]
        if nftPrice==0:
            continue
        
        bidPrice=float(df.iloc[index]["value"])/1e18
        
        txFee0=float(df.iloc[index]["gasPrice"])*float(df.iloc[index]["gasUsed"])/1e18
        txFee2=float(df.iloc[index+2]["gasPrice"])*float(df.iloc[index+2]["gasUsed"])/1e18
        
        coinbase_transfer0=float(df.iloc[index]["coinbase_transfer"])/1e18
        coinbase_transfer2=float(df.iloc[index+2]["coinbase_transfer"])/1e18
        coinbase_transfer_ori_2=float(df.iloc[index+2]["coinbase_transfer"])
        
        revenue_eth=nftPrice-bidPrice-txFee0-txFee2-coinbase_transfer0-coinbase_transfer2
        revenue_dollar=revenue_eth*ethPrice
        
        # 异常值
        if blockNumber==14743812:
            continue
        
        writer.writerow([blockNumber,timestamp,transactionHash,bidPrice,coinbase_transfer_ori_2,nftPrice,ethPrice,revenue_eth,revenue_dollar])
        
        


def main():
    getRevenue()

    
if __name__ == '__main__':
    main()