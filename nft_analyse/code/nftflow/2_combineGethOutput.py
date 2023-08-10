import pandas as pd




def main():
    df0=pd.read_csv("/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/gethOutput_13_14.csv")
    df1=pd.read_csv("/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/gethOutput_14_15.csv")
    
    df = pd.concat([df0, df1])
    
    # print("origin len",len(df))
    # df.drop_duplicates(['transactionHash_0','transactionHash_1'],keep='first')
    # print("after len",len(df))
    
    df.to_csv("/mnt/sde1/geth/nft_analyse_v1/data/nftflow/csv/nftFlow.csv",index=0)



if __name__ == '__main__':
    main()