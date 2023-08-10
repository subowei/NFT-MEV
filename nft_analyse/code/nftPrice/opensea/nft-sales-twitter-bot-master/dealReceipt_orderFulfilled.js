const { ethers } = require('ethers');
const { getSeaportSalePrice } = require('./utils.js');
const marketsAbi = require('./markets_abi.json');
const StreamZip = require('node-stream-zip');
var AdmZip = require('adm-zip');
const fs = require('fs');


const dirPath='/mnt/sda1/bowei/sbw/xblock/receipt/'
const outputCsv = "/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/csv/nftTradefromsSeaport_13_15.csv"

// const dirPath='/mnt/sda1/bowei/sbw/xblock/receipt_15_16/'
// const outputCsv = "/mnt/sde1/geth/nft_analyse_v1/data/nftPrice/csv/nftTradefromsSeaport_15_16.csv"

var fsWrite = fs.openSync(outputCsv, 'w')

var files = fs.readdirSync(dirPath);

rows=[["blockNumber","transactionHash","offerer","recipient","offerSideNfts","nftAddress","nftId","nftPrice"]]
exportCSV(rows)

for(var i=0;i<files.length;i++){
    filePath=dirPath+files[i]
    fileNum=parseInt(files[i].split(".")[0])

    // the time of deploying the first seaport contract
    if (fileNum<14801551){
      continue
    }
    rows=[]
    console.log("filePath",filePath)
    dealZip(rows,filePath)
    console.log(rows.length)
    if (rows.length>1){
      exportCSV(rows)
    }
}

fs.close(fsWrite,function(err){
  if(err){
   throw err;
  }
  console.log('file closed');
})


function dealZip(rows,filePath){
  const interface = new ethers.utils.Interface(marketsAbi);
  const orderFulfilled_select="0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
  var zip = new AdmZip(filePath);
  var zipEntries = zip.getEntries()
  
  for (const key0 in zipEntries) {
    zipEntry=zipEntries[key0]
    entryName = zipEntry.entryName
    receipts = zip.readAsText(entryName);
    if (receipts.length==0){
      continue
    }

    receipts = JSON.parse(receipts)
    receipts=receipts["result"]
    
    for (const key1 in receipts) {
      receipt = receipts[key1]
      logs=receipt["logs"]

      for (const key2 in logs){
        log = logs[key2]
        topics=log["topics"]
        data=log["data"]
        transactionHash=log["transactionHash"]
        blockNumber=parseInt(log["blockNumber"],16)

        if (topics.length==3 && topics[0]===orderFulfilled_select){
          const decodedLogData = interface.parseLog({
            data: data,
            topics: topics
          })

          nftObjectList = getSeaportSalePrice(decodedLogData.args);
          for (const key3 in nftObjectList){
            nftObject=nftObjectList[key3]
            offerSideNfts = nftObject["offerSideNfts"]
            offerer = nftObject["offerer"]
            recipient = nftObject["recipient"]
            nftAddress = nftObject["nftAddress"]
            nftId = nftObject["nftId"]
            nftPrice = nftObject["nftPrice"]

            rows.push([blockNumber,transactionHash,offerer,recipient,offerSideNfts,nftAddress,nftId,nftPrice])
          }
        }
      }
    }
  }
}


function exportCSV(jsonData){
  let csvText="";
  for(let i = 0 ; i < jsonData.length ; i++ ){
      let row="";
      for(let item in jsonData[i]){
          row+=`${jsonData[i][item]},`;
      }
      csvText+=trim(row,",")+'\n';
  }
  // writeStream.write(csvText);
  fs.writeSync(fsWrite,csvText)
}


function trim(str, char) {
  if (char) {
      str=str.replace(new RegExp('^\\'+char+'+|\\'+char+'+$', 'g'), '');
  }
  return str.replace(/^\s+|\s+$/g, '');
};


