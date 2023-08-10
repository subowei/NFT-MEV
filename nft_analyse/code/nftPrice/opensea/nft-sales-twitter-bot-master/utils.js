// external
const { ethers } = require('ethers');
// local
const { currencies } = require('./currencies.js');

const { currencies_onlyEth } = require('./currencies.js');

function _reducer(previous, current) {
  // const currency = currencies[current.token.toLowerCase()];
  const currency = currencies_onlyEth[current.token.toLowerCase()];

  if (currency !== undefined ) {
    const result = previous + Number(ethers.utils.formatUnits(current.amount, currency.decimals));

    return result;
  } else {
    return previous;
  }
}

function getSeaportSalePrice(decodedLogData) {
  const offer = decodedLogData.offer;
  const consideration = decodedLogData.consideration;

  // console.log(decodedLogData)

  // console.log(offer)
  // console.log(consideration)

  const offerSideNfts = offer.some(
    (item) => item.itemType === 2
  );

  var nftSet = new Set();
  // var nftAddressList = []
  // var nftIdList = []
  var totalPrice

  // get nft data
  for (const key in offer) {
    item = offer[key]

    if (item.itemType == 2){
      nftId = parseInt(item[2]._hex ,16).toString()
      nftAddress = item.token.toLowerCase()

      // nftAddressList.push(nftAddress)
      // nftIdList.push(nftId)
      nftSet.add(nftAddress+"_"+nftId)
    }
  }
  for (const key in consideration) {
    item = consideration[key]

    if (item.itemType == 2){
      nftId = parseInt(item[2]._hex ,16)
      nftAddress = item.token.toLowerCase()

      // nftAddressList.push(nftAddress)
      // nftIdList.push(nftId)
      nftSet.add(nftAddress+"_"+nftId)
    }
  }

  // if nfts are on the offer side, then consideration is the total price, otherwise the offer is the total price
  if (offerSideNfts) {
    totalPrice = consideration.reduce(_reducer, 0);
  } else {
    totalPrice = offer.reduce(_reducer, 0);
  }

  resList=[]
  
  for (const item of nftSet) {
    nftAddress=item.split("_")[0]
    nftId=item.split("_")[1]

    resList.push({
      "offerSideNfts":offerSideNfts,
      "offerer":decodedLogData.offerer,
      "recipient":decodedLogData.recipient,
      "nftPrice": totalPrice/nftSet.size,
      "nftAddress": nftAddress,
      "nftId": nftId
    })
  }
  return resList
}

module.exports = {
  getSeaportSalePrice: getSeaportSalePrice,
};
