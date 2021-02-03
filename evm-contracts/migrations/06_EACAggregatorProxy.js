// This should be deployed after aggregator and accessController
const version = process.env.VERSION || 'v0.6'

const { EACAggregatorProxy } = require(`../truffle/${version}/EACAggregatorProxy`)
EACAggregatorProxy.setProvider(web3.currentProvider)

module.exports = function (deployer, _, accounts) {
  deployer.deploy(EACAggregatorProxy,
    "0x5b6AFf7Bc77d24Ff2bd58ae521FE1EDb91d8908B", // 03 aggregator address
    "0xf9D212c349b28E42BfC416d58C0BDDB71Da1D3B7", // 04 accessController address
    { from: accounts[0], overwrite: false })
}