// This should be deployed after LinkToken
const version = process.env.VERSION || 'v0.6'

const { FluxAggregator } = require(`../truffle/${version}/FluxAggregator`)
FluxAggregator.setProvider(web3.currentProvider)

// contract : https://github.com/smartcontractkit/chainlink/blob/develop/evm-contracts/src/v0.6/FluxAggregator.sol
module.exports = function (deployer, _, accounts) {
  deployer.deploy(FluxAggregator,
    "0x11c6d510B5009a45EA9832828DE00f8cCe23c19E", // Contract Address of LinkToken
    1, // _paymentAmount
    1, // _timeout (sec)
    "0x9999999999999999999999999999999999999999", // validator address (optional)
    1, // _minSubmissionValue
    100, //_maxSubmissionValue
    10, // _decimals
    "winnie test deploying", // _description
    { from: accounts[0], overwrite: false })
}