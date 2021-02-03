// This should be deployed after LinkToken
const version = process.env.VERSION || 'v0.4'

const { Aggregator } = require(`../truffle/${version}/Aggregator`)
Aggregator.setProvider(web3.currentProvider)

// contract : https://github.com/smartcontractkit/chainlink/blob/develop/evm-contracts/src/v0.4/Aggregator.sol
module.exports = function (deployer, _, accounts) {
  deployer.deploy(Aggregator,
    "0x11c6d510B5009a45EA9832828DE00f8cCe23c19E", // Contract Address of LinkToken
    1, // _paymentAmount
    0, //_minimumResponses (use number of responses alternative to timeout )
    ["0xE4ffd8d653c54780dbD1708a268488130ebABfdA"], // oracle addresses
    ["0x7465737400000000000000000000000000000000000000000000000000000000"],// jobids
    { from: accounts[0], overwrite: false })
}
