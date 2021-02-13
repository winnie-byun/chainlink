const version = process.env.VERSION || 'v0.6'

const { ConsumerTimestamp } = require(`../truffle/${version}/ConsumerTimestamp`)
ConsumerTimestamp.setProvider(web3.currentProvider)

module.exports = function (deployer, _, accounts) {
  deployer.deploy(ConsumerTimestamp,
    "0xE4ffd8d653c54780dbD1708a268488130ebABfdA", // Contract Address of Oracle
    "0x11c6d510B5009a45EA9832828DE00f8cCe23c19E", // Contract Address of LinkToken
    { from: accounts[0], overwrite: false })
}
