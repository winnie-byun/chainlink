// This should be deployed after LinkToken
const version = process.env.VERSION || 'v0.6'

const { Oracle } = require(`../truffle/${version}/Oracle`)
Oracle.setProvider(web3.currentProvider)

module.exports = function (deployer, _, accounts) {
  deployer.deploy(Oracle,
    "0x11c6d510B5009a45EA9832828DE00f8cCe23c19E", // Contract Address of LinkToken
    { from: accounts[0], overwrite: false })
}
