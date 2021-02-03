const version = process.env.VERSION || 'v0.6'

const { Consumer } = require(`../truffle/${version}/Consumer`)
Consumer.setProvider(web3.currentProvider)

module.exports = function (deployer, _, accounts) {
  deployer.deploy(Consumer, { from: accounts[0], overwrite: false })
}