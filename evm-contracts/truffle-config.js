const HDWalletProvider = require('@truffle/hdwallet-provider')
// const HDWalletProvider = require('truffle-hdwallet-provider-klaytn')

const privateKey = process.env.PRIVATE_KEY

module.exports = {
  //$ truffle test --network <network-name>
  networks: {
    baobab: {
      provider: () => new HDWalletProvider(privateKey, "https://api.baobab.klaytn.net:8651"),
      network_id: '1001', //Klaytn baobab testnet's network id
      gas: '8500000',
      gasPrice: null
    },
    cypress: {
      provider: () => new HDWalletProvider(privateKey, "https://api.cypress.klaytn.net:8651"),
      network_id: '8217', //Klaytn mainnet's network id
      gas: '8500000',
      gasPrice: null
    }
  },

  compilers: {
    solc: {
      version: "0.6.6",    // Fetch exact version from solc-bin (default: truffle's version)
      docker: true,        // Use "0.5.1" you've installed locally with docker (default: false)
      settings: {          // See the solidity docs for advice about optimization and evmVersion
        optimizer: {
          enabled: true,
          runs: 200
        },
        evmVersion: "constantinople"
      }
    }
  },

  // Set default mocha options here, use special reporters etc.
  mocha: {
    reporter: 'eth-gas-reporter',
    reporterOptions: {
      currency: 'USD',
      gasPrice: 21,
      showTimeSpent: true,
    },
  },

  // Configure contracts location to dir without contracts
  // to avoid Truffle compilation step (we use @chainlink/belt instead)
  contracts_directory: './contracts_null',

  verify: {
    preamble: 'LINK\nVersion: 0.1.0',
  },
}
