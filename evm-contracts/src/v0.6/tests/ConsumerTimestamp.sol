// SPDX-License-Identifier: MIT
pragma solidity ^0.6.0;

import "../ChainlinkClient.sol";

contract ConsumerTimestamp is ChainlinkClient {
  bytes32 internal specId;        // Job ID created from Chainlink node
  address internal oracleAddress; // Oracle Address that ChainLink listens to
  uint256[] public times;

  event RequestFulfilled(
    bytes32 indexed requestId,  // User-defined ID
    uint256 indexed time
  );

  constructor (address _oracleAddress) public {
    // specId = _specId;
    specId = "0c63f2461b1e45b08d3b4957607e9b6d";
    oracleAddress = _oracleAddress;
  }

  // requestTime requests time from off-chain
  function requestTime() public {
    Chainlink.Request memory req = buildChainlinkRequest(specId, address(this), this.setTime.selector);
    sendChainlinkRequestTo(oracleAddress, req, 0);
  }

  // setTime is called from off-chain
  function setTime(bytes32 _requestId, uint256 t) public recordChainlinkFulfillment(_requestId) {
    emit RequestFulfilled(_requestId, t);
    times.push(t);
  }

  // getTime can check what is stored in time
  function getTime() public view returns (uint256[] memory) {
      return times;
  }
}
