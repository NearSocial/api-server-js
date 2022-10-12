const nearAPI = require("near-api-js");
const fs = require("fs");

const config = {
  networkId: "mainnet",
  nodeUrl: "https://rpc.mainnet.internal.near.org",
  accountId: "social.near",
  blockHeight: 75942518,
};

const SnapshotFilename = "res/snapshot.json";

function saveJson(json, filename) {
  try {
    const data = JSON.stringify(json);
    fs.writeFileSync(filename, data);
  } catch (e) {
    console.error("Failed to save JSON:", filename, e);
  }
}

(async () => {
  const near = await nearAPI.connect(config);
  const viewCall = async (methodName, args) => {
    args = args || {};
    const result = await near.connection.provider.query({
      request_type: "call_function",
      account_id: config.accountId,
      method_name: methodName,
      args_base64: Buffer.from(JSON.stringify(args)).toString("base64"),
      block_id: config.blockHeight,
    });

    return (
      result.result &&
      result.result.length > 0 &&
      JSON.parse(Buffer.from(result.result).toString())
    );
  };

  const numNodes = await viewCall("get_node_count");
  console.log("Fetching nodes: " + numNodes);

  const limit = 50;
  const nodesPromises = [
    viewCall("get_nodes", {
      from_index: 0,
      limit: 1,
    }),
  ];
  for (let i = 1; i < numNodes; i += limit) {
    nodesPromises.push(
      viewCall("get_nodes", {
        from_index: i,
        limit,
      })
    );
  }
  const nodes = (await Promise.all(nodesPromises)).flat();

  console.log("Processing nodes: " + nodes.length);
  const data = {};
  const nodeLinks = { 0: { o: data, b: 0 } };

  // console.log(JSON.stringify(nodes, undefined, 2));
  nodes.forEach((node) => {
    const obj = nodeLinks[node.node_id];
    obj.b = node.block_height;
    const o = obj.o;
    node.children.forEach(([key, nodeValue]) => {
      if ("Node" in nodeValue) {
        const link = { o: {}, b: 0 };
        const nodeId = nodeValue.Node;
        o[key] = [link];
        nodeLinks[nodeId] = link;
      } else {
        const value = nodeValue.Value;
        o[key] = [{ s: value.value, b: value.block_height }];
      }
    });
  });

  saveJson(
    {
      data,
      lastReceipt: {
        blockHeight: config.blockHeight,
        outcomeIndex: 1e9,
      },
    },
    `res/snapshot.json`
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
