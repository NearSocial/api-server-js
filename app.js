const fs = require("fs");
const bounds = require("binary-search-bounds");

const cors = require("@koa/cors");

const Koa = require("koa");
const app = new Koa();
app.proxy = true;

const Router = require("koa-router");
const router = new Router();

const logger = require("koa-logger");

const bodyParser = require("koa-bodyparser");

const Receipts = require("./receipts");
const axios = require("axios");

const WebSocket = require("ws");
const { options } = require("pg/lib/defaults");

const StateFilename = "res/state.json";
const SnapshotFilename = "res/snapshot.json";
const SubsFilename = "res/subs.json";
const WsSubsFilename = "res/ws_subs.json";

const isObject = function (o) {
  return o === Object(o) && !Array.isArray(o) && typeof o !== "function";
};

const isString = (s) => typeof s === "string";

const KeyBlockHeight = ":block";
const KeyTimestamp = ":timestamp";

const KeysReturnType = {
  True: "True",
  BlockHeight: "BlockHeight",
  History: "History",
};

function saveJson(json, filename) {
  try {
    const data = JSON.stringify(json);
    fs.writeFileSync(filename, data);
  } catch (e) {
    console.error("Failed to save JSON:", filename, e);
  }
}

function loadJson(filename, ignore) {
  try {
    let rawData = fs.readFileSync(filename);
    return JSON.parse(rawData);
  } catch (e) {
    if (!ignore) {
      console.error("Failed to load JSON:", filename, e);
    }
  }
  return null;
}

const PostTimeout = 1000;

let blockTimestamps = {};

const recursiveSet = (obj, newObj, b) => {
  Object.entries(newObj).forEach(([key, newValue]) => {
    const values = obj?.[key] || [];
    const v = values.length > 0 ? values[values.length - 1] : null;
    const o = v?.o ?? (v?.i !== undefined ? values[v.i].o : null);
    // Is the last value a node/object
    if (o) {
      values.push({
        i: v?.i ?? values.length - 1,
        b,
      });
      if (isObject(newValue)) {
        recursiveSet(o, newValue, b);
      } else if (isString(newValue)) {
        o[""] = o[""] || [];
        o[""].push({
          s: newValue,
          b,
        });
      } else {
        throw new Error("newValue is not object not string");
      }
    } else {
      if (isObject(newValue)) {
        const value = {
          o: isString(v?.s) ? { "": [v] } : {},
          b,
        };
        recursiveSet(value.o, newValue, b);
        values.push(value);
        obj[key] = values;
      } else if (isString(newValue)) {
        values.push({
          s: newValue,
          b,
        });
        obj[key] = values;
      } else {
        throw new Error("newValue is not object not string");
      }
    }
  });
};

const mergeData = (obj, newObj) => {
  Object.entries(newObj).forEach(([key, newValue]) => {
    const value = obj?.[key];
    if (isObject(value)) {
      if (isObject(newValue)) {
        mergeData(value, newValue);
      } else if (isString(newValue)) {
        value[""] = newValue;
      } else {
        throw new Error("newValue is not object not string");
      }
    } else if (isString(value)) {
      if (isObject(newValue)) {
        obj[key] = Object.assign({ "": value }, newValue);
      } else if (isString(newValue)) {
        obj[key] = newValue;
      } else {
        throw new Error("newValue is not object not string");
      }
    } else {
      obj[key] = newValue;
    }
  });
};

const valueCmp = (a, b) => a.b - b.b;
const findValueAtBlockHeight = (values, b) =>
  b !== undefined
    ? values?.[bounds.le(values, { b }, valueCmp)]
    : values.length
    ? values[values.length - 1]
    : undefined;

const extractBlockHistory = (values, b) =>
  values
    .slice(
      0,
      b !== undefined ? bounds.le(values, { b }, valueCmp) + 1 : values.length
    )
    .map((v) => v.b);

const addOptions = (v, b, options) => {
  if (options.withBlockHeight) {
    v[KeyBlockHeight] = b;
  }
  if (options.withTimestamp) {
    v[KeyTimestamp] = blockTimestamps[b];
  }
};

const jsonSetKey = (res, key, newValue, options) => {
  const value = res[key];
  let v = newValue.s;
  if (options.withBlockHeight || options.withTimestamp) {
    v = {
      "": v,
    };
  }
  addOptions(v, newValue.b, options);
  if (isObject(value)) {
    value[""] = v;
  } else {
    res[key] = v;
  }
};

const jsonMapSetValue = (res, key, newValue) => {
  const value = res[key];
  if (isObject(value)) {
    value[""] = newValue;
  } else {
    res[key] = newValue;
  }
};

const jsonGetInnerObject = (res, key) => {
  const value = res[key];
  if (isObject(value)) {
    return value;
  } else if (isString(value)) {
    return (res[key] = {
      "": value,
    });
  } else {
    return (res[key] = {});
  }
};

const recursiveGet = (res, obj, objBlock, keys, b, options) => {
  const matchKey = keys[0];
  let isRecursiveMatch = matchKey === "**";
  if (isRecursiveMatch && keys.length > 1) {
    throw new Error("pattern '**' can only be used as a suffix");
  }
  const entries =
    matchKey === "*" || isRecursiveMatch
      ? Object.entries(obj)
      : matchKey in obj
      ? [[matchKey, obj[matchKey]]]
      : [];

  addOptions(res, objBlock, options);
  entries.forEach(([key, values]) => {
    const v = findValueAtBlockHeight(values, b);
    if (!v) {
      return;
    }
    const o = v?.o ?? (v?.i !== undefined ? values[v.i].o : null);
    if (o) {
      if (keys.length > 1 || isRecursiveMatch) {
        // Going deeper
        const innerMap = jsonGetInnerObject(res, key);
        if (keys.length > 1) {
          recursiveGet(innerMap, o, v.b, keys.slice(1), b, options);
        }
        if (isRecursiveMatch) {
          // Non skipping step in.
          recursiveGet(innerMap, o, v.b, keys, b, options);
        }
      } else {
        const innerValue = findValueAtBlockHeight(v[""] || [], b);
        if (isString(innerValue?.s)) {
          jsonSetKey(res, key, innerValue, options);
        } else {
          // mismatch skipping
        }
      }
    } else if (isString(v?.s)) {
      if (keys.length === 1) {
        jsonSetKey(res, key, v, options);
      }
    }
  });
};

const recursiveKeys = (res, obj, keys, b, options) => {
  const matchKey = keys[0];
  const entries =
    matchKey === "*"
      ? Object.entries(obj)
      : matchKey in obj
      ? [[matchKey, obj[matchKey]]]
      : [];
  entries.forEach(([key, values]) => {
    const v = findValueAtBlockHeight(values, b);
    if (!v) {
      return;
    }
    const o = v?.o ?? (v?.i !== undefined ? values[v.i].o : null);
    if (keys.length === 1) {
      if (o || isString(v?.s)) {
        const newValue =
          options.returnType === KeysReturnType.History
            ? extractBlockHistory(values, b)
            : options.returnType === KeysReturnType.BlockHeight
            ? v.b
            : true;
        jsonMapSetValue(res, key, newValue);
      }
    } else if (o) {
      const innerMap = jsonGetInnerObject(res, key);
      recursiveKeys(innerMap, o, keys.slice(1), b, options);
    }
  });
};

const recursiveCleanup = (o) => {
  let hasKeys = false;
  Object.entries(o).forEach(([key, value]) => {
    if (isObject(value)) {
      const newValue = recursiveCleanup(value);
      if (!newValue) {
        delete o[key];
      } else {
        hasKeys = true;
      }
    } else if (!key.startsWith(":")) {
      hasKeys = true;
    }
  });
  return hasKeys ? o : null;
};

(async () => {
  const state = loadJson(StateFilename, true) ||
    loadJson(SnapshotFilename, true) || {
      data: {},
    };
  blockTimestamps = state.blockTimes = state.blockTimes || {};

  const receiptFetcher = await Receipts.init(state?.lastReceipt);

  const applyReceipts = (receipts) => {
    if (receipts.length === 0) {
      return;
    }
    let aggregatedData = {};
    let blockHeight = 0;
    receipts.forEach((receipt) => {
      let receiptBlockHeight = parseInt(receipt.block_height);
      state.blockTimes[receiptBlockHeight] = Math.round(
        parseFloat(receipt.block_timestamp) / 1e6
      );
      if (receiptBlockHeight > blockHeight) {
        if (blockHeight) {
          recursiveSet(state.data, aggregatedData, blockHeight);
          aggregatedData = {};
        }
        blockHeight = receiptBlockHeight;
      }
      mergeData(aggregatedData, receipt.args.data);
    });
    recursiveSet(state.data, aggregatedData, blockHeight);
  };

  const fetchAllReceipts = async () => {
    const allReceipts = [];
    while (true) {
      const receipts = await receiptFetcher.fetchReceipts();
      if (receipts.length === 0) {
        break;
      }
      allReceipts.push(...receipts);
    }
    return allReceipts;
  };

  const fetchAndApply = async () => {
    const newReceipts = await fetchAllReceipts();
    if (newReceipts.length) {
      console.log(`Fetched ${newReceipts.length} receipts.`);
    }
    applyReceipts(newReceipts);
    state.lastReceipt = receiptFetcher.lastReceipt;
  };

  await fetchAndApply();
  saveJson(state, StateFilename);

  const scheduleUpdate = (delay) =>
    setTimeout(async () => {
      await fetchAndApply();
      scheduleUpdate(250);
    }, delay);

  const stateGet = (keys, b, o) => {
    if (!Array.isArray(keys)) {
      throw new Error("keys is not an array");
    }
    b = b !== null && b !== undefined ? parseInt(b) : undefined;
    const res = {};
    keys.forEach((key) => {
      if (!isString(key)) {
        throw new Error("key is not a string");
      }
      const path = key.split("/");
      if (path.length === 0) {
        throw new Error("key is empty");
      }
      recursiveGet(res, state.data, b, path, b, {
        withBlockHeight: o?.with_block_height || o?.withBlockHeight,
        withTimestamp: o?.with_timestamp || o?.withTimestamp,
      });
    });
    return recursiveCleanup(res) || {};
  };

  const stateKeys = (keys, b, o) => {
    if (!Array.isArray(keys)) {
      throw new Error("keys is not an array");
    }
    b = b !== null && b !== undefined ? parseInt(b) : undefined;
    const res = {};
    keys.forEach((key) => {
      if (!isString(key)) {
        throw new Error("key is not a string");
      }
      const path = key.split("/");
      if (path.length === 0) {
        throw new Error("key is empty");
      }
      recursiveKeys(res, state.data, path, b, {
        returnType:
          o?.return_type in KeysReturnType
            ? o.return_type
            : KeysReturnType.True,
      });
    });
    return recursiveCleanup(res) || {};
  };

  // console.log(
  //   JSON.stringify(
  //     stateKeys(["*/post/meme"], undefined, {
  //       return_type: KeysReturnType.BlockHeight,
  //     }),
  //     undefined,
  //     2
  //   )
  // );
  // return;

  //
  // const subs = loadJson(SubsFilename, true) || {};
  //
  // const WS_PORT = process.env.WS_PORT || 7071;
  //
  // const wss = new WebSocket.Server({ port: WS_PORT });
  // console.log("WebSocket server listening on http://localhost:%d/", WS_PORT);
  //
  // const wsClients = new Map();
  // const wsSubs = new Map();
  //
  // const recursiveFilter = (filter, obj) => {
  //   if (isObject(filter) && isObject(obj)) {
  //     return Object.keys(filter).every((key) =>
  //       recursiveFilter(filter[key], obj[key])
  //     );
  //   } else if (Array.isArray(filter) && Array.isArray(obj)) {
  //     return filter.every((value, index) => recursiveFilter(value, obj[index]));
  //   } else {
  //     return filter === obj;
  //   }
  // };
  //
  // const getFilteredEvents = (events, filter) => {
  //   return events.filter((event) =>
  //     Array.isArray(filter)
  //       ? filter.some((f) => recursiveFilter(f, event))
  //       : isObject(filter)
  //       ? recursiveFilter(filter, event)
  //       : false
  //   );
  // };
  //
  // processEvents = async (events) => {
  //   Object.values(subs).forEach((sub) => {
  //     const filteredEvents = getFilteredEvents(events, sub.filter);
  //     // console.log("Filtered events:", filteredEvents.length);
  //     if (filteredEvents.length > 0 && sub.url) {
  //       sub.totalPosts = (sub.totalPosts || 0) + 1;
  //       axios({
  //         method: "post",
  //         url: sub.url,
  //         data: {
  //           secret: sub.secret,
  //           events: filteredEvents,
  //         },
  //         timeout: PostTimeout,
  //       })
  //         .then(() => {
  //           sub.successPosts = (sub.successPosts || 0) + 1;
  //         })
  //         .catch(() => {
  //           sub.failedPosts = (sub.failedPosts || 0) + 1;
  //         });
  //     }
  //   });
  //
  //   [...wsSubs.values()].forEach((sub) => {
  //     const filteredEvents = getFilteredEvents(events, sub.filter);
  //     if (filteredEvents.length > 0 && wsClients.has(sub.ws)) {
  //       try {
  //         sub.ws.send(
  //           JSON.stringify({
  //             secret: sub.secret,
  //             events: filteredEvents,
  //           })
  //         );
  //       } catch (e) {
  //         console.log("Failed to send events to ws", e);
  //       }
  //     }
  //   });
  // };
  //
  // const saveWsSubs = () => {
  //   saveJson(
  //     [...wsSubs.values()].map(
  //       ({ xForwardedFor, remoteAddress, secret, filter }) => ({
  //         xForwardedFor,
  //         remoteAddress,
  //         secret,
  //         filter,
  //       })
  //     ),
  //     WsSubsFilename
  //   );
  // };
  //
  // const getPastEvents = (filter, limit) => {
  //   const filteredEvents = getFilteredEvents(pastEvents, filter);
  //   limit = Math.min(
  //     Math.max(parseInt(limit) || DefaultEventsLimit, 0),
  //     Math.min(MaxEventsLimit, filteredEvents.length)
  //   );
  //   return filteredEvents.slice(filteredEvents.length - limit);
  // };
  //
  // wss.on("connection", (ws, req) => {
  //   console.log("WS Connection open");
  //
  //   wsClients.set(ws, null);
  //
  //   ws.on("close", () => {
  //     console.log("connection closed");
  //     wsClients.delete(ws);
  //     wsSubs.delete(ws);
  //     saveWsSubs();
  //   });
  //
  //   ws.on("message", (messageAsString) => {
  //     try {
  //       const message = JSON.parse(messageAsString);
  //       if ("filter" in message && "secret" in message) {
  //         console.log("WS subscribed to events");
  //         wsSubs.set(ws, {
  //           ws,
  //           secret: message.secret,
  //           filter: message.filter,
  //           xForwardedFor: req.headers["x-forwarded-for"],
  //           remoteAddress: req.connection.remoteAddress,
  //         });
  //         saveWsSubs();
  //         if (message.fetch_past_events) {
  //           ws.send(
  //             JSON.stringify({
  //               secret: message.secret,
  //               events: getPastEvents(
  //                 message.filter,
  //                 message.fetch_past_events
  //               ),
  //               note: "past",
  //             })
  //           );
  //         }
  //       }
  //     } catch (e) {
  //       console.log("Bad message", e);
  //     }
  //   });
  // });

  scheduleUpdate(1);

  // Save state once a minute
  setInterval(() => {
    saveJson(state, StateFilename);
  }, 60000);

  router.post("/get", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      const keys = body.keys;
      if (!keys) {
        throw new Error(`Missing keys`);
      }
      const blockHeight = body.blockHeight;
      const options = body.options;
      console.log("/get", keys, blockHeight, options);
      ctx.body = JSON.stringify(stateGet(keys, blockHeight, options));
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  router.post("/keys", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      const keys = body.keys;
      if (!keys) {
        throw new Error(`Missing keys`);
      }
      const blockHeight = body.blockHeight;
      const options = body.options;
      console.log("/keys", keys, blockHeight, options);
      ctx.body = JSON.stringify(stateKeys(keys, blockHeight, options));
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  // router.post("/subscribe", (ctx) => {
  //   ctx.type = "application/json; charset=utf-8";
  //   try {
  //     const body = ctx.request.body;
  //     if ("filter" in body && "url" in body && "secret" in body) {
  //       const secret = body.secret;
  //       if (secret in subs) {
  //         throw new Error(`Secret "${secret}" is already present`);
  //       }
  //       subs[secret] = {
  //         ip: ctx.request.ip,
  //         filter: body.filter,
  //         url: body.url,
  //         secret,
  //       };
  //       saveJson(subs, SubsFilename);
  //       ctx.body = JSON.stringify(
  //         {
  //           ok: true,
  //         },
  //         null,
  //         2
  //       );
  //     } else {
  //       ctx.body = 'err: Required fields are "filter", "url", "secret"';
  //     }
  //   } catch (e) {
  //     ctx.body = `err: ${e}`;
  //   }
  // });
  //
  // router.post("/unsubscribe", (ctx) => {
  //   ctx.type = "application/json; charset=utf-8";
  //   try {
  //     const body = ctx.request.body;
  //     const secret = body.secret;
  //     if (secret in subs) {
  //       delete subs[secret];
  //       saveJson(subs, SubsFilename);
  //       ctx.body = JSON.stringify(
  //         {
  //           ok: true,
  //         },
  //         null,
  //         2
  //       );
  //     } else {
  //       ctx.body = 'err: No subscription found for "secret"';
  //     }
  //   } catch (e) {
  //     ctx.body = `err: ${e}`;
  //   }
  // });

  app
    .use(logger())
    .use(cors())
    .use(bodyParser())
    .use(router.routes())
    .use(router.allowedMethods());

  const PORT = process.env.PORT || 3000;
  app.listen(PORT);
  console.log("Listening on http://localhost:%d/", PORT);
})();
