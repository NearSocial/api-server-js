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

const Order = {
  desc: "desc",
  asc: "asc",
};

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
const ErrorUnknownType = "newValue is not object, null or string";
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
      } else if (isString(newValue) || newValue === null) {
        o[""] = o[""] || [];
        o[""].push({
          s: newValue,
          b,
        });
      } else {
        throw new Error(ErrorUnknownType);
      }
    } else {
      if (isObject(newValue)) {
        const value = {
          o: v?.s !== undefined ? { "": [v] } : {},
          b,
        };
        recursiveSet(value.o, newValue, b);
        values.push(value);
        obj[key] = values;
      } else if (isString(newValue) || newValue === null) {
        values.push({
          s: newValue,
          b,
        });
        obj[key] = values;
      } else {
        throw new Error(ErrorUnknownType);
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
      } else if (isString(newValue) || newValue === null) {
        value[""] = newValue;
      } else {
        throw new Error(ErrorUnknownType);
      }
    } else if (isString(value) || value === null) {
      if (isObject(newValue)) {
        obj[key] = Object.assign({ "": value }, newValue);
      } else if (isString(newValue) || newValue === null) {
        obj[key] = newValue;
      } else {
        throw new Error(ErrorUnknownType);
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
    if (isString(v) || (options.returnDeleted && v === null)) {
      v = {
        "": v,
      };
    } else {
      v = {};
    }
    addOptions(v, newValue.b, options);
  } else if (!options.returnDeleted && v === null) {
    return;
  }
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
  } else if (isString(value) || value === null) {
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
        const innerValue = findValueAtBlockHeight(o[""] || [], b);
        if (innerValue?.s !== undefined) {
          jsonSetKey(res, key, innerValue, options);
        } else {
          // mismatch skipping
        }
      }
    } else if (v?.s !== undefined) {
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
      if (o || isString(v?.s) || (options.returnDeleted && v?.s === null)) {
        if (o && options.valuesOnly) {
          const innerValue = findValueAtBlockHeight(o[""] || [], b);
          if (
            isString(innerValue?.s) ||
            (options.returnDeleted && innerValue?.s === null)
          ) {
            const newValue =
              options.returnType === KeysReturnType.History
                ? extractBlockHistory(o[""], b)
                : options.returnType === KeysReturnType.BlockHeight
                ? innerValue.b
                : true;
            jsonMapSetValue(res, key, newValue);
          } else {
            // mismatch skipping
          }
        } else {
          const newValue =
            options.returnType === KeysReturnType.History
              ? extractBlockHistory(values, b)
              : options.returnType === KeysReturnType.BlockHeight
              ? v.b
              : true;
          jsonMapSetValue(res, key, newValue);
        }
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

const indexValue = (indexObj, accountId, action, s, blockHeight) => {
  try {
    const { key, value } = JSON.parse(s);
    if (key === undefined || value === undefined) {
      // Not a valid index.
      return;
    }
    const indexKey = JSON.stringify({
      k: key,
      a: action,
    });
    const indexValue = {
      a: accountId,
      v: value,
      b: blockHeight,
    };
    const values = indexObj[indexKey] || (indexObj[indexKey] = []);
    values.push(indexValue);
    // console.log("Added index", indexKey, indexValue);
  } catch {
    // ignore failed indices.
  }
};

const buildIndex = (data, indexObj) => {
  Object.entries(data).forEach(([accountId, accountValues]) => {
    let v = findValueAtBlockHeight(accountValues);
    let o = v?.o ?? (v?.i !== undefined ? accountValues[v.i].o : null);

    const indexValues = o?.index;
    if (!indexValues) {
      return;
    }
    v = findValueAtBlockHeight(indexValues);
    o = v?.o ?? (v?.i !== undefined ? indexValues[v.i].o : null);
    if (!isObject(o)) {
      return;
    }
    Object.entries(o).forEach(([action, values]) => {
      values.forEach((v) => {
        // Only need the first node, since there is no node deletion
        if (v.o) {
          const emptyKeyValues = o[""] || [];
          emptyKeyValues.forEach((v) => {
            if (v.s !== undefined) {
              indexValue(indexObj, accountId, action, v.s, v.b);
            }
          });
        } else if (v.s !== undefined) {
          indexValue(indexObj, accountId, action, v.s, v.b);
        }
      });
    });
  });
};

(async () => {
  const state = loadJson(StateFilename, true) ||
    loadJson(SnapshotFilename, true) || {
      data: {},
    };
  blockTimestamps = state.blockTimes = state.blockTimes || {};
  const indexObj = {};
  buildIndex(state.data, indexObj);

  const receiptFetcher = await Receipts.init(state?.lastReceipt);

  const addData = (data, blockHeight) => {
    recursiveSet(state.data, data, blockHeight);
    Object.entries(data).forEach(([accountId, account]) => {
      const index = account?.index;
      if (isObject(index)) {
        Object.entries(index).forEach(([action, value]) => {
          if (isString(value)) {
            indexValue(indexObj, accountId, action, value, blockHeight);
          } else if (isObject(value)) {
            const emptyKeyValue = value[""];
            if (isString(emptyKeyValue)) {
              indexValue(
                indexObj,
                accountId,
                action,
                emptyKeyValue,
                blockHeight
              );
            }
          }
        });
      }
    });
  };

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
          addData(aggregatedData, blockHeight);
          aggregatedData = {};
        }
        blockHeight = receiptBlockHeight;
      }
      mergeData(aggregatedData, receipt.args.data);
    });
    addData(aggregatedData, blockHeight);
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

  const keyToPath = (key) => {
    if (!isString(key)) {
      throw new Error("key is not a string");
    }
    if (key.endsWith("//")) {
      return null;
    }
    const path = key.split("/");
    if (path?.[path.length - 1] === "") {
      path.pop();
    }
    if (path.length === 0) {
      throw new Error("key is empty");
    }
    return path;
  };

  const stateGet = (keys, b, o) => {
    if (!Array.isArray(keys)) {
      throw new Error("keys is not an array");
    }
    b = b !== null && b !== undefined ? parseInt(b) : undefined;
    const res = {};
    keys.forEach((key) => {
      const path = keyToPath(key);
      if (path === null) {
        return;
      }
      recursiveGet(res, state.data, b, path, b, {
        withBlockHeight: o?.with_block_height ?? o?.withBlockHeight,
        withTimestamp: o?.with_timestamp ?? o?.withTimestamp,
        returnDeleted: o?.return_deleted ?? o?.returnDeleted,
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
      const path = keyToPath(key);
      if (path === null) {
        return;
      }
      recursiveKeys(res, state.data, path, b, {
        returnType:
          o?.return_type in KeysReturnType
            ? o.return_type
            : KeysReturnType.True,
        returnDeleted: o?.return_deleted ?? o?.returnDeleted,
        valuesOnly: o?.values_only ?? o?.valuesOnly,
      });
    });
    return recursiveCleanup(res) || {};
  };

  const stateIndex = (key, action, options) => {
    const indexKey = JSON.stringify({
      k: key,
      a: action,
    });
    let values = indexObj[indexKey];
    if (!values) {
      return [];
    }
    const accountId = options.accountId;
    const accounts = isString(accountId)
      ? { [accountId]: true }
      : Array.isArray(accountId)
      ? accountId.reduce((acc, a) => {
          acc[a] = true;
          return acc;
        }, {})
      : null;
    const limit = options.limit || values.length;
    if (limit <= 0) {
      return [];
    }
    const result = [];

    if (options.order === Order.desc) {
      const from = options.from
        ? bounds.le(values, { b: options.from }, valueCmp)
        : values.length - 1;

      for (let i = from; i >= 0; i--) {
        const v = values[i];
        if (result.length >= limit && v.b !== result[result.length - 1]?.b) {
          break;
        }
        if (!accounts || v.a in accounts) {
          result.push(v);
        }
      }
    } else {
      // Order.asc
      const from = options.from
        ? bounds.lt(values, { b: options.from }, valueCmp) + 1
        : 0;
      // Copy for performance reasons
      for (let i = from; i < values.length; i++) {
        const v = values[i];
        if (result.length >= limit && v.b !== result[result.length - 1]?.b) {
          break;
        }
        if (!accounts || v.a in accounts) {
          result.push(v);
        }
      }
    }
    return result;
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

  router.post("/index", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      const key = body.key;
      const action = body.action;
      if (!key || !action) {
        throw new Error(`"key" and "action" are required`);
      }
      const options = body.options || {};
      if (!isObject(options)) {
        throw new Error(`"options" is not an object`);
      }
      if (body.accountId) {
        options.accountId = options.accountId ?? body.accountId;
      }
      console.log("/index", key, action, options);
      ctx.body = JSON.stringify(stateIndex(key, action, options));
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
