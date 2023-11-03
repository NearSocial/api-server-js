const {
  isObject,
  isString,
  loadJson,
  saveState,
  loadState,
  sortKeys,
  blockCmp,
  keyToPath,
} = require("./utils");
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
const {
  processStateData,
  processBlockData,
  recursiveSet,
  mergeData,
  recursiveGet,
  recursiveCleanup,
  recursiveKeys,
  getInnerMap,
  KeysReturnType,
} = require("./social");
const { Stats } = require("./stats");

const StateFilename = "res/state.json";
const NewStateFilename = "res/state_v2.json";
const SnapshotFilename = "res/snapshot.json";

const Order = {
  desc: "desc",
  asc: "asc",
};

let blockTimestamps = {};

const runServer = async () => {
  console.log("Loading state...");
  const state = (await loadState(NewStateFilename, true)) ||
    loadJson(StateFilename, true) ||
    loadJson(SnapshotFilename, true) || {
      data: {},
    };

  blockTimestamps = state.blockTimes = state.blockTimes || {};
  console.log("accounts", Object.keys(state.data).length);
  console.log("blockTimestamps", Object.keys(state.blockTimes).length);
  const indexObj = {};
  const events = [];
  const stats = new Stats(blockTimestamps);

  console.log("Processing state data...");
  processStateData({ data: state.data, indexObj, events });
  console.log("Computing stats...");
  stats.processEvents(events);

  const oneBlockCache = new Map();
  const receiptFetcher = await Receipts.init(state?.lastReceipt);

  const addData = (changes, blockHeight) => {
    recursiveSet(state.data, changes, blockHeight);
    processBlockData({
      data: state.data,
      changes,
      indexObj,
      blockHeight,
      events,
    });
    stats.processEvents(events);
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
      applyReceipts(newReceipts);
      oneBlockCache.forEach((value) => value.clear());
      oneBlockCache.clear();
    }
    state.lastReceipt = receiptFetcher.lastReceipt;
  };

  console.log("Catching up...");
  await fetchAndApply();
  saveState(state, NewStateFilename);

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
    sortKeys(key);
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
        ? bounds.le(values, { b: options.from }, blockCmp)
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
        ? bounds.lt(values, { b: options.from }, blockCmp) + 1
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
    return result.map((v) => ({
      accountId: v.a,
      blockHeight: v.b,
      value: v.v,
    }));
  };

  const stateTime = (blockHeight) => {
    return Array.isArray(blockHeight)
      ? blockHeight.map((bh) => blockTimestamps[parseInt(bh)] ?? null)
      : blockTimestamps[parseInt(blockHeight)] ?? null;
  };

  const getStats = (accountId) => {
    const isArray = Array.isArray(accountId);
    const result = (isArray ? accountId : [accountId]).map((accountId) => {
      const account = stats.getAccountOptional(accountId);
      return account ? account.stats.toObject() : null;
    });
    return isArray ? result : result[0];
  };

  const getLikes = (item) => {
    const isArray = Array.isArray(item);
    const result = (isArray ? item : [item]).map((item) => {
      sortKeys(item);
      const itemId = JSON.stringify(item);
      const likes = stats.likes.get(itemId);
      return likes ? [...likes.keys()] : [];
    });
    return isArray ? result : result[0];
  };

  scheduleUpdate(1);

  // Save state once a minute
  setInterval(() => {
    saveState(state, NewStateFilename);
  }, 60000);

  const cachedJsonResult = (fn, ...args) => {
    const innerMap = getInnerMap(oneBlockCache, fn);
    const key = JSON.stringify(args);
    if (innerMap.has(key)) {
      return innerMap.get(key);
    }
    const result = JSON.stringify(fn(...args));
    innerMap.set(key, result);
    return result;
  };

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
      console.log("POST /get", keys, blockHeight, options);
      ctx.body = cachedJsonResult(stateGet, keys, blockHeight, options);
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  router.get("/get", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.query;
      let keys = body.keys;
      if (!keys) {
        throw new Error(`Missing keys`);
      }
      if (typeof keys === "string") {
        keys = [keys];
      }
      const blockHeight = body.blockHeight;
      const options = {};
      console.log("GET /get", keys, blockHeight, options);
      ctx.body = cachedJsonResult(stateGet, keys, blockHeight, options);
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
      console.log("POST /keys", keys, blockHeight, options);
      ctx.body = cachedJsonResult(stateKeys, keys, blockHeight, options);
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  router.get("/keys", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.query;
      let keys = body.keys;
      if (!keys) {
        throw new Error(`Missing keys`);
      }
      if (typeof keys === "string") {
        keys = [keys];
      }
      const blockHeight = body.blockHeight;
      const options = {};
      console.log("GET /keys", keys, blockHeight, options);
      ctx.body = cachedJsonResult(stateKeys, keys, blockHeight, options);
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
      console.log("POST /index", key, action, options);
      ctx.body = cachedJsonResult(stateIndex, key, action, options);
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  router.get("/index", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.query;
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
      console.log("GET /index", key, action, options);
      ctx.body = cachedJsonResult(stateIndex, key, action, options);
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  router.get("/time", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.query;
      const blockHeight = body.blockHeight;
      if (!blockHeight) {
        throw new Error(`"blockHeight" is required`);
      }
      console.log("GET /time", blockHeight);
      ctx.body = cachedJsonResult(stateTime, blockHeight);
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  router.post("/time", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      const blockHeight = body.blockHeight;
      if (!blockHeight) {
        throw new Error(`"blockHeight" is required`);
      }
      console.log("POST /time", blockHeight);
      ctx.body = cachedJsonResult(stateTime, blockHeight);
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  router.post("/api/experimental/stats/account", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      const accountId = body.accountId;
      if (!accountId) {
        throw new Error(`"accountId" is required`);
      }
      console.log("POST /api/experimental/stats/account", accountId);
      ctx.body = cachedJsonResult(getStats, accountId);
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  router.post("/api/experimental/likes", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      const item = body.item;
      if (!item) {
        throw new Error(`"accountId" is required`);
      }
      console.log("POST /api/experimental/likes", item);
      ctx.body = cachedJsonResult(getLikes, item);
    } catch (e) {
      ctx.status = 400;
      ctx.body = `${e}`;
    }
  });

  app
    .use(logger("combined"))
    .use(cors())
    .use(bodyParser())
    .use(router.routes())
    .use(router.allowedMethods());

  const PORT = process.env.PORT || 3000;
  app.listen(PORT);
  console.log("Listening on http://localhost:%d/", PORT);
};

module.exports = runServer;
