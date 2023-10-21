const {
  isObject,
  isString,
  sortKeys,
  blockCmp,
  keyToPath,
} = require("./utils");
const bounds = require("binary-search-bounds");
const {
  makeEvent,
  EventDataPatterns,
  EventProcessing,
  EventIndexKeys,
} = require("./events");

const KeyBlockHeight = ":block";
const KeyTimestamp = ":timestamp";
const ErrorUnknownType = "newValue is not object, null or string";

const KeysReturnType = {
  True: "True",
  BlockHeight: "BlockHeight",
  History: "History",
};

const getInnerMap = (map, key) => {
  if (!map.has(key)) {
    map.set(key, new Map());
  }
  return map.get(key);
};

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

const findValueAtBlockHeight = (values, b) =>
  b !== undefined
    ? values?.[bounds.le(values, { b }, blockCmp)]
    : values?.length
    ? values[values.length - 1]
    : undefined;

const extractBlockHistory = (values, b) =>
  values
    .slice(
      0,
      b !== undefined ? bounds.le(values, { b }, blockCmp) + 1 : values.length
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
    if (options.exactBlockMatch && v.b !== b) {
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
    const o = getValuesObject(values, b);
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

const addIndexValue = ({
  events,
  indexObj,
  accountId,
  action,
  value,
  blockHeight,
}) => {
  let objs;
  try {
    const parsed = JSON.parse(value);
    objs = Array.isArray(parsed) ? parsed : [parsed];
  } catch {
    // ignore failed indices.
    return;
  }

  try {
    objs.forEach(({ key, value }) => {
      try {
        if (key === undefined || value === undefined) {
          // Not a valid index.
          return;
        }

        sortKeys(key);
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
        addEventFromIndex({
          events,
          key,
          action,
          accountId,
          value,
          blockHeight,
        });
        // console.log("Added index", indexKey, indexValue);
      } catch {
        // ignore failed indices.
      }
    });
  } catch {
    // ignore failed indices.
  }
};

const buildIndexForBlock = ({ data, indexObj, blockHeight, events }) => {
  Object.entries(data).forEach(([accountId, account]) => {
    const index = account?.index;
    if (isObject(index)) {
      Object.entries(index).forEach(([action, value]) => {
        if (isString(value)) {
          addIndexValue({
            events,
            indexObj,
            accountId,
            action,
            value,
            blockHeight,
          });
        } else if (isObject(value)) {
          const emptyKeyValue = value[""];
          if (isString(emptyKeyValue)) {
            addIndexValue({
              events,
              indexObj,
              accountId,
              action,
              value: emptyKeyValue,
              blockHeight,
            });
          }
        }
      });
    }
  });
};

const buildIndex = ({ data, indexObj, events }) => {
  Object.entries(data).forEach(([accountId, accountValues]) => {
    let o = getValuesObject(accountValues);

    const indexValues = o?.index;
    if (!indexValues) {
      return;
    }
    o = getValuesObject(indexValues);
    if (!isObject(o)) {
      return;
    }
    Object.entries(o).forEach(([action, values]) => {
      values.forEach((v) => {
        // Only need the first node, since there is no node deletion
        if (v.o) {
          const emptyKeyValues = v.o[""] || [];
          emptyKeyValues.forEach((v) => {
            if (v.s !== undefined) {
              addIndexValue({
                events,
                indexObj,
                accountId,
                action,
                value: v.s,
                blockHeight: v.b,
              });
            }
          });
        } else if (v.s !== undefined) {
          addIndexValue({
            events,
            indexObj,
            accountId,
            action,
            value: v.s,
            blockHeight: v.b,
          });
        }
      });
    });
  });
  // sort indices by block height.
  Object.values(indexObj).forEach((values) => {
    values.sort((a, b) => a.b - b.b);
  });
};

const getValuesObject = (values, b) => {
  const v = findValueAtBlockHeight(values, b);
  return v?.o ?? (v?.i !== undefined ? values[v.i].o : null);
};

// Extracts changes from the account object for a given path.
const extractChanges = (accountObject, path, b) => {
  const keys = keyToPath(path);
  let res = {};
  recursiveGet(res, accountObject, b, keys, b, {
    returnDeleted: true,
    exactBlockMatch: true,
  });
  for (const key of keys) {
    if (key === "**" || key === "*") {
      break;
    }
    res = res?.[key];
  }
  return res;
};

const extractAllChanges = (accountObject, path) => {
  const keys = keyToPath(path);
  let o = accountObject;
  let vs = null;
  for (const key of keys) {
    if (key === "**" || key === "*") {
      break;
    }
    vs = o?.[key];
    o = getValuesObject(vs);
  }

  return (
    vs?.map((v) => ({
      blockHeight: v.b,
      changes: extractChanges(accountObject, path, v.b),
    })) || []
  );
};

// This method goes through all data for all block and extracts all known events.
// The list of events it extracts:
// - Profile modified
// - Widget modified
// - Follow Edge created or deleted
// - Post created
// - Comment created
// - Settings modified
// - Hide Edge created
const buildEventsFromData = ({ data, events }) => {
  Object.entries(data).forEach(([accountId, accountValues]) => {
    const accountObject = getValuesObject(accountValues);
    if (!isObject(accountObject)) {
      return;
    }

    EventDataPatterns.forEach(({ eventType, path, processing }) => {
      extractAllChanges(accountObject, path).forEach(
        ({ blockHeight, changes }) => {
          switch (processing) {
            case EventProcessing.ObjectRequired:
              if (!isObject(changes)) {
                return;
              }
              break;
            case EventProcessing.ConvertToObject:
              if (!isObject(changes)) {
                changes = {
                  "": changes,
                };
              }
              break;
            case EventProcessing.ConvertToValue:
              if (isObject(changes)) {
                changes = changes[""];
              }
              break;
          }

          events.push(
            makeEvent({
              eventType,
              blockHeight,
              accountId,
              data: changes,
            })
          );
        }
      );
    });
  });
};

const addEventFromIndex = ({
  events,
  key,
  action,
  accountId,
  value,
  blockHeight,
}) => {
  if (EventIndexKeys.hasOwnProperty(action)) {
    events.push(
      makeEvent({
        eventType: EventIndexKeys[action],
        blockHeight,
        accountId,
        data: { key, value },
      })
    );
  }
};

const processStateData = ({ data, indexObj, events }) => {
  console.log("Building index...");
  buildIndex({ data, indexObj, events });
  console.log("Building events...");
  buildEventsFromData({ data, events });
  console.log("Sorting events...");
  events.sort((a, b) => a.b - b.b);
  console.log("Total events:", events.length);
};

const processBlockData = ({ data, indexObj, blockHeight, events }) => {
  buildIndexForBlock({ data, indexObj, blockHeight, events });
};

module.exports = {
  processStateData,
  processBlockData,
  recursiveSet,
  mergeData,
  recursiveGet,
  recursiveCleanup,
  recursiveKeys,
  getInnerMap,
  KeysReturnType,
};
