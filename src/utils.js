const fs = require("fs");
const JSONStream = require("JSONStream");

const isObject = function (o) {
  return o === Object(o) && !Array.isArray(o) && typeof o !== "function";
};

const isString = (s) => typeof s === "string";

const sortKeys = (o) => {
  if (Array.isArray(o)) {
    o.forEach(sortKeys);
  } else if (isObject(o)) {
    Object.keys(o)
      .sort()
      .forEach((key) => {
        const value = o[key];
        delete o[key];
        o[key] = value;
        sortKeys(value);
      });
  }
};

const blockCmp = (a, b) => a.b - b.b;

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

function saveState(state, filename) {
  try {
    const ws = fs.createWriteStream(filename);
    ws.write("{", "utf-8");
    Object.entries(state).forEach(([key, value], i) => {
      if (i) {
        ws.write(",", "utf-8");
      }
      ws.write(`"${key}":`, "utf-8");
      if (key === "data") {
        ws.write("{", "utf-8");
        Object.entries(state.data).forEach(([key, value], i) => {
          if (i) {
            ws.write(",", "utf-8");
          }
          ws.write(`"${key}":${JSON.stringify(value)}`, "utf-8");
        });
        ws.write("}", "utf-8");
      } else {
        ws.write(JSON.stringify(value), "utf-8");
      }
    });
    ws.write("}", "utf-8");
    ws.end();
  } catch (e) {
    console.error("Failed to save state:", filename, e);
  }
}

function loadState(filename, ignore) {
  return new Promise((resolve, reject) => {
    try {
      const rs = fs.createReadStream(filename, { encoding: "utf-8" });
      const jsonStreamParser = JSONStream.parse();

      rs.on("error", (e) => {
        if (!ignore) {
          reject(e);
        }
        resolve(null);
      });

      jsonStreamParser.on("error", (e) => {
        if (!ignore) {
          reject(e);
        }
        console.warn("Error parsing JSON:", filename, e);
        resolve(null);
      });

      rs.pipe(jsonStreamParser);
      jsonStreamParser.on("data", (data) => {
        resolve(data);
      });

      // Handle the end of the stream
      jsonStreamParser.on("end", () => {
        console.log("State loading and parsing finished.");
      });
    } catch (e) {
      if (!ignore) {
        reject(e);
      }
      console.warn("Failed to load state:", filename, e);
      resolve(null);
    }
  });
}

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

module.exports = {
  sortKeys,
  blockCmp,
  saveJson,
  loadJson,
  isObject,
  isString,
  saveState,
  loadState,
  keyToPath,
};
