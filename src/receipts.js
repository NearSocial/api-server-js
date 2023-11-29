const { Client } = require("pg");

require("dotenv").config();

const SocialDbAccountId = "social.near";
const MethodNameSet = "set";
const MaxLimit = 10000;
const StatusSuccess = "SUCCESS";

const Receipts = {
  init: async function (lastReceipt) {
    this.lastReceipt = lastReceipt ?? { blockHeight: 0, outcomeIndex: 0 };
    if (!process.env.DATABASE_URL) {
      return this;
    }
    const client = new Client({
      connectionString: process.env.DATABASE_URL,
    });
    await client.connect();
    this.client = client;
    return this;
  },

  fetchReceipts: async function () {
    if (!this.client) {
      return [];
    }
    const res = await this.client.query(
      `SELECT * from receipts where
        (block_height > $1 or (block_height = $1 and outcome_index > $2))
        and (account_id = $3)
        and (method_name = $4)
        and (status = $5)
      order by block_height, outcome_index limit $6`,
      [
        this.lastReceipt.blockHeight,
        this.lastReceipt.outcomeIndex,
        SocialDbAccountId,
        MethodNameSet,
        StatusSuccess,
        MaxLimit,
      ]
    );
    res.rows.forEach((row) => {
      const blockHeight = parseInt(row.block_height);
      const outcomeIndex = parseInt(row.outcome_index);
      if (
        blockHeight > this.lastReceipt.blockHeight ||
        (blockHeight === this.lastReceipt.blockHeight &&
          outcomeIndex > this.lastReceipt.outcomeIndex)
      )
        this.lastReceipt = {
          blockHeight,
          outcomeIndex,
        };
      try {
        row.args = JSON.parse(row.args);
      } catch (e) {
        row.args = null;
      }
    });
    return res.rows;
  },
};

module.exports = Receipts;
