const { EventType } = require("./events");
const { blockCmp } = require("./utils");

class StatValue {
  constructor(index, blockHeight) {
    this.index = index;
    this.count = 1;
    this.first = blockHeight;
    this.last = blockHeight;
  }

  inc(blockHeight) {
    this.count++;
    this.last = blockHeight;
  }
}

class StatCounter {
  constructor(accountId) {
    this.stats = new Map();
    this.accountId = accountId;
  }

  inc(key, blockHeight, globalStats) {
    if (!this.stats.has(key)) {
      globalStats?.inc(key, blockHeight, undefined);
      const index = globalStats
        ? globalStats.nextIndex(key, this.accountId)
        : 0;
      this.stats.set(key, new StatValue(index, blockHeight));
    } else {
      this.stats.get(key).inc(blockHeight);
    }
  }

  get(key) {
    return this.stats.get(key);
  }

  nextIndex(key, accountId) {
    const value = this.get(key);
    value.accounts = value.accounts || [];
    value.accounts.push(accountId);
    return value.index++;
  }

  toString() {
    const result = [];
    for (const [key, value] of this.stats.entries()) {
      result.push(`${key}=${JSON.stringify(value)}`);
    }
    return result.join("\n");
  }
}

class Account {
  constructor(accountId) {
    this.following = new Set();
    this.followers = new Set();
    this.hidden = new Set();
    this.widgets = new Set();
    this.stats = new StatCounter(accountId);
  }
}

class Stats {
  constructor(blockTimestamps) {
    this.blockTimestamps = blockTimestamps;
    // accountId -> Account
    this.accounts = new Map();
    // item -> Set<accountId>
    this.likes = new Map();

    this.posts = new Map();
    this.comments = new Map();
    this.eventsCount = 0;

    this.globalStats = new StatCounter();
  }

  processEvents(events) {
    events.slice(this.eventsCount).forEach((event) => {
      this.processEvent(event);
    });
  }

  getAccount(accountId, blockHeight) {
    if (!this.accounts.has(accountId)) {
      this.accounts.set(accountId, new Account(accountId));
    }
    const account = this.accounts.get(accountId);
    account.stats.inc("account", blockHeight, this.globalStats);
    return account;
  }

  processEvent(event) {
    this.eventsCount++;
    const account = this.getAccount(event.a, event.b);
    switch (event.t) {
      case EventType.Profile:
        this.processProfileEvent(event, account);
        break;
      case EventType.Widget:
        this.processWidgetEvent(event, account);
        break;
      case EventType.FollowEdge:
        this.processFollowEdgeEvent(event, account);
        break;
      case EventType.HideEdge:
        this.processHideEdgeEvent(event, account);
        break;
      case EventType.Post:
        this.processPostEvent(event, account);
        break;
      case EventType.Comment:
        this.processCommentEvent(event, account);
        break;
      case EventType.Settings:
        this.processSettingsEvent(event, account);
        break;
      case EventType.IndexLike:
        this.processIndexLikeEvent(event, account);
        break;
      case EventType.IndexNotify:
        this.processIndexNotifyEvent(event, account);
        break;
      case EventType.IndexPost:
        this.processIndexPostEvent(event, account);
        break;
      case EventType.IndexComment:
        this.processIndexCommentEvent(event, account);
        break;
      case EventType.IndexHashtag:
        this.processIndexHashtagEvent(event, account);
        break;
      case EventType.IndexTosAccept:
        this.processIndexTosAcceptEvent(event, account);
        break;
      case EventType.IndexFlag:
        this.processIndexFlagEvent(event, account);
        break;
      case EventType.IndexRepost:
        this.processIndexRepostEvent(event, account);
        break;
      default:
        throw new Error(`Unknown event type: ${event.t}`);
    }
  }

  processProfileEvent(event, account) {
    const changes = event.d;
    const blockHeight = event.b;
    account.stats.inc("profile", blockHeight, this.globalStats);
    if (changes?.name) {
      account.stats.inc("profile.name", blockHeight, this.globalStats);
    }
    if (
      changes?.image?.url ||
      changes?.image?.ipfs_cid ||
      (changes?.image?.nft?.tokenId && changes?.image?.nft?.contractId)
    ) {
      account.stats.inc("profile.image", blockHeight, this.globalStats);
      if (changes?.image?.nft?.tokenId && changes?.image?.nft?.contractId) {
        account.stats.inc("profile.image.nft", blockHeight, this.globalStats);
      }
    }
    if (
      changes?.backgroundImage?.url ||
      changes?.backgroundImage?.ipfs_cid ||
      (changes?.backgroundImage?.nft?.tokenId &&
        changes?.backgroundImage?.nft?.contractId)
    ) {
      account.stats.inc(
        "profile.backgroundImage",
        blockHeight,
        this.globalStats
      );
    }
    if (changes?.description) {
      account.stats.inc("profile.description", blockHeight, this.globalStats);
    }
    if (
      changes?.linktree?.twitter ||
      changes?.linktree?.github ||
      changes?.linktree?.telegram ||
      changes?.linktree?.website
    ) {
      account.stats.inc("profile.linktree", blockHeight, this.globalStats);
      if (changes?.linktree?.twitter) {
        account.stats.inc(
          "profile.linktree.twitter",
          blockHeight,
          this.globalStats
        );
      }
      if (changes?.linktree?.github) {
        account.stats.inc(
          "profile.linktree.github",
          blockHeight,
          this.globalStats
        );
      }
      if (changes?.linktree?.telegram) {
        account.stats.inc(
          "profile.linktree.telegram",
          blockHeight,
          this.globalStats
        );
      }
      if (changes?.linktree?.website) {
        account.stats.inc(
          "profile.linktree.website",
          blockHeight,
          this.globalStats
        );
      }
    }
    if (Object.keys(changes?.tags || {}).length > 0) {
      account.stats.inc("profile.tags", blockHeight, this.globalStats);
    }
  }

  processWidgetEvent(event, account) {
    const blockHeight = event.b;
    Object.entries(event.d || {}).forEach(([widgetSrc, changes]) => {
      account.stats.inc("widget", blockHeight, this.globalStats);
      if (changes?.hasOwnProperty("")) {
        account.stats.inc("widget.code", blockHeight, this.globalStats);
      }
      if (Object.keys(changes?.metadata || {}).length > 0) {
        account.stats.inc("widget.metadata", blockHeight, this.globalStats);
      }
      if (Object.keys(changes?.metadata?.image || {}).length > 0) {
        account.stats.inc(
          "widget.metadata.image",
          blockHeight,
          this.globalStats
        );
      }
      if (changes?.metadata?.tags?.hasOwnProperty("app")) {
        account.stats.inc("widget.app", blockHeight, this.globalStats);
      }
    });
  }

  processFollowEdgeEvent(event, account) {}

  processHideEdgeEvent(event, account) {}

  processPostEvent(event, account) {}

  processCommentEvent(event, account) {}

  processSettingsEvent(event, account) {}

  processIndexLikeEvent(event, account) {}

  processIndexNotifyEvent(event, account) {}

  processIndexPostEvent(event, account) {}

  processIndexCommentEvent(event, account) {}

  processIndexHashtagEvent(event, account) {}

  processIndexTosAcceptEvent(event, account) {}

  processIndexRepostEvent(event, account) {}

  processIndexFlagEvent(event, account) {}
}

module.exports = { Stats };
