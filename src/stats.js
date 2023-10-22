const { EventType } = require("./events");
const { isString, isObject } = require("./utils");

class Post {
  constructor(accountId, blockHeight, groupId) {
    this.accountId = accountId;
    this.blockHeight = blockHeight;
    this.groupId = groupId;
    this.postId = Post.makeId(accountId, blockHeight);
    this.comments = [];
    this.reposts = new Set();
  }

  static makeId(accountId, blockHeight) {
    return JSON.stringify({
      blockHeight,
      path: `${accountId}/post/main`,
      type: "social",
    });
  }
}

class Comment {
  constructor(accountId, blockHeight, item) {
    this.accountId = accountId;
    this.blockHeight = blockHeight;
    this.item = item;
    this.itemId = JSON.stringify(item);
    this.commentId = Comment.makeId(accountId, blockHeight);
    this.likes = new Set();
  }

  static makeId(accountId, blockHeight) {
    return JSON.stringify({
      blockHeight,
      path: `${accountId}/post/comment`,
      type: "social",
    });
  }
}

class StatValue {
  constructor(index, value, blockHeight) {
    this.index = index;
    this.value = value;
    this.blocks = [blockHeight];
  }

  incBy(value, blockHeight) {
    this.value += value;
    this.blocks.push(blockHeight);
  }
}

class StatCounter {
  constructor(accountId) {
    this.stats = new Map();
    this.accountId = accountId;
  }

  incBy(key, value, blockHeight, globalStats) {
    if (!this.stats.has(key)) {
      globalStats?.incBy(key, value, blockHeight, undefined);
      const index = globalStats
        ? globalStats.nextIndex(key, this.accountId)
        : 0;
      this.stats.set(key, new StatValue(index, value, blockHeight));
    } else {
      this.stats.get(key).incBy(value, blockHeight);
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
      result.push(
        `${key}={index:${value.index}, value: ${value.value}, numBlocks:${value.blocks.length}}`
      );
    }
    return result.join("\n");
  }
}

class Account {
  constructor(accountId) {
    this.accountId = accountId;
    this.graph = {
      following: new Set(),
      followers: new Set(),
      hidden: new Set(),
      hiddenBy: new Set(),
    };
    this.widgets = new Set();
    this.posts = [];
    this.comments = [];
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

  getAccount(accountId) {
    if (!this.accounts.has(accountId)) {
      this.accounts.set(accountId, new Account(accountId));
    }
    return this.accounts.get(accountId);
  }

  incBy(key, value, event, account) {
    account = account ?? this.getAccount(event.a);
    account.stats.incBy(key, value, event.b, this.globalStats);
  }

  inc(key, event, account) {
    this.incBy(key, 1, event, account);
  }

  processEvent(event) {
    this.eventsCount++;
    const account = this.getAccount(event.a, event.b);
    this.inc("event", event, account);
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
    this.inc("profile", event, account);
    if (changes?.name) {
      this.inc("profile.name", event, account);
    }
    if (
      changes?.image?.url ||
      changes?.image?.ipfs_cid ||
      (changes?.image?.nft?.tokenId && changes?.image?.nft?.contractId)
    ) {
      this.inc("profile.image", event, account);
      if (changes?.image?.nft?.tokenId && changes?.image?.nft?.contractId) {
        this.inc("profile.image.nft", event, account);
      }
    }
    if (
      changes?.backgroundImage?.url ||
      changes?.backgroundImage?.ipfs_cid ||
      (changes?.backgroundImage?.nft?.tokenId &&
        changes?.backgroundImage?.nft?.contractId)
    ) {
      this.inc("profile.backgroundImage", event, account);
    }
    if (changes?.description) {
      this.inc("profile.description", event, account);
    }
    if (
      changes?.linktree?.twitter ||
      changes?.linktree?.github ||
      changes?.linktree?.telegram ||
      changes?.linktree?.website
    ) {
      this.inc("profile.linktree", event, account);
      if (changes?.linktree?.twitter) {
        this.inc("profile.linktree.twitter", event, account);
      }
      if (changes?.linktree?.github) {
        this.inc("profile.linktree.github", event, account);
      }
      if (changes?.linktree?.telegram) {
        this.inc("profile.linktree.telegram", event, account);
      }
      if (changes?.linktree?.website) {
        this.inc("profile.linktree.website", event, account);
      }
    }
    if (Object.keys(changes?.tags || {}).length > 0) {
      this.inc("profile.tags", event, account);
    }
  }

  processWidgetEvent(event, account) {
    const blockHeight = event.b;
    Object.entries(event.d || {}).forEach(([widgetSrc, changes]) => {
      this.inc("widget", event, account);
      if (changes?.hasOwnProperty("")) {
        this.inc("widget.code", event, account);
        const code = changes[""];
        if (isString(code) && code.length > 0) {
          this.incBy("widget.code.length", code.length, event, account);
        }
      }
      if (Object.keys(changes?.metadata || {}).length > 0) {
        this.inc("widget.metadata", event, account);
      }
      if (changes?.metadata?.hasOwnProperty("name")) {
        this.inc("widget.metadata.name", event, account);
      }
      if (changes?.metadata?.hasOwnProperty("description")) {
        this.inc("widget.metadata.description", event, account);
      }
      if (Object.keys(changes?.metadata?.image || {}).length > 0) {
        this.inc("widget.metadata.image", event, account);
      }
      if (changes?.metadata?.tags?.hasOwnProperty("app")) {
        this.inc("widget.app", event, account);
      }

      if (!account.widgets.has(widgetSrc)) {
        account.widgets.add(widgetSrc);
        this.inc("widget.unique", event, account);
      }
    });
  }

  processFollowEdgeEvent(event, account) {
    const blockHeight = event.b;
    Object.entries(event.d || {}).forEach(([receiverId, changes]) => {
      if (changes !== null) {
        if (!account.graph.following.has(receiverId)) {
          account.graph.following.add(receiverId);
          this.inc("graph.follow", event, account);
          const receiver = this.getAccount(receiverId, blockHeight);
          receiver.graph.followers.add(account.accountId);
          this.inc("graph.followed", event, receiver);
        }
      } else {
        if (account.graph.following.has(receiverId)) {
          account.graph.following.delete(receiverId);
          this.inc("graph.unfollow", event, account);
          const receiver = this.getAccount(receiverId, blockHeight);
          receiver.graph.followers.delete(account.accountId);
          this.inc("graph.unfollowed", event, receiver);
        }
      }
    });
  }

  processHideEdgeEvent(event, account) {
    const blockHeight = event.b;
    Object.entries(event.d || {}).forEach(([receiverId, changes]) => {
      if (changes !== null) {
        if (!account.graph.hidden.has(receiverId)) {
          account.graph.hidden.add(receiverId);
          this.inc("graph.hide", event, account);
          const receiver = this.getAccount(receiverId, blockHeight);
          receiver.graph.hiddenBy.add(account.accountId);
          this.inc("graph.hidden", event, receiver);
        }
      } else {
        if (account.graph.hidden.has(receiverId)) {
          account.graph.hidden.delete(receiverId);
          this.inc("graph.unhide", event, account);
          const receiver = this.getAccount(receiverId, blockHeight);
          receiver.graph.hiddenBy.delete(account.accountId);
          this.inc("graph.unhidden", event, receiver);
        }
      }
    });
  }

  processPostEvent(event, account) {
    const blockHeight = event.b;
    let data;
    try {
      data = JSON.parse(event.d);
    } catch {
      // ignore
      return;
    }
    this.inc("post", event, account);
    const post = new Post(account.accountId, blockHeight, data?.groupId);
    this.posts.set(post.postId, post);
    account.posts.push(post);

    if (Object.keys(data?.image || {}).length > 0) {
      this.inc("post.image", event, account);
    }
    if (isString(data?.text) && data.text.length > 0) {
      this.inc("post.text", event, account);
      this.incBy("post.text.length", data.text.length, event, account);
    }
    if (data?.groupId) {
      this.inc("post.group", event, account);
    }
  }

  processCommentEvent(event, account) {
    const blockHeight = event.b;
    let data;
    try {
      data = JSON.parse(event.d);
    } catch {
      // ignore
      return;
    }
    this.inc("comment", event, account);
    const item = data?.item;
    const comment = new Comment(account.accountId, blockHeight, item);
    this.comments.set(comment.commentId, comment);
    account.comments.push(comment);

    if (Object.keys(data?.image || {}).length > 0) {
      this.inc("comment.image", event, account);
    }
    if (isString(data?.comment) && data.comment.length > 0) {
      this.inc("comment.text", event, account);
      this.incBy("comment.text.length", data.text.length, event, account);
    }

    const post = this.posts.get(comment.itemId);
    if (post) {
      const receiver = this.getAccount(post.accountId);
      this.inc("post.comments", event, receiver);
      post.comments.push(comment);
    }
  }

  processSettingsEvent(event, account) {}

  processIndexLikeEvent(event, account) {
    const { key, value } = event.d;
    if (!isObject(value) || !["like", "unlike"].includes(value?.type)) {
      return;
    }
    const isLike = value?.type === "like";
    const keyId = JSON.stringify(key);
    const likes = this.likes.get(keyId) ?? new Set();
    const hasLike = likes.has(account.accountId);
    if ((isLike && hasLike) || (!isLike && !hasLike)) {
      return;
    }
    if (isLike) {
      likes.add(account.accountId);
    } else {
      likes.delete(account.accountId);
    }
    this.likes.set(keyId, likes);
    this.inc(isLike ? "like" : "unlike", event, account);
    const post = this.posts.get(keyId);
    if (post) {
      const receiver = this.getAccount(post.accountId);
      this.inc(isLike ? "post.likes" : "post.unlikes", event, receiver);
      this.inc(isLike ? "like.post" : "unlike.post", event, account);
    }

    const comment = this.comments.get(keyId);
    if (comment) {
      const receiver = this.getAccount(comment.accountId);
      this.inc(isLike ? "comment.likes" : "comment.unlikes", event, receiver);
      this.inc(isLike ? "like.comment" : "unlike.comment", event, account);
    }
  }

  processIndexNotifyEvent(event, account) {}

  processIndexPostEvent(event, account) {}

  processIndexCommentEvent(event, account) {}

  processIndexHashtagEvent(event, account) {}

  processIndexTosAcceptEvent(event, account) {}

  processIndexRepostEvent(event, account) {}

  processIndexFlagEvent(event, account) {}
}

module.exports = { Stats };
