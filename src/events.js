const EventProcessing = {
  None: 0,
  ObjectRequired: 1,
  ConvertToObject: 2,
  ConvertToValue: 3,
};

const EventType = {
  Profile: 0,
  Widget: 1,
  FollowEdge: 2,
  HideEdge: 3,
  Post: 4,
  Comment: 5,
  Settings: 6,
  // Index
  IndexLike: 100,
  IndexNotify: 101,
  IndexPost: 102,
  IndexComment: 103,
  IndexHashtag: 104,
  IndexTosAccept: 105,
  IndexFlag: 106,
  IndexRepost: 107,
};

const EventDataPatterns = [
  {
    eventType: EventType.Profile,
    path: "profile/**",
    processing: EventProcessing.ObjectRequired,
  },
  {
    eventType: EventType.Widget,
    path: "widget/*/**",
    processing: EventProcessing.ObjectRequired,
  },
  {
    eventType: EventType.FollowEdge,
    path: "graph/follow/*",
    processing: EventProcessing.ObjectRequired,
  },
  {
    eventType: EventType.HideEdge,
    path: "graph/hide/*",
    processing: EventProcessing.ObjectRequired,
  },
  {
    eventType: EventType.Post,
    path: "post/main",
    processing: EventProcessing.ConvertToValue,
  },
  {
    eventType: EventType.Comment,
    path: "post/comment",
    processing: EventProcessing.ConvertToValue,
  },
  {
    eventType: EventType.Settings,
    path: "settings/**",
    processing: EventProcessing.ObjectRequired,
  },
];

const EventIndexKeys = {
  like: EventType.IndexLike,
  notify: EventType.IndexNotify,
  post: EventType.IndexPost,
  comment: EventType.IndexComment,
  hashtag: EventType.IndexHashtag,
  tosAccept: EventType.IndexTosAccept,
  flag: EventType.IndexFlag,
  repost: EventType.IndexRepost,
};

const makeEvent = ({ eventType, accountId, blockHeight, data }) => {
  return {
    t: eventType,
    a: accountId,
    b: blockHeight,
    d: data,
  };
};

module.exports = {
  EventType,
  EventDataPatterns,
  EventProcessing,
  EventIndexKeys,
  makeEvent,
};
