const EventProcessing = {
  None: 0,
  ObjectRequired: 1,
  ConvertToObject: 2,
  ConvertToValue: 3,
};

const Event = {
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
    eventType: Event.Profile,
    path: "profile/**",
    processing: EventProcessing.ObjectRequired,
  },
  {
    eventType: Event.Widget,
    path: "widget/*/**",
    processing: EventProcessing.ConvertToObject,
  },
  {
    eventType: Event.FollowEdge,
    path: "graph/follow/*",
    processing: EventProcessing.ObjectRequired,
  },
  {
    eventType: Event.HideEdge,
    path: "graph/hide/*",
    processing: EventProcessing.ObjectRequired,
  },
  {
    eventType: Event.Post,
    path: "post/main",
    processing: EventProcessing.ConvertToValue,
  },
  {
    eventType: Event.Comment,
    path: "post/comment",
    processing: EventProcessing.ConvertToValue,
  },
  {
    eventType: Event.Settings,
    path: "settings/**",
    processing: EventProcessing.ObjectRequired,
  },
];

const EventIndexKeys = {
  like: Event.IndexLike,
  notify: Event.IndexNotify,
  post: Event.IndexPost,
  comment: Event.IndexComment,
  hashtag: Event.IndexHashtag,
  tosAccept: Event.IndexTosAccept,
  flag: Event.IndexFlag,
  repost: Event.IndexRepost,
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
  Event,
  EventDataPatterns,
  EventProcessing,
  EventIndexKeys,
  makeEvent,
};
