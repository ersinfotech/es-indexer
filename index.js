var Promise, _, elasticsearch, exports, fs, log, maxId, moment;

fs = require("fs");

_ = require("lodash");

Promise = require("bluebird");

elasticsearch = require("elasticsearch");

moment = require("moment");

Promise.promisifyAll(fs);

maxId = null;

log = function (message) {
  return console.log(moment().format("YYYY-MM-DD HH:mm:ss") + " - " + message);
};

exports = module.exports = function (options) {
  var baseUrl,
    client,
    delay,
    es,
    getDataAsync,
    rejectData,
    getMaxId,
    index,
    initMaxId,
    interval,
    maxIdPath,
    type,
    maxIdSince,
    maxIdUntil;

  es = options.es;
  maxIdPath = options.maxIdPath;
  initMaxId = options.initMaxId;
  getMaxId = options.getMaxId;
  getDataAsync = options.getDataAsync;
  rejectData = options.rejectData;
  maxIdSince = options.maxIdSince;
  maxIdUntil = options.maxIdUntil;
  getIndex = options.getIndex;
  getType = options.getType;
  getId = options.getId;
  baseUrl = es.baseUrl;
  index = es.index;
  type = es.type;
  httpAuth = es.httpAuth;

  client = new elasticsearch.Client({
    hosts: baseUrl,
    requestTimeout: 1000 * 60 * 5,
    httpAuth: httpAuth,
  });
  delay = 0;
  return fs
    .readFileAsync(maxIdPath, {
      encoding: "utf-8",
    })
    .then(function (result) {
      return (maxId =
        typeof result !== "undefined" && result !== null
          ? result.trim()
          : void 0);
    })
  ["catch"](function () {
    return (maxId = maxIdSince || initMaxId || 0);
  })
    .then(
      (interval = function () {
        Promise.resolve()
          .then(function () {
            if (maxIdUntil && maxId >= maxIdUntil) {
              throw "Maximum max id reached";
            }
            var fetchStartAt = moment();
            return getDataAsync(maxId).then(function (results) {
              if (!results.length) {
                throw "No data";
              }
              var fetchUsed = moment().diff(fetchStartAt, "seconds", true);
              log(
                "From " +
                maxId +
                ", got " +
                results.length +
                " data, used " +
                fetchUsed +
                " seconds"
              );
              var indexStartAt = moment();
              return client
                .bulk({
                  body: _.flatMap(results, function (result) {
                    if (rejectData && rejectData(result)) {
                      return [];
                    }
                    return [
                      {
                        update: {
                          _index: (getIndex && getIndex(result)) || index,
                          // _type: (getType && getType(result)) || type,
                          _id: (getId && getId(result)) || result.id,
                        },
                      },
                      {
                        doc: _.extend({}, result, {
                          updatedAt: new Date(),
                        }),
                        upsert: _.extend({}, result, {
                          createdAt: new Date(),
                          updatedAt: new Date(),
                        }),
                      },
                    ];
                  }),
                })
                .then(function () {
                  return (maxId = getMaxId(results));
                })
                .then(function () {
                  return fs.writeFileAsync(maxIdPath, maxId);
                })
                .then(function () {
                  delay = 0;
                  var indexUsed = moment().diff(indexStartAt, "seconds", true);
                  return log("Indexed success, used " + indexUsed + " seconds");
                });
            });
          })
        ["catch"](function (err) {
          delay = 30;
          return log(err);
        })
          .then(function () {
            return Promise.delay(1000 * delay);
          })
          .then(interval);
      })
    );
};

exports.getMaxId = function () {
  return maxId;
};

exports.setMaxId = function (data) {
  return (maxId = data);
};
