var Promise, _, elasticsearch, exports, fs, log, maxId, moment;

fs = require('fs');

_ = require('underscore');

Promise = require('bluebird');

elasticsearch = require('elasticsearch');

moment = require('moment');

Promise.promisifyAll(fs);

maxId = null;

log = function(message) {
  return console.log((moment().format('YYYY-MM-DD HH:mm:ss')) + " - " + message);
};

exports = module.exports = function(options) {
  var baseUrl, client, delay, es, getDataAsync, getMaxId, index, initMaxId, interval, maxIdPath, type, maxIdSince, maxIdUntil, setEsMapping;
  es = options.es, maxIdPath = options.maxIdPath, initMaxId = options.initMaxId, getMaxId = options.getMaxId, getDataAsync = options.getDataAsync, maxIdSince = options.maxIdSince, maxIdUntil = options.maxIdUntil, setEsMapping = options.setEsMapping, getIndex = options.getIndex, getType = options.getType;
  baseUrl = es.baseUrl, index = es.index, type = es.type;
  client = new elasticsearch.Client({
    hosts: baseUrl,
    requestTimeout: 1000 * 60 * 5
  });
  delay = 0;
  return fs.readFileAsync(maxIdPath, {
    encoding: 'utf-8'
  }).then(function(result) {
    return maxId = typeof result !== "undefined" && result !== null ? result.trim() : void 0;;
  })["catch"](function() {
    return maxId = maxIdSince || initMaxId || 0;
  }).then(function() {
    if (setEsMapping) {
      return client.indices.putMapping({
        index: index,
        type: type,
        body: {
          properties: setEsMapping
        }
      });
    }
  }).then(interval = function() {
    Promise.resolve().then(function(){
      if (maxIdUntil && maxId >= maxIdUntil) {
        throw "Maximum max id reached";
      };
      var fetchStartAt = moment();
      return getDataAsync(maxId).then(function(results) {
        if (!results.length) {
          throw "No data";
        }
        var fetchUsed = moment().diff(fetchStartAt, 'seconds', true);
        log("From " + maxId + ", got " + results.length + " data, used " + fetchUsed + " seconds");
        var indexStartAt = moment();
        return client.bulk({
          body: (function() {
            return _.chain(results).map(function(result) {
              return [
                {
                  update: {
                    _index: getIndex && getIndex(result) || index,
                    _type: getType && getType(result) || type,
                    _id: result._id || result.id
                  }
                }, {
                  doc: result,
                  doc_as_upsert: true
                }
              ];
            }).flatten().value();
          })()
        }).then(function() {
          return maxId = getMaxId(results);
        }).then(function() {
          return fs.writeFileAsync(maxIdPath, maxId);
        }).then(function() {
          delay = 0;
          var indexUsed = moment().diff(indexStartAt, 'seconds', true);
          return log("Indexed success, used " + indexUsed + " seconds");
        });
      });
    })["catch"](function(err) {
      delay = 30;
      return log(err);
    }).delay(1000 * delay).then(interval);
  });
};

exports.getMaxId = function () {
  return maxId;
}

exports.setMaxId = function(data) {
  return maxId = data;
};
