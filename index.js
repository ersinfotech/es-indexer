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
  var baseUrl, client, delay, es, getDataAsync, getMaxId, index, initMaxId, interval, maxIdPath, type;
  es = options.es, maxIdPath = options.maxIdPath, initMaxId = options.initMaxId, getMaxId = options.getMaxId, getDataAsync = options.getDataAsync;
  baseUrl = es.baseUrl, index = es.index, type = es.type;
  client = new elasticsearch.Client({
    hosts: baseUrl
  });
  delay = 0;
  return fs.readFileAsync(maxIdPath, {
    encoding: 'utf-8'
  }).then(function(result) {
    return maxId = result;
  })["catch"](function() {
    return maxId = initMaxId || 0;
  }).then(interval = function() {
    return getDataAsync(maxId).then(function(results) {
      if (!results.length) {
        delay = 30;
        log("No data. sleep for " + delay + " seconds.");
        return;
      }
      delay = 0;
      log("From " + maxId + ", got " + results.length + " data.");
      return client.bulk({
        body: (function() {
          return _.chain(results).map(function(result) {
            return [
              {
                index: {
                  _index: index,
                  _type: type,
                  _id: result._id || result.id
                }
              }, result
            ];
          }).flatten().value();
        })()
      }).then(function() {
        return maxId = getMaxId(results);
      }).then(function() {
        return fs.writeFileAsync(maxIdPath, maxId);
      }).then(function() {
        return log("Indexed success");
      });
    })["catch"](function(err) {
      log(err);
      return delay = 30;
    }).delay(1000 * delay).then(interval);
  });
};

exports.setMaxId = function(data) {
  return maxId = data;
};
