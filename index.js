var concat = require("concat-stream");
var util = require("./util");
var S3;

function S3Provider(options) {
  S3 = require("knox").createClient({
    bucket: options.bucket,
    key: options.key,
    secret: options.secret
  });
};

S3Provider.prototype.put = function (key, value, callback) {
  // We do extra work to make sure typed arrays survive
  // being stored in the db and still get the right prototype later.
  if (Object.prototype.toString.call(value) === "[object Uint8Array]") {
    value = {
      __isUint8Array: true,
      __array: util.u8toArray(value)
    };
  }
  value = JSON.stringify(value);
  var headers = {
    'x-amz-acl': 'public-read',
    'Content-Length': Buffer.byteLength(value),
  };
  S3.put(key, headers)
    .on("error", onError)
    .on("response", function (res) {
      if (res.statusCode !== 200) {
        onError;
      }
    })
    .end(value);
  return callback(null);

  function onError() {
    return callback("Error " + statusCode);
  }
};


S3Provider.prototype.delete = function (key, callback) {
  S3.del(key).on('response', function (res) {
    return callback(null);
  }).end();
};

S3Provider.prototype.clear = function (callback) {
  var options = {
    prefix: ""
  };
  getAllObjects(options, callback, []);

  function getAllObjects(options, callback, aggregate) {
    S3.list(options, function (err, data) {
      aggregate = aggregate.concat(data.Contents.map(function (content) {
        return content.Key;
      }));
      if (data.IsTruncated) {
        options.marker = data.Contents[data.Contents.length - 1].Key;
        getAllObjects(options, callback, aggregate);
      }
      S3.deleteMultiple(aggregate, function (err, res) {
        return callback(null);
      });
    })
  }

};

S3Provider.prototype.get = function (key, callback) {
  S3.get(key).on('response', function (res) {
    var chunks = [];
    res.on('data', function (chunk) {
      chunks.push(chunk);
    }).on('end', function () {
      var data = chunks.join('');
      return callback(data);
    });
  }).end();
};

module.exports = S3Provider;
