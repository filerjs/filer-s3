var util = require("./util");
var S3;

function S3Context(options) {
  this.readOnly = options.isReadOnly;
  this.keyPrefix = options.keyPrefix;
};

S3Context.prototype.put = function (key, value, callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.")
  }
  key = this.keyPrefix + "/" + key;
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

  function onError() {
    callback("Error " + res.statusCode);
  }

  S3.put(key, headers)
    .on("error", onError)
    .on("response", function (res) {
      if (res.statusCode !== 200) {
        return onError;
      }
      callback(null);
    })
    .end(value);
};

S3Context.prototype.delete = function (key, callback) {
  key = this.keyPrefix + "/" + key;
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.")
  }
  S3.del(key).on('response', function (res) {
    return callback(null);
  }).end();
};

S3Context.prototype.clear = function (callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.")
  }
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
    });
  }

};

S3Context.prototype.get = function (key, callback) {
  key = this.keyPrefix + "/" + key;
  S3.get(key).on('response', function (res) {
    if (res.statusCode === 404) {
      return callback("Error " + res.statusCode);
    };
    var chunks = [];
    res.on('data', function (chunk) {
      chunks.push(chunk);
    }).on('end', function () {
      var value = chunks.join('');
      try {
        if(value) {
          value = JSON.parse(value);
          // Deal with special-cased flattened typed arrays in WebSQL (see put() below)
          if(value.__isUint8Array) {
            value = new Uint8Array(value.__array);
          }
        }
        callback(null, value);
      } catch(e) {
        callback(e);
      }
    });
  }).end();
};

function S3Provider(options) {
  this.name = options.name;
  this.keyPrefix = options.keyPrefix;
}

S3Provider.isSupported = function() {
  return (typeof module !== 'undefined' && module.exports);
};

S3Provider.prototype.open = function(options, callback) {
  if(!this.keyPrefix) {
    callback("Error: Missing keyPrefix");
    return;
  }
  try {
    S3 = require("knox").createClient({
      bucket: options.bucket,
      key: options.key,
      secret: options.secret
    });
    S3.list({ prefix: this.keyPrefix, maxKeys: 1 }, function(err, data) {
      if(err) {
        callback(err);
        return;
      }
      callback(null, data.Contents.length === 0);
    });
  } catch(e) {
    callback("Error: Unable to connect to S3. " + e);
  }
};

S3Provider.prototype.getReadOnlyContext = function() {
  return new S3Context({isReadOnly: true, keyPrefix: this.keyPrefix});
};

S3Provider.prototype.getReadWriteContext = function() {
  return new S3Context({isReadOnly: false, keyPrefix: this.keyPrefix});
};

module.exports = S3Provider;
