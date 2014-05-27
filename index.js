var util = require("./lib/utils");
var knox = require("knox");
var s3;

function S3Context(options) {
  this.readOnly = options.isReadOnly;
  this.keyPrefix = options.keyPrefix;
}

function prefixKey(prefix, key) {
  return prefix + "/" + key;
}

S3Context.prototype.put = function (key, value, callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }
  key = prefixKey(this.keyPrefix, key);
  // We do extra work to make sure typed arrays survive
  // being stored in S3 and still get the right prototype later.
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
    'application/type': 'application/json'
  };

  function onError() {
    callback("Error: Unable to put key to S3");
  }

  s3.put(key, headers)
    .on("error", onError)
    .on("response", function (res) {
      if (res.statusCode !== 200) {
        onError();
      }
      callback(null);
    })
    .end(value);
};

S3Context.prototype.delete = function (key, callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }
  key = prefixKey(this.keyPrefix, key);
  s3.del(key).on('response', function (res) {
    if(res.statusCode === 403) {
      return callback("Error: 403. Permission denied");
    }
    callback(null);
  }).end();
};

S3Context.prototype.clear = function (callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }
  var options = {
    prefix: this.keyPrefix
  };
  getAllObjects(options, callback, []);

  function getAllObjects(options, callback, aggregate) {
    s3.list(options, function (err, data) {
      aggregate = aggregate.concat(data.Contents.map(function (content) {
        return content.Key;
      }));
      if (data.IsTruncated) {
        options.marker = data.Contents[data.Contents.length - 1].Key;
        getAllObjects(options, callback, aggregate);
      }
      s3.deleteMultiple(aggregate, function (err, res) {
        if(res.statusCode === 403) {
          return callback("Error: 403. Permission denied");
        }
        return callback(null);
      });
    });
  }

};

S3Context.prototype.get = function (key, callback) {
  key = prefixKey(this.keyPrefix, key);
  s3.get(key).on('response', function (res) {
    // If object is not found for this key then return null
    if (res.statusCode === 404) {
      return callback(null, null);
    }
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
        return callback(e);
      }
    });
  }).end();
};

function S3Provider(options) {
  this.name = options.name;
  this.keyPrefix = options.keyPrefix;
  this.bucket = options.bucket;
  this.key = options.key;
  this.secret = options.secret;
}

S3Provider.isSupported = function() {
  return (typeof module !== 'undefined' && module.exports);
};

S3Provider.prototype.open = function(callback) {
  if(!this.keyPrefix) {
    callback("Error: Missing keyPrefix");
    return;
  }
  try {
    s3 = knox.createClient({
      bucket: this.bucket,
      key: this.key,
      secret: this.secret
    });
    s3.list({ prefix: this.keyPrefix, maxKeys: 1 }, function(err, data) {
      if(err) {
        callback(err);
        return;
      }
      // Check to see if this is the first access or not"
      callback(null, data.Contents.length === 0);
    });
  } catch(e) {
    callback("Error: Unable to connect to s3. " + e);
  }
};

S3Provider.prototype.getReadOnlyContext = function() {
  return new S3Context({isReadOnly: true, keyPrefix: this.keyPrefix});
};

S3Provider.prototype.getReadWriteContext = function() {
  return new S3Context({isReadOnly: false, keyPrefix: this.keyPrefix});
};

module.exports = S3Provider;
