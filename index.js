var knox = require("knox"),
    Buffer = require('filer').Buffer,
    s3;

function S3Context(options) {
  this.readOnly = options.isReadOnly;
  this.keyPrefix = options.keyPrefix;
}

function prefixKey(prefix, key) {
  return prefix + "/" + key;
}

S3Context.prototype.put = function (key, value, callback) {
  if(this.readOnly) {
    return callback(new Error("Write operation on readOnly context."));
  }
  key = prefixKey(this.keyPrefix, key);
  // We do extra work to make sure typed buffer survive
  // being stored on disk and still get the right prototype later.
  if (Buffer.isBuffer(value)) {
    var data = value.toJSON();
    value = {
      __isBuffer: true,
      // Deal with difference between versions of node and .toJSON()
      __data: data.data || data
    };
  }
  value = JSON.stringify(value);
  var headers = {
    'Content-Length': Buffer.byteLength(value),
    'application/type': 'application/json'
  };

  function onError() {
    callback(new Error("Unable to put key to S3"));
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
    return callback(new Error("Write operation on readOnly context."));
  }
  key = prefixKey(this.keyPrefix, key);
  s3.del(key).on('response', function (res) {
    if(res.statusCode === 403) {
      return callback(new Error("403. Permission denied"));
    }
    callback(null);
  }).end();
};

S3Context.prototype.clear = function (callback) {
  if(this.readOnly) {
    return callback(new Error("Write operation on readOnly context."));
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
          return callback(new Error("403. Permission denied"));
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
          // Deal with special-cased flattened typed buffer (see put() below)
          if(value.__isBuffer) {
            value = new Buffer(value.__data);
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
    callback(new Error("Missing keyPrefix"));
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
    callback(new Error("Unable to connect to s3. " + e));
  }
};

S3Provider.prototype.getReadOnlyContext = function() {
  return new S3Context({isReadOnly: true, keyPrefix: this.keyPrefix});
};

S3Provider.prototype.getReadWriteContext = function() {
  return new S3Context({isReadOnly: false, keyPrefix: this.keyPrefix});
};

module.exports = S3Provider;
