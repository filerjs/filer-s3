var AWS = require('aws-sdk');

function S3Context(options) {
  this.readOnly = options.isReadOnly;
  this.keyPrefix = options.keyPrefix;
  this.s3bucket = options.s3bucket;
}

function prefixKey(prefix, key) {
  return prefix + "/" + key;
}

function _put(s3bucket, keyPrefix, key, value, length, ContentType, callback) {
  if(this.readOnly) {
    return callback(new Error("Write operation on readOnly context."));
  }
  var keyPath = prefixKey(keyPrefix, key);
  var params = {
    Key: keyPath,
    Body: value,
    ContentLength: length,
    ContentType: ContentType
  };
  s3bucket.putObject(params, function(err) {
    if(err) {
      return callback(err);
    }
    callback();
  });
}

S3Context.prototype.putObject = function(key, value, callback) {
  var json = JSON.stringify(value);
  _put(this.s3bucket, this.keyPrefix, key, json, Buffer.byteLength(json), "application/json", callback);
};
S3Context.prototype.putBuffer = function(key, value, callback) {
  _put(this.s3bucket, this.keyPrefix, key, value, value.length, "application/octet-stream", callback);
};

S3Context.prototype.delete = function (key, callback) {
  if(this.readOnly) {
    return callback(new Error("Write operation on readOnly context."));
  }
  key = prefixKey(this.keyPrefix, key);
  this.s3bucket.deleteObject({Key: key}, function(err) {
    if(err) {
      return callback(err);
    }
    callback();
  });
};

S3Context.prototype.clear = function(callback) {
  if(this.readOnly) {
    return callback(new Error("Write operation on readOnly context."));
  }

  var options = {
    Prefix: this.keyPrefix
  };
  getAllObjects(options, callback, []);

  var s3bucket = this.s3bucket;

  function getAllObjects(options, callback, aggregate) {
    s3bucket.listObjects(options, function(err, data) {
      if(err) {
        return callback(err);
      }

      if(data.Contents.length === 0) {
        return callback();
      }

      aggregate = aggregate.concat(data.Contents.map(function(content) {
        return { Key: content.Key };
      }));

      // We have to check  whether or not this object listing is complete.
      if (data.IsTruncated) {
        options.Marker = data.Contents[data.Contents.length - 1].Key;
        getAllObjects(options, callback, aggregate);
      }

      var params = {
        Delete: {
          Objects: aggregate
        }
      };
      s3bucket.deleteObjects(params, function(err) {
        if(err) {
          return callback(err);
        }
        callback();
      });
    });
  }

};

function _get(s3bucket, keyPrefix, type, key, callback) {
  var keyPath = prefixKey(keyPrefix, key);
  var params = {
    Key: keyPath,
    ResponseContentType: type
  };

  s3bucket.getObject(params, function(err, data) {
    if(err && err.code !== "NoSuchKey") {
      return callback(err);
    }
    callback(null, data && data.Body || null);
  });
}

S3Context.prototype.getObject = function(key, callback) {
  _get(this.s3bucket, this.keyPrefix, "application/json", key, function(err, data) {
    if(err) {
      return callback(err);
    }

    if(data) {
      try {
        data = JSON.parse(data);
      } catch(e) {
        return callback(e);
      }
    }

    callback(null, data);
  });
};
S3Context.prototype.getBuffer = function(key, callback) {
  _get(this.s3bucket, this.keyPrefix, "application/octet-stream", key, callback);
};

function S3Provider(options) {
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
    return callback(new Error("Missing keyPrefix"));
  }

  AWS.config.update({
    accessKeyId: this.key,
    secretAccessKey: this.secret
  });
  this.s3bucket = new AWS.S3({params: {Bucket: this.bucket}});
  callback();
};

S3Provider.prototype.getReadOnlyContext = function() {
  return new S3Context({isReadOnly: true, keyPrefix: this.keyPrefix, s3bucket: this.s3bucket});
};

S3Provider.prototype.getReadWriteContext = function() {
  return new S3Context({isReadOnly: false, keyPrefix: this.keyPrefix, s3bucket: this.s3bucket});
};

module.exports = S3Provider;
