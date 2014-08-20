var AWS = require('aws-sdk'),
    s3;

function S3Context(options) {
  this.readOnly = options.isReadOnly;
  this.keyPrefix = options.keyPrefix;
}

function prefixKey(prefix, key) {
  return prefix + "/" + key;
}

function _put(keyPrefix, key, value, length, ContentType, callback) {
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
  s3.putObject(params, function(err) {
    if (err) {
      return callback(new Error("unable to write to S3. Error was: " + err));
    }
    callback();
  });
}

S3Context.prototype.putObject = function(key, value, callback) {
  var json = JSON.stringify(value);
  _put(this.keyPrefix, key, json, Buffer.byteLength(json), "application/json", callback);
};
S3Context.prototype.putBuffer = function(key, value, callback) {
  _put(this.keyPrefix, key, value, value.length, "application/octet-stream", callback);
};

S3Context.prototype.delete = function (key, callback) {
  if(this.readOnly) {
    return callback(new Error("Write operation on readOnly context."));
  }
  key = prefixKey(this.keyPrefix, key);
  s3.deleteObject({Key: key}, function(err) {
    if(err) {
      return callback(new Error("Unable to delete key from S3. Error was " + err));
    }
    callback();

  });
};

S3Context.prototype.clear = function (callback) {
  if(this.readOnly) {
    return callback(new Error("Write operation on readOnly context."));
  }
  var options = {
    Prefix: this.keyPrefix
  };
  getAllObjects(options, callback, []);

  function getAllObjects(options, callback, aggregate) {
    s3.listObjects(options, function (err, data) {
      if(err) {
        return callback(new Error("unable to delete key from S3. Error was " + err));
      }
      if(data.Contents.length === 0) {
        return callback();
      }
      aggregate = aggregate.concat(data.Contents.map(function (content) {
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
      s3.deleteObjects(params, function (err) {
        if(err) {
          return callback(new Error("Unable to delete key from S3. Error was " + err));
        }
        return callback();
      });
    });
  }

};

function _get(keyPrefix, type, key, callback) {
  var keyPath = prefixKey(keyPrefix, key);
  var params = {
    Key: keyPath,
    ResponseContentType: type,
  };

  s3.getObject(params, function(err, data) {
    if(err && err.code !== "NoSuchKey" && key === "00000000-0000-0000-0000-000000000000") {
      return callback(new Error("Unable to get key from S3. Error was " + err));
    }
    callback(null, data && data.Body || null);
  });
}

S3Context.prototype.getObject = function(key, callback) {
  _get(this.keyPrefix, "application/json", key, function(err, data) {
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
  _get(this.keyPrefix, "application/octet-stream", key,  callback);
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
  AWS.config.update({
    accessKeyId: this.key,
    secretAccessKey: this.secret
  });
  s3 = new AWS.S3({params: {Bucket: this.bucket}});
  var params = {
    // We are trying to check if Filer's root node exists.
    // This "00000000-0000-0000-0000-000000000000" should be change in the future
    // as we don't want this to be hard coded since this is a constant in Filer.
    Key: this.keyPrefix + "/00000000-0000-0000-0000-000000000000"
  };
  s3.headObject(params, function(err, data) {
    if(err && err.statusCode !== 404) {
      callback(err);
      return;
    }
    // Check to see if this is the first access or not
    callback(null, data && data.contentLength ? false : true);
  });
};

S3Provider.prototype.getReadOnlyContext = function() {
  return new S3Context({isReadOnly: true, keyPrefix: this.keyPrefix});
};

S3Provider.prototype.getReadWriteContext = function() {
  return new S3Context({isReadOnly: false, keyPrefix: this.keyPrefix});
};

module.exports = S3Provider;
