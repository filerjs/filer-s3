var expect = require('expect.js'),
    S3Provider = require(".."),
    S3Options = { bucket: "<bucket_name>", key: "<S3_KEY>", secret: "<S3_SECRET>" },
    guid = require("../lib/utils").guid,
    randomName,
    randomKeyPrefix;

describe("Filer.FileSystem.providers.S3", function() {
  it("is supported -- if it isn't, none of these tests can run.", function() {
    expect(S3Provider.isSupported).to.be.true;
  });

  it("has open, getReadOnlyContext, and getReadWriteContext instance methods", function() {
    var S3 = new S3Provider({bucket: "<bucket_name>", key: "<S3_KEY>", secret: "<S3_SECRET>", name: guid(), keyPrefix: guid()});
    expect(S3.open).to.be.a('function');
    expect(S3.getReadOnlyContext).to.be.a('function');
    expect(S3.getReadWriteContext).to.be.a('function');
  });

  describe("open an S3 provider", function() {
    var _provider;

    beforeEach(function() {
      randomName = guid();
      randomKeyPrefix = guid();
      _provider = new S3Provider({bucket: "<bucket_name>", key: "<S3_KEY>", secret: "<S3_SECRET>", name: randomName, keyPrefix: randomKeyPrefix });
    });

    afterEach(function(done){
      var s3 = require("knox").createClient(S3Options);
      var options = {
        prefix: randomKeyPrefix
      };
      getAllObjects(options, []);

      function getAllObjects(options, aggregate) {
        s3.list(options, function (err, data) {
          aggregate = aggregate.concat(data.Contents.map(function (content) {
            return content.Key;
          }));
          if (data.IsTruncated) {
            options.marker = data.Contents[data.Contents.length - 1].Key;
            getAllObjects(options, aggregate);
          }
          s3.deleteMultiple(aggregate, function (err, res) {
            if(res.statusCode === 403) {
              return callback("Error 403: Permission deined." + err);
            }
            done();
          });
        });
      }
    });

    it("should open a new S3", function(done) {
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        expect(error).not.to.exist;
        expect(firstAccess).to.be.true;
        done();
      });
    });
  });

  describe("Read/Write operations on an S3 provider", function() {
    var _provider;

    beforeEach(function() {
      _provider = new S3Provider({bucket: "<bucket_name>", key: "<S3_KEY>", secret: "<S3_SECRET>", name: randomName, keyPrefix: randomKeyPrefix });
    });

    afterEach(function(done){
      provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.clear(function(error) {
        if (error) {
          throw error;
        }
        expect(error).not.to.exist;
        done();
        });
      });
    });

    it("should allow put() and get()", function(done) {
      var data = new Uint8Array([5, 2, 5]);
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if(error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.put("key", data, function(error) {
          if(error) {
            throw error;
          }
          context.get("key", function(error, result) {
            expect(error).not.to.exist;
            expect(result).to.exist;
            expect(result).to.eql(data);
            done();
          });
        });
      });
    });

    it("should allow delete()", function(done) {
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.put("key", "value", function(error) {
          if (error) {
            throw error;
          }
          context.delete("key", function(error) {
            if (error) {
              throw error;
            }
            context.get("key", function(error, result) {
              expect(error).not.to.exist;
              expect(result).not.to.exist;
              done();
            });
          });
        });
      });
    });

    it("should allow clear()", function(done) {
      var data1 = new Uint8Array([5, 2, 5]);
      var data2 = new Uint8Array([10, 20, 50]);

      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.put("key1", data1, function(error) {
          if (error) {
            throw error;
          }
          expect(error).not.to.exist;
          context.put("key2", data2, function(error) {
            if (error) {
              throw error;
            }
            expect(error).not.to.exist;
            context.clear(function(error) {
              if (error) {
                throw error;
              }
              context.get("key1", function(error, result) {
               expect(error).to.exist;
                expect(result).not.to.exist;

                context.get("key2", function(error, result) {
                  expect(error).to.exist;
                  expect(result).not.to.exist;
                  done();
                });
              });
            });
          });
        });
      });
    });

    it("should fail when trying to write on ReadOnlyContext", function(done) {
      var data1 = new Uint8Array([5, 2, 5]);
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadOnlyContext();
        context.put("key1", data1, function(error) {
          expect(error).to.exist;
          done();
        });
      });
    });
  });

});
