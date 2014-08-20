var expect = require('expect.js'),
    S3Provider = require(".."),
    guid = require("../lib/utils").guid,
    randomName,
    randomKeyPrefix;

describe("Filer.FileSystem.providers.S3", function() {
  it("is supported -- if it isn't, none of these tests can run.", function() {
    expect(S3Provider.isSupported).to.be.true;
  });

  it("has open, getReadOnlyContext, and getReadWriteContext instance methods", function() {
    var S3 = new S3Provider({bucket: process.env.S3_BUCKET, key: process.env.S3_KEY, secret: process.env.S3_SECRET, name: guid(), keyPrefix: guid()});
    expect(S3.open).to.be.a('function');
    expect(S3.getReadOnlyContext).to.be.a('function');
    expect(S3.getReadWriteContext).to.be.a('function');
  });

  describe("open an S3 provider", function() {
    var _provider;

    beforeEach(function() {
      randomName = guid();
      randomKeyPrefix = guid();
      _provider = new S3Provider({bucket: process.env.S3_BUCKET, key: process.env.S3_KEY, secret: process.env.S3_SECRET, name: randomName, keyPrefix: randomKeyPrefix });
    });

    afterEach(function(done){
      var AWS = require('aws-sdk'),
      s3 = new AWS.S3({params: {Bucket: process.env.S3_BUCKET}});
      AWS.config.update({
        accessKeyId: process.env.S3_KEY,
        secretAccessKey: process.env.S3_SECRET
      });
      var options = {
        Prefix: randomKeyPrefix
      };
      getAllObjects(options, []);

      function getAllObjects(options, aggregate) {
        s3.listObjects(options, function (err, data) {
          expect(err, "[Error on listObjects]").to.not.exist;
          if(data.Contents.length === 0) {
            done();
            return;
          }
          aggregate = aggregate.concat(data.Contents.map(function (content) {
            return { Key: content.Key };
          }));
          if (data.IsTruncated) {
            options.Marker = data.Contents[data.Contents.length - 1].Key;
            getAllObjects(options, aggregate);
          }
          var params = {
            Delete: {
              Objects: aggregate
            }
          };

          s3.deleteObjects(params, function (err) {
            expect(err, "[Error on deleteObjects]").to.not.exist;
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
      _provider = new S3Provider({bucket: process.env.S3_BUCKET, key: process.env.S3_KEY, secret: process.env.S3_SECRET, name: randomName, keyPrefix: randomKeyPrefix });
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
      var data = new Buffer([5, 2, 5]);
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if(error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.putBuffer("key", data, function(error) {
          if(error) {
            throw error;
          }
          context.getBuffer("key", function(error, result) {
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
        context.putObject("key", "value", function(error) {
          if (error) {
            throw error;
          }
          context.delete("key", function(error) {
            if (error) {
              throw error;
            }
            context.getObject("key", function(error, result) {
              expect(error).not.to.exist;
              expect(result).not.to.exist;
              done();
            });
          });
        });
      });
    });

    it("should allow clear()", function(done) {
      var data1 = new Buffer([5, 2, 5]);
      var data2 = new Buffer([10, 20, 50]);

      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.putBuffer("key1", data1, function(error) {
          if (error) {
            throw error;
          }
          expect(error).not.to.exist;
          context.putBuffer("key2", data2, function(error) {
            if (error) {
              throw error;
            }
            expect(error).not.to.exist;
            context.clear(function(error) {
              if (error) {
                throw error;
              }
              context.getBuffer("key1", function(error, result) {
               expect(error).to.exist;
                expect(result).not.to.exist;

                context.getBuffer("key2", function(error, result) {
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
      var data1 = new Buffer([5, 2, 5]);
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadOnlyContext();
        context.putBuffer("key1", data1, function(error) {
          expect(error).to.exist;
          done();
        });
      });
    });
  });

});
