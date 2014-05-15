var should = require("should"),
    S3Provider = require("../index.js"),
    S3;

describe("Filer.FileSystem.providers.S3", function () {
  it("Check if S3 isSuppoeted", function (){
    if(!S3Provider.isSupported) {
      should.fail("Skipping Filer.FileSystem.providers.S3 tests, since S3 isn't supported.");
      return;
    }
  });

  it("has open, getReadOnlyContext, and getReadWriteContext instance methods", function() {
    S3 = new S3Provider({name: "aali", keyPrefix: "thisonefornow"});
    S3.open.should.be.type('function');
    S3.getReadOnlyContext.should.be.type('function');
    S3.getReadWriteContext.should.be.type('function');
  });

  it("should allow put() and get()", function(done) {
    var data = new Uint8Array([5, 2, 5]);
    provider = new S3Provider({name: "aali", keyPrefix: "thisonefornow"});
    provider.open({bucket:"<name>", key: "<key>", secret: "<secret>"}, function(error, firstAccess) {
      if(error) throw error;

      var context = provider.getReadWriteContext();
      context.put("key", data, function(error, result) {
        if(error) {
          throw error;
        }

        context.get("key", function(error, result) {
          should.not.exist(error);
          result.should.eql(data);
          done();
        });
      });
    });
  });

});
