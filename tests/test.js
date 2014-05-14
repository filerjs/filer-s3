var should = require("should"),
    S3Provider = require("../index.js"),
    S3;

describe("Filer.FileSystem.providers.S3", function () {

  it("should finish the initialization",function() {
    should(function() {
      S3 = new S3Provider({bucket:"<bucket_name>", key: "<S3_KEY>", secret: "<S3_SECRET>"});
    }).not.throw();
  });

  it("should allow put()", function (done) {
    S3.put("/tmp/file.js", {
        id: "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx",
        mode: "file",
        size: 0,
        data: "<p>hello world</p>"
      }, function(err, data) {
      should.not.exist(err);
      done();
    });
  });

  it("should allow put()", function (done) {
    var data = new Uint8Array(5);
    S3.put("/tmp/path/to/file.txt", data, function(err, data) {
      should.not.exist(err);
      done();
    });
  });

  it("should allow put()", function (done) {
    var data = new Uint8Array(5);
    S3.put("/tmp/path/to/file.md", data, function(err, data) {
      should.not.exist(err);
      done();
    });
  });

  it("should allow put()", function (done) {
    S3.put("/tmp/data.js", {"0":17,"1":0,"2":0,"3":0,"4":0}, function(err, data) {
      should.not.exist(err);
      done();
    });
  });

  it("should allow get()", function (done) {
    S3.get("file.txt", function(err, data) {
      should.exist(err);
      done();
    });
  });

  it("should allow delete()", function (done) {
    S3.delete("/tmp/foo.bar", function(err, data) {
      should.not.exist(err);
      done();
    });
  });

  it("should allow clear()", function (done) {
    S3.clear(function(err, data) {
      should.not.exist(err);
      done();
    });
  });

});