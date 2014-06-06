var fs = require('fs');
var path = require('path');
var expect = require('chai').expect;
var sinon = require('sinon');
var knox = require('knox');
var through = require('through');
var archiver = require('archiver');
var proxyquire = require('proxyquire');
var uploadToS3 = require('../lib/index');
var internals = uploadToS3.internals;

var fileData = {
  Location: 'location',
  Bucket: 'bucket',
  Key: 'key',
  ETag: 'etag',
  size: 'size'
};
var clientOptions = {
  key: 'key',
  secret: 'secret',
  bucket: 'bucket'
};

describe('Upload to S3', function () {
  var tmpDir = './.tmp';
  var fileNames = ['test.txt', 'test2.txt'];
  var zipFilePath = path.join(tmpDir, 'test.zip');
  var fileContent = 'test';

  beforeEach(function (done) {
    fs.mkdirSync(tmpDir)

    var output = fs.createWriteStream(zipFilePath);
    var archive = archiver('zip');

    archive.pipe(output);
    for (var i=0; i<fileNames.length; i++) {
      var filePath = path.join(tmpDir, fileNames[i]);
      fs.writeFileSync(filePath, fileContent);
      archive.append(fs.createReadStream(filePath), {name: fileNames[i]});
    }

    archive.finalize(function (err, bytes) {
      done();
    });
  });

  afterEach(function (done) {
    for (var i=0; i<fileNames.length; i++) {
      fs.unlinkSync(path.join(tmpDir, fileNames[i]));
    }

    fs.unlinkSync(zipFilePath);
    fs.rmdirSync(tmpDir);
    done();
  });

  describe('options validation', function() {
    var options;

    beforeEach(function () {
      options = {
        key: 'key',
        secret: 'secret',
        bucket: 'bucket'
      };
    });

    it('validates the key option', function () {
      delete options.key;
      var err = tryOptions(options);
      expect(err instanceof Error).to.be.ok;
    });

    it('validates the secret option', function () {
      delete options.secret;
      var err = tryOptions(options);
      expect(err instanceof Error).to.be.ok;
    });

    it('validates the bucket option', function () {
      delete options.bucket;
      var err = tryOptions(options);

      expect(err instanceof Error).to.be.ok;
    });

    it('sets the directory path option default if none is provided', function () {
      var _options = internals._validateOptions(options);
      expect(_options.path).to.equal('/');
    });

    it('validates the filter option', function() {
      options.filter = 'not a function';
      var err = tryOptions(options);
      expect(err instanceof Error).to.be.ok;
    });
  });

  it('builds the file stream object', function () {
    var file = {};
    file = internals._buildFileObject(file, fileData)
    expect(file.location).to.equal('location');
    expect(file.bucket).to.equal('bucket');
    expect(file.key).to.equal('key');
    expect(file.etag).to.equal('etag');
    expect(file.size).to.equal('size');
  });

  it('creats a knox client', function () {
    var client = internals._createClient({ key: 'key', secret: 'secret', bucket: 'bucket' });
    var knoxClient = knox.createClient({ key: 'key', secret: 'secret', bucket: 'bucket' });
    expect(client.toString()).to.eql(knoxClient.toString());
  });

  describe('#internals.unzipFiles()', function() {

    // This test is required because the unzipping module
    // emits a "finish" event, instead of "end". Want to be sure we address that.
    it('emits and "end" event', function (done) {
      internals.unzipFiles(fs.createReadStream(zipFilePath)).on('end', function () {
        done();
      });
    });

    it('only unzips types of "File" from the zip file', function (done) {
      internals.unzipFiles(fs.createReadStream(zipFilePath)).on('data', function (file) {
        expect(file.type).to.equal('File');
      }).on('end', function () {
        done();
      });
    });
  });

  it('uploads files', function (done) {
    var knoxMpuSpy = sinon.spy();
    var uploadToS3 = proxyquire('../lib/index', { 'knox-mpu': knoxMpuSpy });
    var internals = uploadToS3.internals;
    var returnStream = through();
    var dirPath = 'dirPath';
    var client = internals._createClient(clientOptions);
    var zipStream = internals.unzipFiles(fs.createReadStream(zipFilePath));
    var uploadStream = internals.uploadFiles(client, returnStream, dirPath);

    zipStream.pipe(uploadStream).on('end', function () {
      var args = knoxMpuSpy.args[0];

      expect(knoxMpuSpy.called).to.be.ok;
      expect(args[0].client.toString()).to.equal(client.toString());
      expect(args[1]).to.be.a('function');

      done();
    });
  });

  it('streams with options', function () {
    var options = clientOptions;
    var streamer = internals.streamWithOptions(options);

    expect(streamer).to.be.a('function');
  });

  describe('stream with filter option', function() {
    var options, uploadToS3, internals, knoxMpuSpy, zipStream;
    beforeEach(function() {
      options = {
        key: 'key',
        secret: 'secret',
        bucket: 'bucket'
      };

      uploadToS3 = proxyquire('../lib/index', { 'knox-mpu': sinon.spy() });
      internals = uploadToS3.internals;
      zipStream = fs.createReadStream(zipFilePath);
    });

    it('filters out file by path', function(done) {
      options.filter = through(function(file) {
        // Filter out file with name test.txt
        if (file.path !== 'test.txt')
          this.emit('data', file);
      });

      var emittedFiles = [];
      internals.unzipFiles(zipStream)
        .pipe(options.filter).on('data', function(f) {
          emittedFiles.push(f.path);
        })
        .on('end', function() {
          expect(emittedFiles).not.to.contain('test.txt');
          expect(emittedFiles).to.contain('test2.txt');
          done();
        });
    });

    it('filter renames file', function(done) {
      options.filter = through(function(file) {
        file.path = '_' + file.path;
        this.emit('data', file);
      });

      var emittedFiles = [];
      internals.unzipFiles(zipStream)
        .pipe(options.filter).on('data', function(f) {
          emittedFiles.push(f.path);
        })
        .on('end', function() {
          expect(emittedFiles).to.contain('_test.txt');
          expect(emittedFiles).to.contain('_test2.txt');
          done();
        });
    });

    // it('filter rewrites file', function(done) {
    //   options.filter = through(function(file) {
    //     var contents = '';
    //     file.on('data', function(chunk) {
    //       contents += chunk;
    //     }).on('end', function() {
    //       // Rewrite the contents of the file and stream it out
    //       this.emit()
    //       console.log("contents: " + contents);
    //     });
    //   });
    //
    //   internals.unzipFiles(zipStream)
    //     .pipe(options.filter).on('data', function(f) {
    //
    //     })
    //     .on('end', function() {
    //       // expect(emittedFiles).to.contain('_test.txt');
    //       // expect(emittedFiles).to.contain('_test2.txt');
    //       done();
    //     });
    // });
  });
});

function tryOptions (options) {
  var err;

  try{
    internals._validateOptions(options);
  }
  catch (e) {
    err = e;
  }
  finally {
    return err;
  }
}
