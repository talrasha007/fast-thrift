/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
var emptyBuf = new Buffer(0);

var InputBufferUnderrunError = exports.InputBufferUnderrunError = function() {
};

var TFramedTransport = exports.TFramedTransport = function(buffer, callback) {
  this.inBuf = buffer || emptyBuf;
  this.outBuffers = [];
  this.outCount = 0;
  this.readPos = 0;
  this.onFlush = callback;
};
TFramedTransport.receiver = function(callback) {
  var frameLeft = 0,
      framePos = 0,
      frame = null;
  var residual = null;

  return function(data) {
    // Prepend any residual data from our previous read
    if (residual) {
      data = Buffer.concat([residual, data]);
    }

    // framed transport
    while (data.length) {
      if (frameLeft === 0) {
        // TODO assumes we have all 4 bytes
        if (data.length < 4) {
          console.log("Expecting > 4 bytes, found only " + data.length);
          residual = data;
          break;
          //throw Error("Expecting > 4 bytes, found only " + data.length);
        }
        frameLeft = data.readInt32BE(0);
        frame = new Buffer(frameLeft);
        framePos = 0;
        data = data.slice(4, data.length);
      }
      
      if (data.length >= frameLeft) {
        data.copy(frame, framePos, 0, frameLeft);
        data = data.slice(frameLeft, data.length);
        
        frameLeft = 0;
        callback(new TFramedTransport(frame));
      } else if (data.length) {
        data.copy(frame, framePos, 0, data.length);
        frameLeft -= data.length;
        framePos += data.length;
        data = data.slice(data.length, data.length);
      }
    }
  };
};

TFramedTransport.prototype = {
  commitPosition: function(){},
  rollbackPosition: function(){},

  // TODO: Implement open/close support
  isOpen: function() {return true;},
  open: function() {},
  close: function() {},

  read: function(len) { // this function will be used for each frames.
    var end = this.readPos + len;

    if (this.inBuf.length < end) {
      throw new Error('read(' + len + ') failed - not enough data');
    }

    var buf = this.inBuf.slice(this.readPos, end);
    this.readPos = end;
    return buf;
  },

  readByte: function() {
    return this.inBuf.readInt8(this.readPos++);
  },

  readI16: function() {
    var i16 = this.inBuf.readInt16BE(this.readPos);
    this.readPos += 2;
    return i16;
  },

  readI32: function() {
    var i32 = this.inBuf.readInt32BE(this.readPos);
    this.readPos += 4;
    return i32;
  },

  readDouble: function() {
    var d = this.inBuf.readDoubleBE(this.readPos);
    this.readPos += 8;
    return d;
  },

  readString: function(len) {
    var str = this.inBuf.toString('utf8', this.readPos, this.readPos + len);
    this.readPos += len;
    return str;
  },

  readAll: function() {
    return this.inBuf;
  },

  write: function(buf, len) {
    if (len === undefined) len = typeof buf === 'string' ? Buffer.byteLength(buf, 'utf8') : buf.length;
    if (len <= 0) return ;

    var curBuf = this.outBuffers[this.outBuffers.length - 1];
    if (!curBuf || curBuf.length - curBuf.cur < len) {
      curBuf = new Buffer(Math.max(len, 1024));
      curBuf.cur = 0;
      this.outBuffers.push(curBuf);
    }

    if (typeof buf === 'string') curBuf.write(buf, curBuf.cur, len, 'utf8');
    else buf.copy(curBuf, curBuf.cur, 0, len);
    curBuf.cur += len;

    this.outCount += len;
  },

  flush: function() {
      if (this.onFlush) {
          var out = new Buffer(this.outCount + 4),
              pos = 4;

          out.writeInt32BE(this.outCount);
          this.outBuffers.forEach(function(buf) {
              buf.copy(out, pos, 0, buf.cur);
              pos += buf.cur;
          });

          //frameLeft = this.inBuf.readInt32BE(0);
          this.onFlush(out);
      }

      this.outBuffers = [];
      this.outCount = 0;
  }
};

var defaultReadBufferSize = 1024,
    writeBufferSize = 512;
var TBufferedTransport = exports.TBufferedTransport = function(buffer, callback) {
  //this.defaultReadBufferSize = 1024;
  //this.writeBufferSize = 512; // Soft Limit
  this.inBuf = new Buffer(defaultReadBufferSize);
  this.readCursor = 0;
  this.writeCursor = 0; // for input buffer
  this.outBuffers = new Buffer(writeBufferSize);
  this.outCount = 0;
  this.onFlush = callback;
};
TBufferedTransport.receiver = function(callback) {
  var reader = new TBufferedTransport();

  return function(data) {
    if (reader.writeCursor + data.length > reader.inBuf.length) {
      var buf = new Buffer(reader.writeCursor + data.length);
      reader.inBuf.copy(buf, 0, 0, reader.writeCursor);
      reader.inBuf = buf;
    }
    data.copy(reader.inBuf, reader.writeCursor, 0);
    reader.writeCursor += data.length;

    callback(reader);
  };
};

TBufferedTransport.prototype = {
  commitPosition: function(){
    var unreadedSize = this.writeCursor - this.readCursor;
    if (unreadedSize > 0) {
      this.inBuf.copy(this.inBuf, 0, this.readCursor, this.writeCursor);
    }
    this.readCursor = 0;
    this.writeCursor = unreadedSize;
  },
  rollbackPosition: function(){
    this.readCursor = 0;
  },

  // TODO: Implement open/close support
  isOpen: function() {return true;},
  open: function() {},
  close: function() {},

  ensureAvailable: function(len) {
    if (this.readCursor + len > this.writeCursor) {
      throw new InputBufferUnderrunError();
    }
  },

  read: function(len) {
    this.ensureAvailable(len)
    var buf = new Buffer(len);
    this.inBuf.copy(buf, 0, this.readCursor, this.readCursor + len);
    this.readCursor += len;
    return buf;
  },

  readByte: function() {
    this.ensureAvailable(1)
    return this.inBuf.readInt8(this.readCursor++);
  },

  readI16: function() {
    this.ensureAvailable(2)
    var i16 = this.inBuf.readInt16BE(this.readCursor);
    this.readCursor += 2;
    return i16;
  },

  readI32: function() {
    this.ensureAvailable(4)
    var i32 = this.inBuf.readInt32BE(this.readCursor);
    this.readCursor += 4;
    return i32;
  },

  readDouble: function() {
    this.ensureAvailable(8)
    var d = this.inBuf.readDoubleBE(this.readCursor);
    this.readCursor += 8;
    return d;
  },

  readString: function(len) {
    this.ensureAvailable(len)
    var str = this.inBuf.toString('utf8', this.readCursor, this.readCursor + len);
    this.readCursor += len;
    return str;
  },


  readAll: function() {
    if (this.readCursor >= this.writeCursor) {
      throw new InputBufferUnderrunError();
    }
    var buf = new Buffer(this.writeCursor - this.readCursor);
    this.inBuf.copy(buf, 0, this.readCursor, this.writeCursor);
    this.readCursor = this.writeCursor;
    return buf;
  },

  write: function(buf, len) {
    if (len === undefined) len = typeof buf === 'string' ? Buffer.byteLength(buf, 'utf8') : buf.length;
    if (len <= 0) return ;

    if (this.outCount + len > writeBufferSize) {
      this.flush();
      if (len >= writeBufferSize) return this.onFlush && this.onFlush(buf);
    }

    if (typeof buf === 'string') this.outBuffers.write(buf, this.outCount, len, 'utf8');
    else buf.copy(this.outBuffers, this.outCount, 0, len);

    this.outCount += len;
  },

  flush: function() {
    if (this.outCount < 1) {
      return;
    }
    
    if (this.onFlush) {
        this.onFlush(this.outBuffers.slice(0, this.outCount));
    }

    this.outCount = 0;
  }
};
