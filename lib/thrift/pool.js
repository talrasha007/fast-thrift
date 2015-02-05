var _ = require('lodash'),
    thrift = require('./');

exports.createPooledClient = function (cls, host, port, opt) {
    var proto = cls.Client.prototype;

    var PooledClient = proto.___co_pooled___;
    if (!PooledClient) {
        PooledClient = proto.___co_pooled___  = function (cls, host, port, opt) {
            this._class = cls;
            this._host = host;
            this._port = port;
            this._opt = opt;
            this._pool = [];
        };

        PooledClient.prototype = {
            _getClient: function () {
                if (this._pool.length > 0) return this._pool.pop();
console.log('cc');
                var connection = thrift.createConnection(this._host, this._port, this._opt);
                return thrift.createClient(this._class, connection);
            }
        };

        _.each(proto, function (fn, f) {
            if (typeof fn === 'function' && !/send_.*/.test(f) && !/recv_.*/.test(f)) {
                PooledClient.prototype[f] = function () {
                    var ctx = this;
                    var args = Array.prototype.slice.call(arguments);

                    if (typeof args[args.length - 1] === 'function') {
                        /* callback style */
                        var done = args[args.length - 1];
                        var client = ctx._getClient();

                        args[args.length - 1] = function () {
                            ctx._pool.push(client);
                            done.apply(null, arguments);
                        };

                        client[f].apply(client, args);
                    } else {
                        /* co style */
                        return function(done){
                            args.push(function(){
                                ctx._pool.push(client);
                                done.apply(null, arguments);
                            });

                            try {
                                var client = ctx._getClient();
                                client[f].apply(client, args);
                            } catch (err) {
                                done(err);
                            }
                        };
                    }
                };
            }
        });
    }

    return new PooledClient(cls, host, port, opt);
};
