/*!
 * kue - RedisClient factory
 * Copyright (c) 2013 Automattic <behradz@gmail.com>
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 * Author: behradz@gmail.com
 */

/**
 * Module dependencies.
 */

const isPlainObject = require('lodash/isPlainObject');
const mapValues = require('lodash/mapValues');
const redis = require('redis');
const { URL } = require('url');

/**
 *
 * @param options
 * @param queue
 */

exports.configureFactory = function( options, queue ) {
  // URL keywords that should not result in strings after parsing
  const keywords = {
    true: true,
    false: false,
    null: null,
    undefined,
  };

  options.prefix = options.prefix || 'q';

  if( typeof options.redis === 'string' ) {
    // parse the url
    var conn_info = new URL(options.redis);
    if( conn_info.protocol !== 'redis:' && conn_info.protocol !== 'rediss:' ) {
      throw new Error('kue connection string must use the redis: protocol');
    }

    // Parse query params (aka searchParams)
    const mappedOptions = {};
    for(const [key, value] of conn_info.searchParams.entries()) {
      const parsedValue = JSON.parse(value);
      if (isPlainObject(parsedValue)) {
        mappedOptions[key] = mapValues(parsedValue, function (v) { return (v in keywords) ? keywords[v] : v; });
      } else {
        mappedOptions[key] = (value in keywords) ? keywords[value] : value;
      }
    }

    options.redis = {
      port: conn_info.port || 6379,
      host: conn_info.hostname,
      db: (conn_info.pathname ? conn_info.pathname.substr(1) : null) || conn_info.searchParams.get('db') || 0,
      // see https://github.com/mranney/node_redis#rediscreateclient
      options: mappedOptions,
    };

    if( conn_info.password ) {
      options.redis.auth = conn_info.password
    }
  }

  options.redis = options.redis || {};

  // guarantee that redis._client has not been populated.
  // may warrant some more testing - i was running into cases where shutdown
  // would call redis.reset but an event would be emitted after the reset
  // which would re-create the client and cache it in the redis module.
  exports.reset();

  /**
   * Create a RedisClient.
   *
   * @return {RedisClient}
   * @api private
   */
  exports.createClient = function() {
    var clientFactoryMethod = options.redis.createClientFactory || exports.createClientFactory;
    var client              = clientFactoryMethod(options);

    client.on('error', function( err ) {
      queue.emit('error', err);
    });

    client.prefix           = options.prefix;

    // redefine getKey to use the configured prefix
    client.getKey = function( key ) {
      if( client.constructor.name == 'Redis'  || client.constructor.name == 'Cluster') {
        // {prefix}:jobs format is needed in using ioredis cluster to keep they keys in same node
        // otherwise multi commands fail, since they use ioredis's pipeline.
        return '{' + this.prefix + '}:' + key;
      }
      return this.prefix + ':' + key;
    };

    client.createFIFO = function( id ) {
      //Create an id for the zset to preserve FIFO order
      var idLen = '' + id.toString().length;
      var len = 2 - idLen.length;
      while (len--) idLen = '0' + idLen;
      return idLen + '|' + id;
    };

    // Parse out original ID from zid
    client.stripFIFO = function( zid ) {
      if ( typeof zid === 'string' ) {
        return +zid.substr(zid.indexOf('|')+1);
      } else {
        // Sometimes this gets called with an undefined
        // it seems to be OK to have that not resolve to an id
        return zid;
      }
    };

    return client;
  };
};

/**
 * Create a RedisClient from options
 * @param options
 * @return {RedisClient}
 * @api private
 */

exports.createClientFactory = function( options ) {
  var socket = options.redis.socket;
  var port   = !socket ? (options.redis.port || 6379) : null;
  var host   = !socket ? (options.redis.host || '127.0.0.1') : null;
  var db   = !socket ? (options.redis.db || 0) : null;
  var client = redis.createClient(socket || port, host, options.redis.options);
  if( options.redis.auth ) {
    client.auth(options.redis.auth);
  }
  if( db >= 0 ){
    client.select(db);
  }
  return client;
};

/**
 * Create or return the existing RedisClient.
 *
 * @return {RedisClient}
 * @api private
 */

exports.client = function() {
  return exports._client || (exports._client = exports.createClient());
};

/**
 * Return the pubsub-specific redis client.
 *
 * @return {RedisClient}
 * @api private
 */

exports.pubsubClient = function() {
  return exports._pubsub || (exports._pubsub = exports.createClient());
};

/**
 * Resets internal variables to initial state
 *
 * @api private
 */
exports.reset = function() {
  exports._client && exports._client.quit();
  exports._pubsub && exports._pubsub.quit();
  exports._client = null;
  exports._pubsub = null;
};
