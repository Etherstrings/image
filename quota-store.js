const crypto = require('crypto');
const { createClient } = require('redis');
const { Redis: UpstashRedis } = require('@upstash/redis');

const KEYS_SET = process.env.KEYS_SET || 'image_keys';
const DEFAULT_FREE_QUOTA = Number.parseInt(process.env.FREE_QUOTA || '2', 10);
const FREE_QUOTA = Number.isFinite(DEFAULT_FREE_QUOTA) && DEFAULT_FREE_QUOTA >= 0 ? DEFAULT_FREE_QUOTA : 2;
const USER_KEY_PREFIX = process.env.USER_KEY_PREFIX || 'image_user:';
const USER_JOB_LIST_PREFIX = process.env.USER_JOB_LIST_PREFIX || 'image_user_jobs:';

let standardStorePromise = null;
let upstashStore = null;

function isTruthy(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function getStandardRedisOptions() {
  const redisUrl = process.env.REDIS_URL || process.env.REDIS_URI || '';
  if (redisUrl) {
    return { url: redisUrl };
  }

  const host = process.env.REDIS_HOST || '';
  if (!host) {
    return null;
  }

  const port = Number.parseInt(process.env.REDIS_PORT || '6379', 10);
  const db = Number.parseInt(process.env.REDIS_DB || '0', 10);
  const socket = {
    host,
    port: Number.isFinite(port) ? port : 6379,
  };

  if (isTruthy(process.env.REDIS_TLS)) {
    socket.tls = true;
  }

  const options = { socket };

  if (process.env.REDIS_USERNAME) {
    options.username = process.env.REDIS_USERNAME;
  }

  if (process.env.REDIS_PASSWORD) {
    options.password = process.env.REDIS_PASSWORD;
  }

  if (Number.isFinite(db) && db >= 0) {
    options.database = db;
  }

  return options;
}

async function getStandardStore() {
  if (!standardStorePromise) {
    const options = getStandardRedisOptions();
    if (!options) {
      return null;
    }

    standardStorePromise = (async () => {
      const client = createClient(options);
      client.on('error', (error) => {
        console.error('[quota-store] redis client error:', error);
      });
      await client.connect();
      return {
        mode: 'redis',
        async get(key) {
          return client.get(key);
        },
        async set(key, value, ttlSeconds) {
          if (Number.isFinite(ttlSeconds) && ttlSeconds > 0) {
            return client.set(key, value, { EX: ttlSeconds });
          }
          return client.set(key, value);
        },
        async del(key) {
          return client.del(key);
        },
        async incr(key) {
          return client.incr(key);
        },
        async sismember(key, member) {
          return client.sIsMember(key, member);
        },
        async srem(key, member) {
          return client.sRem(key, member);
        },
        async sadd(key, ...members) {
          return client.sAdd(key, members);
        },
        async lpush(key, ...values) {
          return client.lPush(key, values);
        },
        async lrange(key, start, stop) {
          return client.lRange(key, start, stop);
        },
        async ltrim(key, start, stop) {
          return client.lTrim(key, start, stop);
        },
        async scard(key) {
          return client.sCard(key);
        },
        async keys(pattern) {
          return client.keys(pattern);
        },
      };
    })().catch((error) => {
      standardStorePromise = null;
      throw error;
    });
  }

  return standardStorePromise;
}

function getUpstashStore() {
  const url = process.env.KV_REST_API_URL || '';
  const token = process.env.KV_REST_API_TOKEN || '';

  if (!url || !token) {
    return null;
  }

  if (!upstashStore) {
    const client = new UpstashRedis({ url, token });
    upstashStore = {
      mode: 'upstash',
      get(key) {
        return client.get(key);
      },
      set(key, value, ttlSeconds) {
        if (Number.isFinite(ttlSeconds) && ttlSeconds > 0) {
          return client.set(key, value, { ex: ttlSeconds });
        }
        return client.set(key, value);
      },
      del(key) {
        return client.del(key);
      },
      incr(key) {
        return client.incr(key);
      },
      sismember(key, member) {
        return client.sismember(key, member);
      },
      srem(key, member) {
        return client.srem(key, member);
      },
      sadd(key, ...members) {
        return client.sadd(key, ...members);
      },
      lpush(key, ...values) {
        return client.lpush(key, ...values);
      },
      lrange(key, start, stop) {
        return client.lrange(key, start, stop);
      },
      ltrim(key, start, stop) {
        return client.ltrim(key, start, stop);
      },
      scard(key) {
        return client.scard(key);
      },
      keys(pattern) {
        return client.keys(pattern);
      },
    };
  }

  return upstashStore;
}

async function getQuotaStore() {
  const standardStore = await getStandardStore();
  if (standardStore) {
    return standardStore;
  }

  return getUpstashStore();
}

function fingerprint(req) {
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || 'unknown';
  const ua = req.headers['user-agent'] || '';
  return 'free:' + crypto.createHash('sha256').update(`${ip}:${ua}`).digest('hex').slice(0, 16);
}

function buildUserKey(userId) {
  return `${USER_KEY_PREFIX}${userId}`;
}

function buildUserJobsKey(userId) {
  return `${USER_JOB_LIST_PREFIX}${userId}`;
}

module.exports = {
  FREE_QUOTA,
  KEYS_SET,
  USER_KEY_PREFIX,
  USER_JOB_LIST_PREFIX,
  buildUserKey,
  buildUserJobsKey,
  fingerprint,
  getQuotaStore,
};
