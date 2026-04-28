const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const path = require('path');
const { URL } = require('url');
const {
  FREE_QUOTA,
  KEYS_SET,
  buildUserJobsKey,
  buildUserKey,
  fingerprint,
  getQuotaStore
} = require('./quota-store');

const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const CONFIG_PATH = process.env.PROVIDERS_CONFIG_PATH || path.join(__dirname, 'providers.json');
const DEFAULT_PREFERRED_PROVIDER_NAMES = ['1024token', 'custom'];
const MODEL_CAPACITY_ERROR = '当前模型资源有点紧张，请稍后再试一次。';
const DEFAULT_JOB_TTL_SECONDS = Number.parseInt(process.env.JOB_TTL_SECONDS || '3600', 10);
const JOB_TTL_SECONDS = Number.isFinite(DEFAULT_JOB_TTL_SECONDS) && DEFAULT_JOB_TTL_SECONDS > 0 ? DEFAULT_JOB_TTL_SECONDS : 3600;
const JOB_KEY_PREFIX = process.env.JOB_KEY_PREFIX || 'image_job:';
const DEFAULT_JOB_HISTORY_LIMIT = Number.parseInt(process.env.JOB_HISTORY_LIMIT || '20', 10);
const JOB_HISTORY_LIMIT = Number.isFinite(DEFAULT_JOB_HISTORY_LIMIT) && DEFAULT_JOB_HISTORY_LIMIT > 0 ? DEFAULT_JOB_HISTORY_LIMIT : 20;
const DEFAULT_ACCOUNT_CREDIT = Number.parseInt(process.env.ACCOUNT_CREDIT_PER_KEY || '1', 10);
const ACCOUNT_CREDIT_PER_KEY = Number.isFinite(DEFAULT_ACCOUNT_CREDIT) && DEFAULT_ACCOUNT_CREDIT > 0 ? DEFAULT_ACCOUNT_CREDIT : 1;
const FREE_QUOTA_CONFIG_KEY = process.env.FREE_QUOTA_CONFIG_KEY || 'image_config:free_quota';

const activeJobControllers = new Map();
const memoryJobs = new Map();

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload, null, 2);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body),
    'Cache-Control': 'no-store'
  });
  res.end(body);
}

function sendText(res, statusCode, text) {
  res.writeHead(statusCode, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Content-Length': Buffer.byteLength(text),
    'Cache-Control': 'no-store'
  });
  res.end(text);
}

function sendImage(res, buffer, extraHeaders = {}) {
  res.writeHead(200, {
    'Content-Type': 'image/png',
    'Content-Length': buffer.length,
    'Cache-Control': 'no-store',
    ...extraHeaders
  });
  res.end(buffer);
}

function readConfig() {
  const raw = fs.readFileSync(CONFIG_PATH, 'utf8');
  const parsed = JSON.parse(raw);
  parsed.roundRobinIndex = Number.isInteger(parsed.roundRobinIndex) ? parsed.roundRobinIndex : 0;
  parsed.request = parsed.request || {};
  parsed.providers = Array.isArray(parsed.providers) ? parsed.providers : [];
  return parsed;
}

function writeConfig(nextConfig) {
  const tempPath = `${CONFIG_PATH}.tmp`;
  fs.writeFileSync(tempPath, JSON.stringify(nextConfig, null, 2), 'utf8');
  fs.renameSync(tempPath, CONFIG_PATH);
}

function requireAdmin(req) {
  if (!ADMIN_KEY) return true;
  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  return req.headers['x-admin-key'] === ADMIN_KEY || parsedUrl.searchParams.get('key') === ADMIN_KEY;
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function parseJsonBody(raw) {
  if (!raw) return {};
  return JSON.parse(raw);
}

function sanitizeProvider(provider) {
  const rawUrl = String(provider.url || provider.baseUrl || '').trim();
  return {
    name: String(provider.name || '').trim(),
    url: rawUrl.replace(/\/+$/, ''),
    apiKey: String(provider.apiKey || '').trim(),
    enabled: provider.enabled !== false
  };
}

function validateProvider(provider, index) {
  if (!provider.name) throw new Error(`providers[${index}].name is required`);
  if (!provider.url) throw new Error(`providers[${index}].url or providers[${index}].baseUrl is required`);
  if (!provider.apiKey) throw new Error(`providers[${index}].apiKey is required`);
}

function rotateProviders(providers, startIndex) {
  if (providers.length === 0) return [];
  const offset = ((startIndex % providers.length) + providers.length) % providers.length;
  return providers.map((_, index) => providers[(offset + index) % providers.length]);
}

function normalizeProviderNames(values) {
  if (!Array.isArray(values)) return [];
  return uniqueStrings(values.map((value) => String(value || '').trim()).filter(Boolean));
}

function prioritizeProviders(providers, preferredProviderNames = DEFAULT_PREFERRED_PROVIDER_NAMES) {
  const remaining = [...providers];
  const prioritized = [];

  for (const name of normalizeProviderNames(preferredProviderNames)) {
    const index = remaining.findIndex((provider) => provider.name === name);
    if (index < 0) continue;
    prioritized.push(remaining[index]);
    remaining.splice(index, 1);
  }

  return prioritized.concat(remaining);
}

function summarizeAttemptError(error) {
  if (!error) return null;
  if (typeof error === 'string') return error.slice(0, 200);
  if (error.error && typeof error.error === 'string') return error.error.slice(0, 200);
  if (error.body && typeof error.body === 'string') return error.body.slice(0, 200);
  if (error.sse && error.sse.error) return JSON.stringify(error.sse.error).slice(0, 200);
  return JSON.stringify(error).slice(0, 200);
}

function readInteger(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getProviderKey(provider) {
  return `${provider.name}::${provider.url}`;
}

function isRetryableStatus(status) {
  if (!Number.isFinite(status)) return false;
  return status === 408 || status === 409 || status === 425 || status === 429 || status >= 500;
}

function isRetryableAttemptFailure(result) {
  if (!result || result.ok) return false;
  if (Number.isFinite(result.status)) {
    return isRetryableStatus(result.status);
  }

  const summary = String(summarizeAttemptError(result.error || result.body || result.sse || '') || '').toLowerCase();
  if (!summary) {
    return false;
  }

  if (
    summary.includes('invalid_api_key') ||
    summary.includes('incorrect api key') ||
    summary.includes('unauthorized') ||
    summary.includes('forbidden') ||
    summary.includes('not found') ||
    summary.includes('unsupported') ||
    summary.includes('invalid model') ||
    summary.includes('insufficient credits')
  ) {
    return false;
  }

  return (
    summary.includes('timed out') ||
    summary.includes('timeout') ||
    summary.includes('rate limit') ||
    summary.includes('capacity') ||
    summary.includes('overloaded') ||
    summary.includes('temporar') ||
    summary.includes('unavailable') ||
    summary.includes('fetch failed') ||
    summary.includes('network') ||
    summary.includes('connection') ||
    summary.includes('socket') ||
    summary.includes('econn') ||
    summary.includes('aborted')
  );
}

function buildGenerationFailure(result) {
  if (result.canceled) {
    return {
      statusCode: 499,
      error: '生成任务已取消',
      errorCode: 'canceled',
      retryable: true
    };
  }

  if (result.error === 'All providers failed') {
    return {
      statusCode: 503,
      error: MODEL_CAPACITY_ERROR,
      errorCode: 'model_capacity',
      retryable: true
    };
  }

  if (result.error !== 'All providers failed') {
    return {
      statusCode: result.statusCode || 500,
      error: result.error,
      errorCode: 'generate_failed',
      retryable: false
    };
  }
}

function isAbortError(error) {
  if (!error) return false;
  if (error.name === 'AbortError') return true;
  return String(error.message || error).toLowerCase().includes('abort');
}

function createJobId() {
  if (typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return crypto.randomBytes(16).toString('hex');
}

function createUserId() {
  if (typeof crypto.randomUUID === 'function') {
    return `u_${crypto.randomUUID()}`;
  }

  return `u_${crypto.randomBytes(16).toString('hex')}`;
}

function buildJobStorageKey(jobId) {
  return `${JOB_KEY_PREFIX}${jobId}`;
}

function normalizeUserId(value) {
  const userId = String(value || '').trim();
  return /^u_[a-z0-9-]{8,}$/i.test(userId) ? userId : '';
}

function normalizeProfileName(value) {
  return String(value || '').trim().slice(0, 40);
}

function buildGuestUsageKey(userId, guestKey) {
  if (userId) {
    return `free_user:${userId}`;
  }

  return guestKey;
}

async function getEffectiveFreeQuota(store) {
  if (!store) {
    return FREE_QUOTA;
  }

  const raw = await store.get(FREE_QUOTA_CONFIG_KEY);
  const configured = Number.parseInt(String(raw || ''), 10);
  return Number.isFinite(configured) && configured >= 0 ? configured : FREE_QUOTA;
}

async function setEffectiveFreeQuota(store, freeQuota) {
  const nextQuota = Number.parseInt(String(freeQuota ?? ''), 10);
  if (!Number.isFinite(nextQuota) || nextQuota < 0 || nextQuota > 100000) {
    throw new Error('freeQuota must be an integer between 0 and 100000');
  }

  if (!store) {
    return nextQuota;
  }

  await store.set(FREE_QUOTA_CONFIG_KEY, String(nextQuota));
  return nextQuota;
}

async function listFreeUsageKeys(store) {
  if (!store || typeof store.keys !== 'function') {
    return [];
  }

  const [guestKeys, userKeys] = await Promise.all([
    store.keys('free:*'),
    store.keys('free_user:*'),
  ]);

  return uniqueStrings([...(guestKeys || []), ...(userKeys || [])]);
}

async function resetFreeUsage(store) {
  const keys = await listFreeUsageKeys(store);
  for (const key of keys) {
    await store.del(key);
  }
  return keys.length;
}

async function migrateGuestUsageToUser(store, guestKey, userId) {
  if (!store || !guestKey || !userId) {
    return 0;
  }

  const guestUsageKey = buildGuestUsageKey('', guestKey);
  const userUsageKey = buildGuestUsageKey(userId, guestKey);
  const guestUsed = Number((await store.get(guestUsageKey)) || 0);
  const userUsed = Number((await store.get(userUsageKey)) || 0);
  const mergedUsed = Math.max(guestUsed, userUsed);

  if (mergedUsed > 0) {
    await store.set(userUsageKey, String(mergedUsed), JOB_TTL_SECONDS * 24);
  }

  return mergedUsed;
}

async function saveUser(store, user) {
  if (!store) {
    return user;
  }

  await store.set(buildUserKey(user.id), JSON.stringify(user), JOB_TTL_SECONDS * 24);
  return user;
}

async function readUser(store, userId) {
  if (!store || !userId) {
    return null;
  }

  const raw = await store.get(buildUserKey(userId));
  if (!raw) {
    return null;
  }

  if (typeof raw === 'string') {
    return JSON.parse(raw);
  }

  return raw;
}

async function ensureUser(store, userId) {
  const normalized = normalizeUserId(userId);
  if (normalized) {
    const existing = await readUser(store, normalized);
    if (existing) {
      return existing;
    }
  }

  const now = Date.now();
  const nextUser = {
    id: normalized || createUserId(),
    role: 'guest',
    credits: 0,
    createdAt: now,
    updatedAt: now,
    profileName: null,
  };

  await saveUser(store, nextUser);
  return nextUser;
}

async function updateUser(store, userId, updater) {
  const current = await readUser(store, userId);
  if (!current) {
    return null;
  }

  const next = typeof updater === 'function' ? updater(current) : { ...current, ...updater };
  if (!next) {
    return null;
  }

  next.updatedAt = Date.now();
  await saveUser(store, next);
  return next;
}

async function appendUserJob(store, userId, jobId) {
  if (!store || !userId || !jobId || typeof store.lpush !== 'function') {
    return;
  }

  const key = buildUserJobsKey(userId);
  await store.lpush(key, jobId);
  if (typeof store.ltrim === 'function') {
    await store.ltrim(key, 0, JOB_HISTORY_LIMIT - 1);
  }
}

async function listUserJobs(store, userId, limit = JOB_HISTORY_LIMIT) {
  if (!store || !userId || typeof store.lrange !== 'function') {
    return [];
  }

  const key = buildUserJobsKey(userId);
  const jobIds = await store.lrange(key, 0, Math.max(0, limit - 1));
  return Array.isArray(jobIds) ? jobIds : [];
}

async function resolveAccessContext(req) {
  const store = await getQuotaStore();
  const guestKey = fingerprint(req);
  const userId = normalizeUserId(req.headers['x-user-id']);
  let user = null;

  if (userId) {
    user = await readUser(store, userId);
  }

  return {
    store,
    guestKey,
    user,
    userId: user ? user.id : '',
  };
}

function isTerminalJobStatus(status) {
  return status === 'succeeded' || status === 'failed' || status === 'canceled';
}

function parseStoredJob(raw) {
  if (!raw) return null;
  if (typeof raw === 'string') {
    return JSON.parse(raw);
  }
  return raw;
}

function readMemoryJob(jobId) {
  const key = buildJobStorageKey(jobId);
  const cached = memoryJobs.get(key);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    memoryJobs.delete(key);
    return null;
  }
  return parseStoredJob(cached.raw);
}

async function persistJob(job) {
  const key = buildJobStorageKey(job.id);
  const raw = JSON.stringify(job);
  const store = await getQuotaStore();

  if (store && typeof store.set === 'function') {
    await store.set(key, raw, JOB_TTL_SECONDS);
    return job;
  }

  memoryJobs.set(key, {
    raw,
    expiresAt: Date.now() + JOB_TTL_SECONDS * 1000
  });
  return job;
}

async function readJob(jobId) {
  const key = buildJobStorageKey(jobId);
  const store = await getQuotaStore();

  if (store && typeof store.get === 'function') {
    const raw = await store.get(key);
    return parseStoredJob(raw);
  }

  return readMemoryJob(jobId);
}

async function updateJob(jobId, updater) {
  const current = await readJob(jobId);
  if (!current) return null;

  const next = typeof updater === 'function' ? updater(current) : { ...current, ...updater };
  if (!next) return null;

  next.updatedAt = Date.now();
  await persistJob(next);
  return next;
}

function summarizeAttempts(attempts = []) {
  return attempts.map((attempt) => ({
    provider: attempt.provider,
    status: attempt.status,
    round: attempt.round,
    retry: attempt.retry,
    retryable: attempt.retryable !== false,
    error: summarizeAttemptError(attempt.error)
  }));
}

function buildJobResponse(job) {
  const response = {
    ok: true,
    jobId: job.id,
    status: job.status,
    prompt: job.prompt,
    providerName: job.providerName || null,
    error: job.error || null,
    errorCode: job.errorCode || null,
    retryable: job.retryable === true,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    completedAt: job.completedAt || null
  };

  if (job.status === 'succeeded' && job.imageBase64) {
    response.imageUrl = `/api/generate/jobs/${encodeURIComponent(job.id)}/image`;
  }

  return response;
}

function getJobImageBuffer(job) {
  if (!job || job.status !== 'succeeded' || !job.imageBase64) {
    return null;
  }

  return Buffer.from(job.imageBase64, 'base64');
}

function createClientAbortSignal(req, res) {
  const controller = new AbortController();
  const abort = () => {
    if (!controller.signal.aborted) {
      controller.abort();
    }
  };
  const onClose = () => {
    if (!res.writableEnded) {
      abort();
    }
  };

  req.on('aborted', abort);
  req.on('error', abort);
  res.on('close', onClose);

  return {
    signal: controller.signal,
    cleanup() {
      req.off('aborted', abort);
      req.off('error', abort);
      res.off('close', onClose);
    }
  };
}

async function readSseResult(response) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const result = {
    responseId: null,
    createdTool: null,
    finalCall: null,
    outputText: '',
    error: null
  };

  function captureOutputItem(item) {
    if (!item || typeof item !== 'object') return;

    if (item.type === 'image_generation_call') {
      result.finalCall = item;
      return;
    }

    if (item.type === 'message' && Array.isArray(item.content)) {
      for (const part of item.content) {
        if (part.type === 'output_text' && part.text) {
          result.outputText += part.text;
        }
      }
    }
  }

  function handleEvent(obj) {
    if (obj.response && obj.response.id) {
      result.responseId = obj.response.id;
    }

    if (
      (obj.type === 'response.created' || obj.type === 'response.in_progress') &&
      obj.response &&
      Array.isArray(obj.response.tools) &&
      obj.response.tools[0] &&
      !result.createdTool
    ) {
      result.createdTool = obj.response.tools[0];
    }

    if (obj.type === 'response.output_text.delta' && obj.delta) {
      result.outputText += obj.delta;
    }

    if (obj.type === 'response.output_item.done' && obj.item) {
      captureOutputItem(obj.item);
    }

    if (
      (obj.type === 'response.completed' || obj.type === 'response.incomplete') &&
      obj.response &&
      Array.isArray(obj.response.output)
    ) {
      for (const item of obj.response.output) {
        captureOutputItem(item);
      }
    }

    if (obj.type === 'error' && obj.error) {
      result.error = obj.error;
    }

    if (obj.type === 'response.failed' && obj.response && obj.response.error && !result.error) {
      result.error = obj.response.error;
    }
  }

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    let splitIndex;
    while ((splitIndex = buffer.indexOf('\n\n')) >= 0) {
      const block = buffer.slice(0, splitIndex);
      buffer = buffer.slice(splitIndex + 2);
      const lines = block.split(/\r?\n/);
      const dataLines = [];

      for (const line of lines) {
        if (line.startsWith('data:')) {
          dataLines.push(line.slice(5).trim());
        }
      }

      const dataText = dataLines.join('\n');
      if (!dataText || dataText === '[DONE]') continue;

      try {
        handleEvent(JSON.parse(dataText));
      } catch {
        // Ignore malformed intermediary chunks.
      }
    }
  }

  return result;
}

function uniqueStrings(values) {
  const seen = new Set();
  const list = [];
  for (const value of values) {
    if (!value || seen.has(value)) continue;
    seen.add(value);
    list.push(value);
  }
  return list;
}

function delay(ms) {
  if (!ms || ms <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildResponseEndpoints(provider) {
  const raw = String(provider.url || provider.baseUrl || '').trim();
  const normalized = raw.replace(/\/+$/, '');
  if (!normalized) return [];

  const candidates = [];

  if (/\/responses$/i.test(normalized)) {
    candidates.push(normalized);
  } else {
    candidates.push(`${normalized}/responses`);
  }

  candidates.push(normalized.replace(/\/openai\/v1\/responses$/i, '/v1/responses'));
  candidates.push(normalized.replace(/\/openai\/v1$/i, '/v1/responses'));
  candidates.push(normalized.replace(/\/v1$/i, '/v1/responses'));

  return uniqueStrings(candidates);
}

async function tryProvider(provider, prompt, requestConfig, options = {}) {
  const inputImage = typeof options.inputImage === 'string' && options.inputImage.startsWith('data:image/')
    ? options.inputImage
    : null;
  const payload = {
    model: requestConfig.model || 'gpt-5.4',
    input: inputImage
      ? [
          {
            role: 'user',
            content: [
              { type: 'input_text', text: prompt },
              { type: 'input_image', image_url: inputImage }
            ]
          }
        ]
      : prompt,
    tools: [
      {
        type: 'image_generation',
        model: requestConfig.imageModel || 'gpt-image-2',
        size: requestConfig.size || '1024x1536',
        quality: requestConfig.quality || 'high',
        output_format: requestConfig.outputFormat || 'png'
      }
    ],
    tool_choice: { type: 'image_generation' },
    stream: true
  };

  const endpoints = buildResponseEndpoints(provider);
  let lastFailure = null;
  const requestSignal = options.signal;

  if (requestSignal?.aborted) {
    return {
      ok: false,
      provider: provider.name,
      endpoint: endpoints[0] || '',
      status: null,
      contentType: null,
      error: 'Request aborted',
      retryable: false,
      canceled: true
    };
  }

  for (const endpoint of endpoints) {
    if (requestSignal?.aborted) {
      return {
        ok: false,
        provider: provider.name,
        endpoint,
        status: null,
        contentType: null,
        error: 'Request aborted',
        retryable: false,
        canceled: true
      };
    }

    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${provider.apiKey}`,
          'Content-Type': 'application/json',
          Accept: 'text/event-stream'
        },
        body: JSON.stringify(payload),
        signal: requestSignal
      });

      const contentType = response.headers.get('content-type') || '';

      if (!response.ok) {
        lastFailure = {
          ok: false,
          provider: provider.name,
          endpoint,
          status: response.status,
          contentType,
          body: (await response.text()).slice(0, 2000),
          retryable: isRetryableStatus(response.status)
        };
        continue;
      }

      if (!contentType.includes('text/event-stream')) {
        lastFailure = {
          ok: false,
          provider: provider.name,
          endpoint,
          status: response.status,
          contentType,
          body: (await response.text()).slice(0, 2000),
          retryable: false
        };
        continue;
      }

      const sse = await readSseResult(response);
      const imageBase64 = sse.finalCall && sse.finalCall.result;

      if (!imageBase64) {
        lastFailure = {
          ok: false,
          provider: provider.name,
          endpoint,
          status: response.status,
          contentType,
          sse: {
            responseId: sse.responseId,
            createdTool: sse.createdTool,
            finalCall: sse.finalCall
              ? {
                  type: sse.finalCall.type,
                  quality: sse.finalCall.quality,
                  size: sse.finalCall.size,
                  output_format: sse.finalCall.output_format,
                  revised_prompt: sse.finalCall.revised_prompt || null
                }
              : null,
            outputText: sse.outputText || '',
            error: sse.error
          },
          retryable: true
        };
        continue;
      }

      return {
        ok: true,
        provider: provider.name,
        endpoint,
        imageBuffer: Buffer.from(imageBase64, 'base64'),
        meta: {
          responseId: sse.responseId,
          createdTool: sse.createdTool,
          finalCall: {
            type: sse.finalCall.type,
            quality: sse.finalCall.quality,
            size: sse.finalCall.size,
            output_format: sse.finalCall.output_format,
            revised_prompt: sse.finalCall.revised_prompt || null
          }
        }
      };
    } catch (error) {
      if (requestSignal?.aborted || isAbortError(error)) {
        return {
          ok: false,
          provider: provider.name,
          endpoint,
          status: null,
          contentType: null,
          error: 'Request aborted',
          retryable: false,
          canceled: true
        };
      }

      lastFailure = {
        ok: false,
        provider: provider.name,
        endpoint,
        status: null,
        contentType: null,
        error: String(error),
        retryable: true
      };
      continue;
    }
  }

  return lastFailure || {
    ok: false,
    provider: provider.name,
    endpoint: endpoints[0] || '',
    status: null,
    contentType: null,
    body: 'No valid endpoint candidates',
    retryable: false
  };
}

async function generateImageWithFallback(prompt, options = {}) {
  const config = readConfig();
  const enabledProviders = config.providers.filter((provider) => provider.enabled !== false);
  const requestSignal = options.signal;
  if (enabledProviders.length === 0) {
    return {
      ok: false,
      statusCode: 500,
      error: 'No enabled providers configured',
      attempts: []
    };
  }

  const preferredProviderNames =
    normalizeProviderNames(options.preferredProviders).length > 0
      ? normalizeProviderNames(options.preferredProviders)
      : DEFAULT_PREFERRED_PROVIDER_NAMES;
  const orderedProviders = prioritizeProviders(
    rotateProviders(enabledProviders, config.roundRobinIndex),
    preferredProviderNames
  );
  const attempts = [];
  const retriesPerProvider = Math.max(3, readInteger(config.request.retriesPerProvider, 1));
  const retryRounds = Math.max(1, readInteger(config.request.retryRounds, 1));
  const retryDelayMs = Math.max(0, readInteger(config.request.retryDelayMs, 0));
  const exhaustedProviders = new Set();

  for (let round = 0; round < retryRounds; round += 1) {
    if (requestSignal?.aborted) {
      return {
        ok: false,
        statusCode: 499,
        error: 'Generation canceled',
        attempts,
        canceled: true
      };
    }

    for (let providerIndex = 0; providerIndex < orderedProviders.length; providerIndex += 1) {
      const provider = orderedProviders[providerIndex];
      const providerKey = getProviderKey(provider);
      if (exhaustedProviders.has(providerKey)) {
        continue;
      }

      for (let retry = 0; retry < retriesPerProvider; retry += 1) {
        if (requestSignal?.aborted) {
          return {
            ok: false,
            statusCode: 499,
            error: 'Generation canceled',
            attempts,
            canceled: true
          };
        }

        const result = await tryProvider(provider, prompt, config.request, {
          inputImage: options.inputImage || null,
          signal: requestSignal
        });
        attempts.push({
          provider: result.provider,
          endpoint: result.endpoint,
          ok: result.ok,
          status: result.status || null,
          round: round + 1,
          retry: retry + 1,
          retryable: result.retryable !== false,
          meta: result.meta || null,
          error: result.error || result.body || result.sse || null
        });

        if (result.canceled) {
          return {
            ok: false,
            statusCode: 499,
            error: 'Generation canceled',
            attempts,
            canceled: true
          };
        }

        if (result.ok) {
          const providerPosition = enabledProviders.findIndex((item) => item.name === provider.name && item.url === provider.url);
          if (providerPosition >= 0) {
            config.roundRobinIndex = (providerPosition + 1) % enabledProviders.length;
            writeConfig(config);
          }
          return {
            ok: true,
            buffer: result.imageBuffer,
            provider: provider.name,
            attempts,
            meta: result.meta
          };
        }

        if (result.retryable === false) {
          exhaustedProviders.add(providerKey);
          break;
        }

        if (retry < retriesPerProvider - 1 && retryDelayMs > 0) {
          await delay(retryDelayMs);
        }
      }
    }
  }

  return {
    ok: false,
    statusCode: 502,
    error: 'All providers failed',
    attempts
  };
}

async function handleGenerate(req, res) {
  const requestAbort = createClientAbortSignal(req, res);

  try {
    const raw = await readRequestBody(req);
    const body = parseJsonBody(raw);
    const prompt = String(body.prompt || '').trim();
    const preferredProviders = normalizeProviderNames(body.preferredProviders);

    if (!prompt) {
      sendJson(res, 400, { ok: false, error: 'prompt is required' });
      return;
    }

    const result = await generateImageWithFallback(prompt, {
      preferredProviders,
      signal: requestAbort.signal
    });

    if (requestAbort.signal.aborted || res.writableEnded || res.destroyed || result.canceled) {
      return;
    }

    if (!result.ok) {
      const failure = buildGenerationFailure(result);
      console.error('[generate] request failed', JSON.stringify({
        error: result.error,
        attempts: result.attempts.map((attempt) => ({
          provider: attempt.provider,
          status: attempt.status,
          round: attempt.round,
          retry: attempt.retry,
          error: summarizeAttemptError(attempt.error)
        }))
      }));

      sendJson(res, failure.statusCode, {
        ok: false,
        error: failure.error,
        errorCode: failure.errorCode,
        retryable: failure.retryable,
        attempts: result.attempts
      });
      return;
    }

    sendImage(res, result.buffer, {
      'X-Provider-Name': result.provider,
      'X-Response-Id': result.meta.responseId || '',
      'X-Image-Quality': (result.meta.finalCall && result.meta.finalCall.quality) || '',
      'X-Image-Size': (result.meta.finalCall && result.meta.finalCall.size) || ''
    });
  } catch (error) {
    if (requestAbort.signal.aborted || res.writableEnded || res.destroyed) {
      return;
    }

    sendJson(res, 500, {
      ok: false,
      error: String(error)
    });
  } finally {
    requestAbort.cleanup();
  }
}

async function createGenerateJob(prompt, preferredProviders, options = {}) {
  const now = Date.now();
  const job = {
    id: createJobId(),
    prompt,
    preferredProviders,
    userId: options.userId || null,
    inputImage: options.inputImage || null,
    mode: options.inputImage ? 'image' : 'text',
    status: 'queued',
    createdAt: now,
    updatedAt: now,
    retryable: false
  };

  await persistJob(job);

   if (options.userId) {
    await appendUserJob(options.store, options.userId, job.id);
  }

  return job;
}

async function cancelGenerateJob(jobId, reason = '生成任务已取消') {
  const job = await readJob(jobId);
  if (!job) {
    return null;
  }

  if (isTerminalJobStatus(job.status)) {
    return job;
  }

  const controller = activeJobControllers.get(jobId);
  if (controller && !controller.signal.aborted) {
    controller.abort();
  }

  return updateJob(jobId, (current) => {
    if (!current || isTerminalJobStatus(current.status)) {
      return current;
    }

    return {
      ...current,
      status: 'canceled',
      error: reason,
      errorCode: 'canceled',
      retryable: true,
      completedAt: Date.now()
    };
  });
}

async function runGenerateJob(jobId) {
  const currentJob = await readJob(jobId);
  if (!currentJob || isTerminalJobStatus(currentJob.status)) {
    return currentJob;
  }

  const controller = new AbortController();
  activeJobControllers.set(jobId, controller);

  await updateJob(jobId, (current) => {
    if (!current || isTerminalJobStatus(current.status)) {
      return current;
    }

    return {
      ...current,
      status: 'running',
      startedAt: current.startedAt || Date.now(),
      error: null,
      errorCode: null,
      retryable: false
    };
  });

  try {
    const job = await readJob(jobId);
    if (!job || job.status === 'canceled') {
      return job;
    }

    const result = await generateImageWithFallback(job.prompt, {
      preferredProviders: job.preferredProviders,
      inputImage: job.inputImage || null,
      signal: controller.signal
    });

    if (controller.signal.aborted || result.canceled) {
      return updateJob(jobId, (current) => {
        if (!current || isTerminalJobStatus(current.status)) {
          return current;
        }

        return {
          ...current,
          status: 'canceled',
          error: '生成任务已取消',
          errorCode: 'canceled',
          retryable: true,
          completedAt: Date.now()
        };
      });
    }

    if (!result.ok) {
      const failure = buildGenerationFailure(result);
      console.error('[generate-job] request failed', JSON.stringify({
        jobId,
        error: result.error,
        attempts: summarizeAttempts(result.attempts)
      }));

      return updateJob(jobId, (current) => {
        if (!current || isTerminalJobStatus(current.status)) {
          return current;
        }

        return {
          ...current,
          status: 'failed',
          error: failure.error,
          errorCode: failure.errorCode,
          retryable: failure.retryable,
          attempts: summarizeAttempts(result.attempts),
          completedAt: Date.now()
        };
      });
    }

    return updateJob(jobId, (current) => {
      if (!current || isTerminalJobStatus(current.status)) {
        return current;
      }

      return {
        ...current,
        status: 'succeeded',
        providerName: result.provider,
        mimeType: 'image/png',
        imageBase64: result.buffer.toString('base64'),
        attempts: summarizeAttempts(result.attempts),
        retryable: false,
        completedAt: Date.now(),
        error: null,
        errorCode: null
      };
    });
  } catch (error) {
    if (controller.signal.aborted || isAbortError(error)) {
      return updateJob(jobId, (current) => {
        if (!current || isTerminalJobStatus(current.status)) {
          return current;
        }

        return {
          ...current,
          status: 'canceled',
          error: '生成任务已取消',
          errorCode: 'canceled',
          retryable: true,
          completedAt: Date.now()
        };
      });
    }

    console.error('[generate-job] unexpected failure', jobId, error);
    return updateJob(jobId, (current) => {
      if (!current || isTerminalJobStatus(current.status)) {
        return current;
      }

      return {
        ...current,
        status: 'failed',
        error: '生成服务暂时不可用，请稍后再试',
        errorCode: 'generate_failed',
        retryable: true,
        completedAt: Date.now()
      };
    });
  } finally {
    activeJobControllers.delete(jobId);
  }
}

async function handleCreateGenerateJob(req, res) {
  try {
    const raw = await readRequestBody(req);
    const body = parseJsonBody(raw);
    const context = await resolveAccessContext(req);
    const prompt = String(body.prompt || '').trim();
    const preferredProviders = normalizeProviderNames(body.preferredProviders);
    const inputImage = typeof body.image === 'string' && body.image.startsWith('data:image/') ? body.image : null;

    if (!prompt) {
      sendJson(res, 400, { ok: false, error: 'prompt is required' });
      return;
    }

    if (inputImage && !context.user) {
      sendJson(res, 403, { ok: false, error: '图生图需要先创建账号', errorCode: 'account_required_for_image' });
      return;
    }

    const job = await createGenerateJob(prompt, preferredProviders, {
      inputImage,
      userId: context.userId,
      store: context.store,
    });
    runGenerateJob(job.id).catch((error) => {
      console.error('[generate-job] unhandled failure', job.id, error);
    });

    sendJson(res, 202, {
      ok: true,
      jobId: job.id,
      status: job.status,
      createdAt: job.createdAt
    });
  } catch (error) {
    sendJson(res, 500, {
      ok: false,
      error: String(error)
    });
  }
}

async function handleGetGenerateJob(req, res, jobId) {
  try {
    const job = await readJob(jobId);
    if (!job) {
      sendJson(res, 404, { ok: false, error: 'job_not_found' });
      return;
    }

    const context = await resolveAccessContext(req);
    if (job.userId && job.userId !== context.userId) {
      sendJson(res, 403, { ok: false, error: 'forbidden' });
      return;
    }

    sendJson(res, 200, buildJobResponse(job));
  } catch (error) {
    sendJson(res, 500, {
      ok: false,
      error: String(error)
    });
  }
}

async function handleGetGenerateJobImage(req, res, jobId) {
  try {
    const job = await readJob(jobId);
    if (!job) {
      sendJson(res, 404, { ok: false, error: 'job_not_found' });
      return;
    }

    const context = await resolveAccessContext(req);
    if (job.userId && job.userId !== context.userId) {
      sendJson(res, 403, { ok: false, error: 'forbidden' });
      return;
    }

    const imageBuffer = getJobImageBuffer(job);
    if (!imageBuffer) {
      sendJson(res, 404, { ok: false, error: 'image_not_ready' });
      return;
    }

    sendImage(res, imageBuffer, {
      'Content-Type': job.mimeType || 'image/png',
      'Content-Disposition': `inline; filename="${job.id}.png"`
    });
  } catch (error) {
    sendJson(res, 500, {
      ok: false,
      error: String(error)
    });
  }
}

async function handleCancelGenerateJob(req, res, jobId) {
  try {
    const context = await resolveAccessContext(req);
    const current = await readJob(jobId);
    if (current && current.userId && current.userId !== context.userId) {
      sendJson(res, 403, { ok: false, error: 'forbidden' });
      return;
    }

    const job = await cancelGenerateJob(jobId);
    if (!job) {
      sendJson(res, 404, { ok: false, error: 'job_not_found' });
      return;
    }

    sendJson(res, 200, buildJobResponse(job));
  } catch (error) {
    sendJson(res, 500, {
      ok: false,
      error: String(error)
    });
  }
}

async function handleSession(req, res) {
  try {
    const raw = await readRequestBody(req);
    const body = parseJsonBody(raw);
    const store = await getQuotaStore();
    const user = await ensureUser(store, body.userId);
    sendJson(res, 200, {
      ok: true,
      user: {
        id: user.id,
        role: user.role,
        credits: user.credits || 0,
        profileName: user.profileName || null,
      }
    });
  } catch (error) {
    sendJson(res, 500, { ok: false, error: String(error) });
  }
}

async function handleRegister(req, res) {
  try {
    const raw = await readRequestBody(req);
    const body = parseJsonBody(raw);
    const store = await getQuotaStore();
    const user = await ensureUser(store, body.userId);
    const profileName = normalizeProfileName(body.profileName || body.name || '');

    if (!profileName) {
      sendJson(res, 400, { ok: false, error: '请输入账号名称' });
      return;
    }

    const nextUser = await updateUser(store, user.id, (current) => ({
      ...current,
      role: 'account',
      profileName,
    }));

    await migrateGuestUsageToUser(store, fingerprint(req), nextUser.id);

    sendJson(res, 200, {
      ok: true,
      user: {
        id: nextUser.id,
        role: nextUser.role,
        credits: nextUser.credits || 0,
        profileName: nextUser.profileName || null,
      }
    });
  } catch (error) {
    sendJson(res, 500, { ok: false, error: String(error) });
  }
}

async function handleUserJobs(req, res, userId) {
  try {
    const context = await resolveAccessContext(req);
    if (!context.user || context.user.id !== userId) {
      sendJson(res, 403, { ok: false, error: 'forbidden' });
      return;
    }

    const jobIds = await listUserJobs(context.store, userId);
    const jobs = [];
    for (const jobId of jobIds) {
      const job = await readJob(jobId);
      if (!job) continue;
      jobs.push(buildJobResponse(job));
    }

    sendJson(res, 200, { ok: true, jobs });
  } catch (error) {
    sendJson(res, 500, { ok: false, error: String(error) });
  }
}

async function handleDashboardStats(req, res) {
  try {
    if (!requireAdmin(req)) {
      sendJson(res, 401, { ok: false, error: 'unauthorized' });
      return;
    }

    const store = await getQuotaStore();
    if (!store || typeof store.keys !== 'function') {
      sendJson(res, 200, { ok: true, totals: {}, recentJobs: [] });
      return;
    }

    const [userKeys, jobKeys] = await Promise.all([
      store.keys(`${buildUserKey('')}*`),
      store.keys(`${JOB_KEY_PREFIX}*`),
    ]);
    const [freeQuota, freeUsageKeys] = await Promise.all([
      getEffectiveFreeQuota(store),
      listFreeUsageKeys(store),
    ]);

    const users = [];
    for (const key of userKeys || []) {
      const raw = await store.get(key);
      if (!raw) continue;
      users.push(typeof raw === 'string' ? JSON.parse(raw) : raw);
    }

    const jobs = [];
    for (const key of jobKeys || []) {
      const raw = await store.get(key);
      const job = parseStoredJob(raw);
      if (job) jobs.push(job);
    }

    jobs.sort((left, right) => Number(right.createdAt || 0) - Number(left.createdAt || 0));
    const now = Date.now();
    const recent24hJobs = jobs.filter((job) => now - Number(job.createdAt || 0) <= 24 * 60 * 60 * 1000);

    sendJson(res, 200, {
      ok: true,
      totals: {
        users: users.length,
        accounts: users.filter((user) => user.role === 'account').length,
        guests: users.filter((user) => user.role !== 'account').length,
        jobs: jobs.length,
        recent24h: recent24hJobs.length,
        succeeded: jobs.filter((job) => job.status === 'succeeded').length,
        failed: jobs.filter((job) => job.status === 'failed').length,
        running: jobs.filter((job) => job.status === 'running').length,
        queued: jobs.filter((job) => job.status === 'queued').length,
        canceled: jobs.filter((job) => job.status === 'canceled').length,
      },
      quota: {
        freeQuota,
        freeUsageUsers: freeUsageKeys.length,
      },
      recentJobs: jobs.slice(0, 20).map((job) => ({
        id: job.id,
        prompt: String(job.prompt || '').slice(0, 80),
        status: job.status,
        mode: job.mode || 'text',
        providerName: job.providerName || null,
        error: job.error || null,
        createdAt: job.createdAt,
        completedAt: job.completedAt || null,
        userId: job.userId || null,
      })),
    });
  } catch (error) {
    sendJson(res, 500, { ok: false, error: String(error) });
  }
}

async function handleDashboardAction(req, res) {
  try {
    if (!requireAdmin(req)) {
      sendJson(res, 401, { ok: false, error: 'unauthorized' });
      return;
    }

    const raw = await readRequestBody(req);
    const body = parseJsonBody(raw);
    const action = String(body.action || '').trim();
    const store = await getQuotaStore();

    if (!store) {
      sendJson(res, 503, { ok: false, error: 'quota store unavailable' });
      return;
    }

    if (action === 'set_free_quota') {
      const freeQuota = await setEffectiveFreeQuota(store, body.freeQuota);
      sendJson(res, 200, { ok: true, freeQuota });
      return;
    }

    if (action === 'reset_free_usage') {
      const deleted = await resetFreeUsage(store);
      sendJson(res, 200, { ok: true, deleted });
      return;
    }

    sendJson(res, 400, { ok: false, error: 'unknown action' });
  } catch (error) {
    sendJson(res, 500, { ok: false, error: String(error.message || error) });
  }
}

async function handleKeys(req, res) {
  try {
    const raw = await readRequestBody(req);
    const body = parseJsonBody(raw);
    const action = String(body.action || '').trim();
    const key = String(body.key || '').trim();
    const context = await resolveAccessContext(req);
    const store = context.store;

    if (!store) {
      if (action === 'check_free') {
        sendJson(res, 200, { free: true, mock: true });
        return;
      }

      if (action === 'validate') {
        sendJson(res, 200, { valid: true, mock: true });
        return;
      }

      sendJson(res, 200, { ok: true, mock: true });
      return;
    }

    if (action === 'check_free') {
      const freeQuota = await getEffectiveFreeQuota(store);
      const used = Number((await store.get(buildGuestUsageKey(context.userId, context.guestKey))) || 0);
      sendJson(res, 200, { free: used < freeQuota });
      return;
    }

    if (action === 'consume_free') {
      const freeQuota = await getEffectiveFreeQuota(store);
      const freeKey = buildGuestUsageKey(context.userId, context.guestKey);
      const used = Number((await store.get(freeKey)) || 0);

      if (used >= freeQuota) {
        sendJson(res, 403, { error: 'free_exhausted' });
        return;
      }

      await store.incr(freeKey);
      sendJson(res, 200, { ok: true });
      return;
    }

    if (action === 'status') {
      const freeQuota = await getEffectiveFreeQuota(store);
      sendJson(res, 200, {
        freeQuota,
        user: context.user
          ? {
              id: context.user.id,
              role: context.user.role,
              credits: context.user.credits || 0,
              profileName: context.user.profileName || null,
            }
          : null,
      });
      return;
    }

    if (action === 'validate') {
      if (!key) {
        sendJson(res, 400, { valid: false, error: '请输入密钥' });
        return;
      }

      const exists = await store.sismember(KEYS_SET, key);
      sendJson(res, 200, { valid: !!exists });
      return;
    }

    if (action === 'consume') {
      if (!context.user) {
        sendJson(res, 403, { error: '请先创建账号再使用密钥' });
        return;
      }

      if (!key) {
        sendJson(res, 400, { error: '请输入密钥' });
        return;
      }

      const removed = await store.srem(KEYS_SET, key);
      if (!removed) {
        sendJson(res, 403, { error: '密钥无效或已使用' });
        return;
      }

      await updateUser(store, context.user.id, (current) => ({
        ...current,
        credits: Number(current.credits || 0) + ACCOUNT_CREDIT_PER_KEY,
      }));

      sendJson(res, 200, { ok: true, creditsAdded: ACCOUNT_CREDIT_PER_KEY });
      return;
    }

    if (action === 'consume_credit') {
      if (!context.user) {
        sendJson(res, 403, { error: '请先创建账号' });
        return;
      }

      const current = await readUser(store, context.user.id);
      const credits = Number(current?.credits || 0);

      if (credits <= 0) {
        sendJson(res, 403, { error: 'credits_exhausted', errorCode: 'credits_exhausted' });
        return;
      }

      const nextUser = await updateUser(store, context.user.id, (user) => ({
        ...user,
        credits: Math.max(0, Number(user.credits || 0) - 1),
      }));

      sendJson(res, 200, {
        ok: true,
        credits: Number(nextUser.credits || 0),
      });
      return;
    }

    if (action === 'import') {
      if (!requireAdmin(req)) {
        sendJson(res, 401, { error: 'Unauthorized' });
        return;
      }

      const keys = Array.isArray(body.keys) ? body.keys.map((item) => String(item).trim()).filter(Boolean) : [];
      if (keys.length === 0) {
        sendJson(res, 400, { error: 'keys array required' });
        return;
      }

      await store.sadd(KEYS_SET, ...keys);
      const total = await store.scard(KEYS_SET);
      sendJson(res, 200, { imported: keys.length, total });
      return;
    }

    sendJson(res, 400, { error: 'Invalid action' });
  } catch (error) {
    sendJson(res, 500, {
      ok: false,
      error: String(error)
    });
  }
}

async function handleUpdateProviders(req, res) {
  if (!requireAdmin(req)) {
    sendJson(res, 401, { ok: false, error: 'unauthorized' });
    return;
  }

  try {
    const raw = await readRequestBody(req);
    const body = parseJsonBody(raw);
    const current = readConfig();

    if (!Array.isArray(body.providers) || body.providers.length === 0) {
      sendJson(res, 400, { ok: false, error: 'providers must be a non-empty array' });
      return;
    }

    const providers = body.providers.map(sanitizeProvider);
    providers.forEach(validateProvider);

    const nextConfig = {
      roundRobinIndex: Number.isInteger(body.roundRobinIndex) ? body.roundRobinIndex : 0,
      request: {
        ...current.request,
        ...(body.request || {})
      },
      providers
    };

    writeConfig(nextConfig);
    sendJson(res, 200, {
      ok: true,
      providerCount: providers.length
    });
  } catch (error) {
    sendJson(res, 400, {
      ok: false,
      error: String(error)
    });
  }
}

function handleGetProviders(req, res) {
  if (!requireAdmin(req)) {
    sendJson(res, 401, { ok: false, error: 'unauthorized' });
    return;
  }

  const config = readConfig();
  sendJson(res, 200, {
    ok: true,
    roundRobinIndex: config.roundRobinIndex,
    request: config.request,
    providers: config.providers.map((provider) => ({
      name: provider.name,
      url: provider.url,
      enabled: provider.enabled !== false,
      apiKeyMasked: provider.apiKey ? `${provider.apiKey.slice(0, 6)}...${provider.apiKey.slice(-4)}` : ''
    }))
  });
}

const server = http.createServer(async (req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const generateJobImageMatch = parsedUrl.pathname.match(/^\/api\/generate\/jobs\/([^/]+)\/image$/);
  const generateJobMatch = parsedUrl.pathname.match(/^\/api\/generate\/jobs\/([^/]+)$/);
  const userJobsMatch = parsedUrl.pathname.match(/^\/api\/session\/([^/]+)\/jobs$/);

  if (req.method === 'GET' && parsedUrl.pathname === '/health') {
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === 'POST' && parsedUrl.pathname === '/api/generate/jobs') {
    await handleCreateGenerateJob(req, res);
    return;
  }

  if (req.method === 'POST' && parsedUrl.pathname === '/api/session') {
    await handleSession(req, res);
    return;
  }

  if (req.method === 'POST' && parsedUrl.pathname === '/api/session/register') {
    await handleRegister(req, res);
    return;
  }

  if (userJobsMatch && req.method === 'GET') {
    await handleUserJobs(req, res, decodeURIComponent(userJobsMatch[1]));
    return;
  }

  if (req.method === 'GET' && parsedUrl.pathname === '/internal/dashboard/stats') {
    await handleDashboardStats(req, res);
    return;
  }

  if (req.method === 'POST' && parsedUrl.pathname === '/internal/dashboard/stats') {
    await handleDashboardAction(req, res);
    return;
  }

  if (generateJobImageMatch && req.method === 'GET') {
    const jobId = decodeURIComponent(generateJobImageMatch[1]);
    await handleGetGenerateJobImage(req, res, jobId);
    return;
  }

  if (generateJobMatch) {
    const jobId = decodeURIComponent(generateJobMatch[1]);

    if (req.method === 'GET') {
      await handleGetGenerateJob(req, res, jobId);
      return;
    }

    if (req.method === 'DELETE') {
      await handleCancelGenerateJob(req, res, jobId);
      return;
    }
  }

  if (req.method === 'POST' && parsedUrl.pathname === '/api/generate-image') {
    await handleGenerate(req, res);
    return;
  }

  if (req.method === 'POST' && parsedUrl.pathname === '/api/generate') {
    await handleGenerate(req, res);
    return;
  }

  if (req.method === 'POST' && parsedUrl.pathname === '/api/keys') {
    await handleKeys(req, res);
    return;
  }

  if (req.method === 'POST' && parsedUrl.pathname === '/internal/providers') {
    await handleUpdateProviders(req, res);
    return;
  }

  if (req.method === 'GET' && parsedUrl.pathname === '/internal/providers') {
    handleGetProviders(req, res);
    return;
  }

  sendText(res, 404, 'Not Found');
});

if (require.main === module) {
  server.listen(PORT, HOST, () => {
    console.log(`image-relay-backend listening on http://${HOST}:${PORT}`);
  });
}

module.exports = {
  server,
  __test__: {
    buildGuestUsageKey,
    ensureUser,
    readUser,
    updateUser,
    migrateGuestUsageToUser,
    normalizeUserId,
  },
};
