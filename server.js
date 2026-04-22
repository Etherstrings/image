const fs = require('fs');
const http = require('http');
const path = require('path');
const { URL } = require('url');

const PORT = Number.parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const CONFIG_PATH = path.join(__dirname, 'providers.json');

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
  return req.headers['x-admin-key'] === ADMIN_KEY;
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
      if (obj.item.type === 'image_generation_call') {
        result.finalCall = obj.item;
      }
      if (obj.item.type === 'message' && Array.isArray(obj.item.content)) {
        for (const part of obj.item.content) {
          if (part.type === 'output_text' && part.text) {
            result.outputText += part.text;
          }
        }
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

async function tryProvider(provider, prompt, requestConfig) {
  const payload = {
    model: requestConfig.model || 'gpt-5.4',
    input: prompt,
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

  const timeoutMs = Number.isFinite(requestConfig.timeoutMs) ? requestConfig.timeoutMs : 180000;
  const endpoints = buildResponseEndpoints(provider);
  let lastFailure = null;

  for (const endpoint of endpoints) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${provider.apiKey}`,
          'Content-Type': 'application/json',
          Accept: 'text/event-stream'
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      });

      const contentType = response.headers.get('content-type') || '';

      if (!response.ok) {
        lastFailure = {
          ok: false,
          provider: provider.name,
          endpoint,
          status: response.status,
          contentType,
          body: (await response.text()).slice(0, 2000)
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
          body: (await response.text()).slice(0, 2000)
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
          }
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
    } finally {
      clearTimeout(timer);
    }
  }

  return lastFailure || {
    ok: false,
    provider: provider.name,
    endpoint: endpoints[0] || '',
    status: null,
    contentType: null,
    body: 'No valid endpoint candidates'
  };
}

async function generateImageWithFallback(prompt) {
  const config = readConfig();
  const enabledProviders = config.providers.filter((provider) => provider.enabled !== false);
  if (enabledProviders.length === 0) {
    return {
      ok: false,
      statusCode: 500,
      error: 'No enabled providers configured',
      attempts: []
    };
  }

  const orderedProviders = rotateProviders(enabledProviders, config.roundRobinIndex);
  const attempts = [];
  const retriesPerProvider = Math.max(1, Number.parseInt(String(config.request.retriesPerProvider || 1), 10));
  const retryRounds = Math.max(1, Number.parseInt(String(config.request.retryRounds || 1), 10));
  const retryDelayMs = Math.max(0, Number.parseInt(String(config.request.retryDelayMs || 0), 10));

  for (let round = 0; round < retryRounds; round += 1) {
    for (let providerIndex = 0; providerIndex < orderedProviders.length; providerIndex += 1) {
      const provider = orderedProviders[providerIndex];
      for (let retry = 0; retry < retriesPerProvider; retry += 1) {
        const result = await tryProvider(provider, prompt, config.request);
        attempts.push({
          provider: result.provider,
          endpoint: result.endpoint,
          ok: result.ok,
          status: result.status || null,
          round: round + 1,
          retry: retry + 1,
          meta: result.meta || null,
          error: result.error || result.body || result.sse || null
        });

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

        await delay(retryDelayMs);
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
  try {
    const raw = await readRequestBody(req);
    const body = parseJsonBody(raw);
    const prompt = String(body.prompt || '').trim();

    if (!prompt) {
      sendJson(res, 400, { ok: false, error: 'prompt is required' });
      return;
    }

    const result = await generateImageWithFallback(prompt);
    if (!result.ok) {
      sendJson(res, result.statusCode || 500, {
        ok: false,
        error: result.error,
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

  if (req.method === 'GET' && parsedUrl.pathname === '/health') {
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method === 'POST' && parsedUrl.pathname === '/api/generate-image') {
    await handleGenerate(req, res);
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

server.listen(PORT, HOST, () => {
  console.log(`image-relay-backend listening on http://${HOST}:${PORT}`);
});
