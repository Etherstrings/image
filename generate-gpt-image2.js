const fs = require('fs');
const path = require('path');

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = 'true';
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

function buildEndpoint(baseUrl) {
  return `${baseUrl.replace(/\/+$/, '')}/responses`;
}

function requireValue(name, value) {
  if (!value) {
    throw new Error(`Missing required value: ${name}`);
  }
  return value;
}

async function readSseResponse(response) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const result = {
    responseId: null,
    createdTool: null,
    finalCall: null,
    outputText: '',
    error: null,
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
        // Ignore malformed chunks from intermediary relays.
      }
    }
  }

  return result;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const apiKey = requireValue('OPENAI_API_KEY', args['api-key'] || process.env.OPENAI_API_KEY);
  const baseUrl = args['base-url'] || process.env.BASE_URL || 'https://api.asxs.top/v1';
  const prompt = requireValue('prompt', args.prompt || process.env.PROMPT);
  const outputPath = args.output || process.env.OUTPUT || 'generated-image.png';
  const retries = Number.parseInt(args.retries || process.env.RETRIES || '3', 10);
  const endpoint = buildEndpoint(baseUrl);

  const payload = {
    model: 'gpt-5.4',
    input: prompt,
    tools: [
      {
        type: 'image_generation',
        model: 'gpt-image-2',
        size: '1024x1536',
        quality: 'high',
        output_format: 'png',
      },
    ],
    tool_choice: { type: 'image_generation' },
    stream: true,
  };

  let lastFailure = null;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        Accept: 'text/event-stream',
      },
      body: JSON.stringify(payload),
    });

    const contentType = response.headers.get('content-type') || '';

    if (!response.ok) {
      lastFailure = {
        attempt,
        status: response.status,
        contentType,
        body: (await response.text()).slice(0, 2000),
      };
      continue;
    }

    if (!contentType.includes('text/event-stream')) {
      lastFailure = {
        attempt,
        status: response.status,
        contentType,
        body: (await response.text()).slice(0, 2000),
      };
      continue;
    }

    const sse = await readSseResponse(response);
    const imageBase64 = sse.finalCall && sse.finalCall.result;

    if (!imageBase64) {
      lastFailure = {
        attempt,
        status: response.status,
        contentType,
        sse,
      };
      continue;
    }

    const absoluteOutput = path.resolve(outputPath);
    fs.writeFileSync(absoluteOutput, Buffer.from(imageBase64, 'base64'));

    const summary = {
      ok: true,
      endpoint,
      output: absoluteOutput,
      bytes: fs.statSync(absoluteOutput).size,
      response_id: sse.responseId,
      created_tool: sse.createdTool
        ? {
            type: sse.createdTool.type,
            model: sse.createdTool.model,
            quality: sse.createdTool.quality,
            size: sse.createdTool.size,
            output_format: sse.createdTool.output_format,
          }
        : null,
      final_call: sse.finalCall
        ? {
            type: sse.finalCall.type,
            quality: sse.finalCall.quality,
            size: sse.finalCall.size,
            output_format: sse.finalCall.output_format,
            revised_prompt: sse.finalCall.revised_prompt || null,
          }
        : null,
      output_text: sse.outputText || '',
    };

    console.log(JSON.stringify(summary, null, 2));
    return;
  }

  console.log(
    JSON.stringify(
      {
        ok: false,
        endpoint,
        failure: lastFailure,
      },
      null,
      2
    )
  );
  process.exit(1);
}

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: String(error),
        cause: error && error.cause
          ? {
              code: error.cause.code,
              message: error.cause.message,
            }
          : null,
      },
      null,
      2
    )
  );
  process.exit(1);
});
