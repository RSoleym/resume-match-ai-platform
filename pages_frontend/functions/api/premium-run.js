const MAX_PREMIUM_SEARCHES = 3;
const DEFAULT_SEARCH_MODEL = 'gpt-4o-search-preview';
const TARGET_RESULTS = 5;
const MAX_SEARCH_CALLS = 2;
const MAX_EXCLUDE_URLS = 8;

const CATEGORY_ROLE_HINTS = {
  'Hardware / RTL / Verification': [
    'Design Verification Engineer',
    'ASIC Verification Engineer',
    'RTL Design Engineer',
    'FPGA Engineer',
  ],
  'Embedded / Firmware': [
    'Embedded Firmware Engineer',
    'Firmware Engineer',
    'Embedded Software Engineer',
  ],
  'Software Engineering': ['Software Engineer', 'Backend Engineer', 'C++ Software Engineer', 'Python Engineer'],
  'Data / AI / ML': ['Machine Learning Engineer', 'AI Engineer', 'Data Scientist'],
};

export async function onRequestPost(context) {
  const supabaseUrl = readEnv(context.env, ['SUPABASE_URL']);
  const anonKey = readEnv(context.env, ['SUPABASE_ANON_KEY', 'SUPABASE_PUBLISHABLE_KEY']);
  const secretKey = readEnv(context.env, ['SUPABASE_SECRET_KEY', 'SUPABASE_SERVICE_ROLE_KEY', 'SUPABASE_SERVICE_KEY']);
  const openAiKey = readEnv(context.env, ['OPENAI_API_KEY', 'OPENAI_KEY']);
  const searchModel = readEnv(context.env, ['OPENAI_WEB_CHAT_MODEL', 'OPENAI_SEARCH_MODEL', 'OPENAI_MODEL']) || DEFAULT_SEARCH_MODEL;

  const missing = [];
  if (!supabaseUrl) missing.push('SUPABASE_URL');
  if (!anonKey) missing.push('SUPABASE_ANON_KEY');
  if (!secretKey) missing.push('SUPABASE_SECRET_KEY');
  if (!openAiKey) missing.push('OPENAI_API_KEY');
  if (missing.length) {
    return json({ error: `Missing premium backend configuration: ${missing.join(', ')}.` }, 500);
  }

  try {
    const token = readBearer(context.request);
    const user = await getAuthedUser({ supabaseUrl, anonKey, token });
    const body = await context.request.json().catch(() => ({}));
    const resumeContext = body?.resumeContext || {};
    const filters = body?.filters || {};
    const resumeId = clean(body?.resumeId, 120);

    if (!resumeId) return json({ error: 'Resume ID is required.' }, 400);
    if (!hasMeaningfulResumeContext(resumeContext)) {
      return json({ error: 'Parsed resume data is required for premium web search.' }, 400);
    }

    const profile = await getProfile({ supabaseUrl, secretKey, userId: user.id });
    const premiumUnlocked = !!profile?.premium_access || !!profile?.premium_admin_access;
    if (!premiumUnlocked) return json({ error: 'Premium is still locked for this account.' }, 403);

    const searchesUsed = Number(profile?.premium_searches_used || 0);
    const isAdmin = !!profile?.premium_admin_access;
    if (!isAdmin && searchesUsed >= MAX_PREMIUM_SEARCHES) {
      return json({ error: 'No premium searches remaining.' }, 403);
    }

    const startedAt = Date.now();
    const searchResult = await searchLiveJobsWithOpenAI({
      apiKey: openAiKey,
      searchModel,
      resumeContext,
      filters,
    });
    const premium_compare_ms = Date.now() - startedAt;

    if (!isAdmin) {
      await patchProfile({
        supabaseUrl,
        secretKey,
        userId: user.id,
        patch: {
          premium_searches_used: searchesUsed + 1,
          premium_last_run_at: new Date().toISOString(),
          premium_access: true,
        },
      });
    }

    return json({
      ok: true,
      results: searchResult.results,
      filters,
      used: isAdmin ? searchesUsed : searchesUsed + 1,
      max_searches: MAX_PREMIUM_SEARCHES,
      timings: { premium_compare_ms },
      search_mode: 'live_web_search',
      sources: searchResult.sources,
    });
  } catch (error) {
    return json({ error: simplifyErrorMessage(error) }, 500);
  }
}

async function searchLiveJobsWithOpenAI({ apiKey, searchModel, resumeContext, filters }) {
  const requestedCountry = clean(filters?.country || resumeContext?.candidate_country || '', 80);
  const requestedRegion = clean(filters?.region || '', 80);
  const requestedWorkMode = clean(filters?.workMode || '', 40);
  const postedWindow = normalizePosted(filters?.posted || 'all');

  const sourceMap = new Map();
  const merged = new Map();
  const attempts = buildBroadeningPlan({
    country: requestedCountry,
    region: requestedRegion,
    workMode: requestedWorkMode,
    posted: postedWindow,
  }).slice(0, MAX_SEARCH_CALLS);
  const focusTitles = makeFocusTitleBatches(resumeContext)[0] || [];

  let lastError = '';
  for (const attempt of attempts) {
    try {
      const { jobs, sources } = await callChatSearch({
        apiKey,
        model: searchModel,
        resumeContext,
        filters: attempt,
        focusTitles,
        excludeUrls: Array.from(sourceMap.keys()).slice(0, MAX_EXCLUDE_URLS),
      });
      addSources(sourceMap, sources);
      addJobs(merged, jobs, attempt);
      if (merged.size >= TARGET_RESULTS) break;
    } catch (error) {
      lastError = clean(error?.message || error, 600);
    }
  }

  if (!merged.size) {
    try {
      const fallback = await callResponsesSearch({
        apiKey,
        model: searchModel,
        resumeContext,
        filters: attempts[0] || { country: requestedCountry, region: requestedRegion, workMode: requestedWorkMode, posted: postedWindow },
        focusTitles,
        excludeUrls: Array.from(sourceMap.keys()).slice(0, MAX_EXCLUDE_URLS),
      });
      addSources(sourceMap, fallback.sources);
      addJobs(merged, fallback.jobs, filters);
    } catch (error) {
      lastError = clean(error?.message || error, 600);
    }
  }

  if (!merged.size) {
    throw new Error(lastError || 'Premium live web search could not find usable job results.');
  }

  const results = Array.from(merged.values())
    .sort((a, b) => (b.final_match_percent || 0) - (a.final_match_percent || 0))
    .slice(0, TARGET_RESULTS);

  return {
    results,
    sources: Array.from(sourceMap.values()).slice(0, TARGET_RESULTS),
  };
}

async function callChatSearch({ apiKey, model, resumeContext, filters, focusTitles, excludeUrls }) {
  const prompt = buildCompactSearchPrompt({ resumeContext, filters, focusTitles, excludeUrls, maxResults: TARGET_RESULTS });
  const webSearchOptions = { search_context_size: 'low' };
  const userLocation = buildChatUserLocation(filters?.country, filters?.region);
  if (userLocation) webSearchOptions.user_location = userLocation;

  const body = {
    model,
    messages: [
      {
        role: 'developer',
        content: 'Find 5 current direct job-posting pages from the live web that fit the candidate. Do not use database jobs. Avoid search result pages. Return only JSON with a top-level jobs array.',
      },
      { role: 'user', content: prompt },
    ],
    web_search_options: webSearchOptions,
    max_completion_tokens: 700,
    temperature: 0.1,
  };

  const data = await fetchOpenAiJsonWithRetry({
    url: 'https://api.openai.com/v1/chat/completions',
    apiKey,
    body,
  });
  const { text, sources } = extractChatTextAndSources(data);
  const jobs = normalizeSearchResults(extractJsonArray(text), filters, sources);
  return { jobs, sources };
}

async function callResponsesSearch({ apiKey, model, resumeContext, filters, focusTitles, excludeUrls }) {
  const prompt = buildCompactSearchPrompt({ resumeContext, filters, focusTitles, excludeUrls, maxResults: TARGET_RESULTS });
  const tool = { type: 'web_search' };
  const userLocation = buildResponsesUserLocation(filters?.country, filters?.region);
  if (userLocation) tool.user_location = userLocation;

  const body = {
    model,
    tools: [tool],
    tool_choice: 'auto',
    include: ['web_search_call.action.sources'],
    input: prompt,
    max_output_tokens: 900,
    store: false,
  };

  const data = await fetchOpenAiJsonWithRetry({
    url: 'https://api.openai.com/v1/responses',
    apiKey,
    body,
  });

  const text = extractOutputText(data);
  const sources = extractSources(data);
  const jobs = normalizeSearchResults(extractJsonArray(text), filters, sources);
  return { jobs, sources };
}

async function fetchOpenAiJsonWithRetry({ url, apiKey, body, attempts = 2 }) {
  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(body),
    });

    if (response.ok) {
      return response.json().catch(() => ({}));
    }

    const message = await response.text();
    if (response.status === 429 && attempt < attempts) {
      const waitMs = extractRetryDelayMs(message);
      await sleep(waitMs);
      lastError = new Error(message || 'Rate limit reached.');
      continue;
    }
    throw new Error(message || 'OpenAI request failed.');
  }
  throw lastError || new Error('OpenAI request failed.');
}

function buildCompactSearchPrompt({ resumeContext, filters, focusTitles, excludeUrls, maxResults }) {
  const profile = deriveResumeSearchProfile(resumeContext);
  const requestedTitles = uniqueKeepOrder([...(focusTitles || []), ...(profile.role_titles || [])], 4);
  const keywords = uniqueKeepOrder(profile.keywords || [], 10);
  const negative = uniqueKeepOrder(profile.negative_terms || [], 6);

  return JSON.stringify({
    candidate: {
      category: clean(resumeContext.candidate_category_key, 80),
      function: clean(resumeContext.candidate_function, 80),
      domain: clean(resumeContext.candidate_domain, 80),
      target_titles: requestedTitles,
      skill_keywords: keywords,
      summary: clean(resumeContext.summary || resumeContext.experience || resumeContext.projects || resumeContext.resume_text, 700),
    },
    filters: {
      country: clean(filters?.country, 60),
      region: clean(filters?.region, 60),
      work_mode: clean(filters?.workMode, 24),
      posted_window: clean(filters?.posted, 40),
      exclude_urls: Array.isArray(excludeUrls) ? excludeUrls.slice(0, MAX_EXCLUDE_URLS) : [],
    },
    instructions: {
      live_web_only: true,
      no_database_jobs: true,
      direct_job_pages_only: true,
      max_results: Math.max(3, Number(maxResults || TARGET_RESULTS)),
      avoid_terms: negative,
      return_shape: {
        jobs: [
          {
            title: 'string',
            company: 'string',
            location: 'string',
            country: 'string',
            work_mode: 'string',
            posted_date: 'string',
            description_text: 'short string',
            url: 'string',
            match_percentage: 0,
            reason: 'short string',
          },
        ],
      },
      json_only: true,
    },
  });
}

function hasMeaningfulResumeContext(resumeContext) {
  return !!(
    clean(resumeContext?.resume_text, 80)
    || clean(resumeContext?.summary, 80)
    || clean(resumeContext?.skills, 80)
    || clean(resumeContext?.experience, 80)
    || clean(resumeContext?.candidate_function, 40)
  );
}

function deriveResumeSearchProfile(resumeContext) {
  const category = clean(resumeContext?.candidate_category_key, 120);
  const functionName = clean(resumeContext?.candidate_function, 120);
  const domainName = clean(resumeContext?.candidate_domain, 120);
  const textBlob = [
    clean(resumeContext?.summary, 800),
    clean(resumeContext?.skills, 900),
    clean(resumeContext?.experience, 900),
    clean(resumeContext?.projects, 700),
    clean(resumeContext?.resume_text, 1200),
  ].join(' ').toLowerCase();

  const roles = [];
  if (functionName) roles.push(functionName);
  if (domainName) roles.push(domainName);
  roles.push(...(CATEGORY_ROLE_HINTS[category] || []));
  if (textBlob.includes('design verification') || textBlob.includes('uvm') || textBlob.includes('systemverilog')) roles.unshift('Design Verification Engineer');
  if (textBlob.includes('rtl') || textBlob.includes('verilog')) roles.unshift('RTL Design Engineer');
  if (textBlob.includes('fpga')) roles.push('FPGA Engineer');
  if (textBlob.includes('embedded') || textBlob.includes('firmware') || textBlob.includes('cortex-m')) roles.push('Embedded Firmware Engineer');

  const keywords = [];
  for (const piece of String(resumeContext?.skills || '').split(/[,;|/\n]/)) {
    const token = clean(piece, 40);
    if (token) keywords.push(token);
  }
  for (const token of ['systemverilog', 'verilog', 'rtl', 'uvm', 'asic', 'fpga', 'embedded', 'firmware', 'python', 'c++', 'vivado']) {
    if (textBlob.includes(token)) keywords.push(token);
  }

  const negative = [];
  if (category === 'Hardware / RTL / Verification') {
    negative.push('mechanical', 'civil', 'construction', 'power systems');
  }

  return {
    role_titles: uniqueKeepOrder(roles, 6),
    keywords: uniqueKeepOrder(keywords, 10),
    negative_terms: uniqueKeepOrder(negative, 6),
  };
}

function makeFocusTitleBatches(resumeContext) {
  const titles = deriveResumeSearchProfile(resumeContext).role_titles || [];
  if (!titles.length) return [['']];
  return [titles.slice(0, 2)];
}

function buildBroadeningPlan({ country, region, workMode, posted }) {
  const plan = [
    { country, region, workMode, posted },
    { country, region: '', workMode, posted },
  ];
  const seen = new Set();
  return plan.filter((item) => {
    const key = JSON.stringify(item);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function normalizeSearchResults(items, filters, sources) {
  const rows = [];
  const sourceQueue = Array.isArray(sources) ? sources.slice() : [];
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index] || {};
    const source = sourceQueue[index] || null;
    const bestUrl = clean(item.url || item.best_url || item.link || item.source_url || source?.url, 500);
    const title = clean(item.title || item.job_title || source?.title, 180);
    if (!bestUrl || !title) continue;
    const company = clean(item.company || inferCompanyFromTitle(source?.title || title), 140) || 'Unknown company';
    const location = clean(item.location, 160);
    const country = clean(item.country, 80) || clean(filters?.country, 80);
    const workMode = normalizeWorkMode(item.work_mode || item.workMode || filters?.workMode || 'on-site');
    const score = clamp(Number(item.match_percentage || item.premium_score || 0), 0, 100);
    rows.push({
      job_id: makeStableId(`${title}-${company}-${bestUrl}`),
      title,
      company,
      location,
      country,
      region: inferRegion(location),
      work_mode: workMode,
      posted_date_display: clean(item.posted_date || item.posted_date_display, 60) || 'Recently posted',
      best_url: bestUrl,
      description_text: clean(item.description_text || item.description || '', 600),
      job_function: clean(item.job_function, 120),
      job_domain: clean(item.job_domain, 120),
      job_category_key: clean(item.job_category_key || item.job_category, 120),
      raw_match_percent: Math.round(score),
      final_match_percent: Math.round(score),
      premium_reason: clean(item.reason || item.premium_reason, 140),
      penalty_applied: false,
      penalty_points: 0,
    });
  }
  return dedupeJobs(rows);
}

function extractChatTextAndSources(data) {
  const message = data?.choices?.[0]?.message || {};
  const text = extractChatText(data);
  const annotations = Array.isArray(message?.annotations) ? message.annotations : [];
  const sources = [];
  for (const item of annotations) {
    const citation = item?.url_citation || item;
    const url = clean(citation?.url, 500);
    if (!url) continue;
    sources.push({ url, title: clean(citation?.title, 180) });
  }
  return { text, sources: dedupeSources(sources) };
}

function extractChatText(data) {
  const message = data?.choices?.[0]?.message || {};
  if (typeof message?.content === 'string') return message.content.trim();
  if (Array.isArray(message?.content)) {
    return message.content.map((part) => clean(part?.text || part?.content || '', 12000)).filter(Boolean).join('\n').trim();
  }
  return '';
}

function extractOutputText(data) {
  if (typeof data?.output_text === 'string' && data.output_text.trim()) {
    return data.output_text.trim();
  }
  const parts = [];
  const output = Array.isArray(data?.output) ? data.output : [];
  for (const item of output) {
    if (item?.type !== 'message' || !Array.isArray(item?.content)) continue;
    for (const block of item.content) {
      if ((block?.type === 'output_text' || block?.type === 'text') && typeof block?.text === 'string') {
        parts.push(block.text);
      }
    }
  }
  return parts.join('\n').trim();
}

function extractJsonArray(text) {
  const raw = String(text || '').trim();
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed.filter((item) => item && typeof item === 'object');
    if (parsed && typeof parsed === 'object') {
      const jobs = parsed.jobs || parsed.results || parsed.suitable_jobs;
      if (Array.isArray(jobs)) return jobs.filter((item) => item && typeof item === 'object');
    }
  } catch {}
  const arrayMatch = raw.match(/\[[\s\S]*\]/);
  if (arrayMatch) {
    try {
      const parsed = JSON.parse(arrayMatch[0]);
      if (Array.isArray(parsed)) return parsed.filter((item) => item && typeof item === 'object');
    } catch {}
  }
  const obj = extractJsonObject(raw);
  const jobs = obj.jobs || obj.results || obj.suitable_jobs;
  return Array.isArray(jobs) ? jobs.filter((item) => item && typeof item === 'object') : [];
}

function extractJsonObject(text) {
  const raw = String(text || '').trim();
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    const match = raw.match(/\{[\s\S]*\}/);
    if (!match) return {};
    try {
      return JSON.parse(match[0]);
    } catch {
      return {};
    }
  }
}

function extractSources(data) {
  const out = [];
  const output = Array.isArray(data?.output) ? data.output : [];
  for (const item of output) {
    if (item?.type === 'web_search_call') {
      const sources = Array.isArray(item?.action?.sources) ? item.action.sources : [];
      for (const source of sources) {
        const url = clean(source?.url, 500);
        if (!url) continue;
        out.push({ url, title: clean(source?.title, 180) });
      }
    }
  }
  return dedupeSources(out);
}

function addJobs(targetMap, jobs, filters) {
  for (const job of jobs || []) {
    const url = clean(job?.best_url, 500);
    if (!url || targetMap.has(url)) continue;
    const row = {
      ...job,
      country: clean(job.country, 80) || clean(filters?.country, 80),
      work_mode: normalizeWorkMode(job.work_mode || filters?.workMode || 'on-site'),
    };
    targetMap.set(url, row);
    if (targetMap.size >= TARGET_RESULTS) break;
  }
}

function addSources(targetMap, sources) {
  for (const source of sources || []) {
    const url = clean(source?.url, 500);
    if (!url || targetMap.has(url)) continue;
    targetMap.set(url, { url, title: clean(source?.title, 180) });
  }
}

function dedupeJobs(rows) {
  const out = [];
  const seen = new Set();
  for (const row of rows) {
    const key = clean(row?.best_url || row?.job_id, 500);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function dedupeSources(rows) {
  const out = [];
  const seen = new Set();
  for (const row of rows) {
    const key = clean(row?.url, 500);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function inferCompanyFromTitle(title) {
  const text = String(title || '');
  const parts = text.split(/\s[-–|•]\s/).map((item) => item.trim()).filter(Boolean);
  if (parts.length >= 2) return parts[1];
  return '';
}

function buildChatUserLocation(countryName, city) {
  const country = countryCode(countryName);
  const locality = clean(city, 80);
  if (!country && !locality) return null;
  const approximate = {};
  if (country) approximate.country = country;
  if (locality) {
    approximate.city = locality;
    approximate.region = locality;
  }
  return {
    type: 'approximate',
    approximate,
  };
}

function buildResponsesUserLocation(countryName, city) {
  const country = countryCode(countryName);
  const locality = clean(city, 80);
  if (!country && !locality) return null;
  const out = { type: 'approximate' };
  if (country) out.country = country;
  if (locality) {
    out.city = locality;
    out.region = locality;
  }
  return out;
}

function countryCode(countryName) {
  const raw = String(countryName || '').trim();
  if (/^[A-Za-z]{2}$/.test(raw)) return raw.toUpperCase();
  const key = raw.toLowerCase();
  const map = {
    australia: 'AU',
    canada: 'CA',
    china: 'CN',
    'costa rica': 'CR',
    france: 'FR',
    germany: 'DE',
    india: 'IN',
    ireland: 'IE',
    israel: 'IL',
    japan: 'JP',
    'korea, republic of': 'KR',
    korea: 'KR',
    malaysia: 'MY',
    mexico: 'MX',
    netherlands: 'NL',
    poland: 'PL',
    singapore: 'SG',
    taiwan: 'TW',
    usa: 'US',
    'united states': 'US',
    'united kingdom': 'GB',
    uk: 'GB',
  };
  return map[key] || '';
}

function normalizePosted(value) {
  const posted = String(value || '').trim().toLowerCase();
  if (posted === 'day') return 'past 24 hours';
  if (posted === 'week') return 'past week';
  if (posted === 'month') return 'past month';
  return 'any recent time';
}

function normalizeWorkMode(value) {
  const text = String(value || '').trim().toLowerCase();
  if (!text) return 'on-site';
  if (text.includes('remote')) return 'remote';
  if (text.includes('hybrid')) return 'hybrid';
  if (text.includes('on-site') || text.includes('onsite')) return 'on-site';
  return text;
}

function inferRegion(location) {
  const text = String(location || '').trim();
  if (!text) return '';
  return text.split(',')[0].trim();
}

function uniqueKeepOrder(values, limit = 12) {
  const out = [];
  const seen = new Set();
  for (const raw of values || []) {
    const item = clean(raw, 120);
    const low = item.toLowerCase();
    if (!item || seen.has(low)) continue;
    seen.add(low);
    out.push(item);
    if (out.length >= limit) break;
  }
  return out;
}

function makeStableId(seed) {
  let hash = 0;
  const text = String(seed || 'job');
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash) + text.charCodeAt(i);
    hash |= 0;
  }
  return `premium_${Math.abs(hash)}`;
}

function clean(value, maxChars) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxChars);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function readEnv(env, keys) {
  for (const key of keys) {
    const value = String(env?.[key] || '').trim();
    if (value) return value;
  }
  return '';
}

function readBearer(request) {
  const header = request.headers.get('authorization') || '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) throw new Error('Missing session token. Sign in again.');
  return match[1].trim();
}

async function getAuthedUser({ supabaseUrl, anonKey, token }) {
  const response = await fetch(`${supabaseUrl}/auth/v1/user`, {
    headers: {
      apikey: anonKey,
      Authorization: `Bearer ${token}`,
    },
  });
  if (!response.ok) throw new Error('Your session expired. Sign in again.');
  return response.json();
}

async function getProfile({ supabaseUrl, secretKey, userId }) {
  const url = `${supabaseUrl}/rest/v1/profiles?id=eq.${encodeURIComponent(userId)}&select=premium_access,premium_admin_access,premium_searches_used`;
  const response = await fetch(url, {
    headers: {
      apikey: secretKey,
      Authorization: `Bearer ${secretKey}`,
    },
  });
  if (!response.ok) throw new Error('Could not load premium profile state.');
  const rows = await response.json().catch(() => []);
  return Array.isArray(rows) ? rows[0] || null : null;
}

async function patchProfile({ supabaseUrl, secretKey, userId, patch }) {
  const response = await fetch(`${supabaseUrl}/rest/v1/profiles?id=eq.${encodeURIComponent(userId)}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      apikey: secretKey,
      Authorization: `Bearer ${secretKey}`,
    },
    body: JSON.stringify(patch),
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Could not update premium usage count.');
  }
}

function extractRetryDelayMs(message) {
  const text = String(message || '');
  const match = text.match(/try again in\s+([\d.]+)s/i);
  const seconds = match ? Number(match[1]) : 6;
  if (!Number.isFinite(seconds) || seconds <= 0) return 6000;
  return Math.min(Math.ceil(seconds * 1000) + 350, 10000);
}

function simplifyErrorMessage(error) {
  const text = clean(error?.message || error, 1200);
  if (/rate_limit_exceeded|Rate limit reached/i.test(text)) {
    return 'Premium search hit the OpenAI rate limit. Wait about 10 seconds and run it again.';
  }
  return text || 'Unexpected premium backend error.';
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
    },
  });
}
