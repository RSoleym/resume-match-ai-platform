const DEFAULT_MODEL = 'gpt-5';
const MAX_PREMIUM_SEARCHES = 5;

export async function onRequestPost(context) {
  const supabaseUrl = readEnv(context.env, ['SUPABASE_URL']);
  const anonKey = readEnv(context.env, ['SUPABASE_ANON_KEY', 'SUPABASE_PUBLISHABLE_KEY']);
  const secretKey = readEnv(context.env, ['SUPABASE_SECRET_KEY', 'SUPABASE_SERVICE_ROLE_KEY', 'SUPABASE_SERVICE_KEY']);
  const openAiKey = readEnv(context.env, ['OPENAI_API_KEY', 'OPENAI_KEY']);
  const openAiModel = readEnv(context.env, ['OPENAI_MODEL', 'OPENAI_DEFAULT_MODEL']) || DEFAULT_MODEL;

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
    const resumeId = String(body?.resumeId || '').trim();

    if (!resumeId) return json({ error: 'Resume ID is required.' }, 400);
    if (!String(resumeContext?.resume_text || '').trim()) {
      return json({ error: 'Parsed resume text is required for premium web search.' }, 400);
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
      model: openAiModel,
      resumeContext,
      filters,
    });
    const premium_compare_ms = Date.now() - startedAt;

    if (!isAdmin) {
      const nextUsed = searchesUsed + 1;
      await patchProfile({
        supabaseUrl,
        secretKey,
        userId: user.id,
        patch: {
          premium_searches_used: nextUsed,
          premium_last_run_at: new Date().toISOString(),
          premium_access: nextUsed < MAX_PREMIUM_SEARCHES,
        },
      });
    }

    return json({
      ok: true,
      results: searchResult.results,
      filters,
      used: isAdmin ? searchesUsed : Math.min(MAX_PREMIUM_SEARCHES, searchesUsed + 1),
      timings: { premium_compare_ms },
      search_mode: 'live_web_search',
      sources: searchResult.sources,
    });
  } catch (error) {
    return json({ error: error?.message || 'Unexpected premium backend error.' }, 500);
  }
}

async function searchLiveJobsWithOpenAI({ apiKey, model, resumeContext, filters }) {
  const requestedCountry = clean(filters?.country || resumeContext?.candidate_country || '', 80);
  const requestedRegion = clean(filters?.region || '', 80);
  const requestedWorkMode = clean(filters?.workMode || '', 40);
  const postedWindow = normalizePosted(filters?.posted || 'all');
  const userLocation = buildUserLocation(requestedCountry, requestedRegion);

  const inputPayload = {
    candidate: {
      country: clean(resumeContext.candidate_country, 80),
      experience_years: resumeContext.candidate_experience_years ?? null,
      degree_level: clean(resumeContext.candidate_degree_level, 60),
      degree_family: clean(resumeContext.candidate_degree_family, 120),
      degree_fields: Array.isArray(resumeContext.candidate_degree_fields) ? resumeContext.candidate_degree_fields.slice(0, 8) : [],
      function: clean(resumeContext.candidate_function, 120),
      domain: clean(resumeContext.candidate_domain, 120),
      category: clean(resumeContext.candidate_category_key, 120),
      resume_text_excerpt: clean(resumeContext.resume_text, 3200),
    },
    filters: {
      country: requestedCountry,
      region: requestedRegion,
      work_mode: requestedWorkMode,
      posted_window: postedWindow,
    },
    task: {
      search_the_live_web: true,
      do_not_use_database_jobs: true,
      infer_target_roles_from_resume: true,
      max_results: 12,
      prefer_official_company_or_ats_pages: true,
      require_current_open_roles_if_possible: true,
    },
  };

  const requestBody = {
    model,
    reasoning: { effort: 'low' },
    tool_choice: 'required',
    tools: [
      userLocation
        ? { type: 'web_search', user_location: userLocation }
        : { type: 'web_search' },
    ],
    include: ['web_search_call.action.sources'],
    text: {
      format: {
        type: 'json_schema',
        name: 'premium_job_search_results',
        strict: true,
        schema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            role_summary: { type: 'string' },
            results: {
              type: 'array',
              maxItems: 12,
              items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  title: { type: 'string' },
                  company: { type: 'string' },
                  location: { type: 'string' },
                  country: { type: 'string' },
                  work_mode: { type: 'string' },
                  posted_date_display: { type: 'string' },
                  best_url: { type: 'string' },
                  description_text: { type: 'string' },
                  job_function: { type: 'string' },
                  job_domain: { type: 'string' },
                  job_category_key: { type: 'string' },
                  final_match_percent: { type: 'number' },
                  premium_reason: { type: 'string' }
                },
                required: [
                  'title',
                  'company',
                  'location',
                  'country',
                  'work_mode',
                  'posted_date_display',
                  'best_url',
                  'description_text',
                  'job_function',
                  'job_domain',
                  'job_category_key',
                  'final_match_percent',
                  'premium_reason'
                ]
              }
            }
          },
          required: ['role_summary', 'results']
        }
      },
      verbosity: 'low',
    },
    input: [
      {
        role: 'developer',
        content: [
          {
            type: 'input_text',
            text: [
              'You are a live job-search and reranking engine for a premium resume matcher.',
              'You must search the live web for active job openings and not use any database job list.',
              'Infer the most likely target roles from the resume context, then search the web for matching jobs.',
              'Respect the requested country, region/city, work mode, and posted window when possible.',
              'Prefer official company careers pages or ATS/application pages. Avoid stale duplicates and closed roles when possible.',
              'Return only the JSON schema output. Keep premium_reason short and practical.',
            ].join(' '),
          },
        ],
      },
      {
        role: 'user',
        content: [
          {
            type: 'input_text',
            text: JSON.stringify(inputPayload),
          },
        ],
      },
    ],
    max_output_tokens: 3200,
    store: false,
  };

  const response = await fetch('https://api.openai.com/v1/responses', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(requestBody),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Premium web search failed.');
  }

  const data = await response.json();
  const rawText = extractOutputText(data);
  if (!rawText) {
    throw new Error('Premium web search returned no structured results.');
  }

  let parsed;
  try {
    parsed = JSON.parse(rawText);
  } catch {
    parsed = extractJsonObject(rawText);
  }

  const sources = extractSources(data);
  const results = Array.isArray(parsed?.results) ? parsed.results : [];
  const normalized = results
    .map((job, index) => normalizePremiumJob(job, index, requestedCountry, requestedWorkMode, sources))
    .filter((job) => job.title && job.best_url);

  return { results: normalized, sources };
}

function normalizePremiumJob(job, index, requestedCountry, requestedWorkMode, sources) {
  const title = clean(job?.title, 180);
  const company = clean(job?.company, 140) || 'Unknown company';
  const location = clean(job?.location, 160);
  const country = clean(job?.country, 80) || requestedCountry;
  const workMode = normalizeWorkMode(clean(job?.work_mode, 40) || requestedWorkMode || 'on-site');
  const bestUrl = clean(job?.best_url, 500) || sources?.[0]?.url || '';
  const descriptionText = clean(job?.description_text, 1600);
  const finalPct = clamp(Number(job?.final_match_percent || 0), 0, 100);
  const premiumReason = clean(job?.premium_reason, 140);
  const categoryKey = clean(job?.job_category_key, 120) || clean(job?.job_function, 120);
  const idSeed = `${title}-${company}-${bestUrl || index}`.toLowerCase();

  return {
    job_id: makeStableId(idSeed),
    title,
    company,
    location,
    country,
    region: inferRegion(location),
    work_mode: workMode,
    posted_date_display: clean(job?.posted_date_display, 60) || 'Recently posted',
    best_url: bestUrl,
    description_text: descriptionText,
    job_function: clean(job?.job_function, 120),
    job_domain: clean(job?.job_domain, 120),
    job_category_key: categoryKey,
    raw_match_percent: Math.round(finalPct),
    final_match_percent: Math.round(finalPct),
    premium_reason: premiumReason,
    penalty_applied: false,
    penalty_points: 0,
  };
}

function buildUserLocation(countryName, region) {
  const country = countryCode(countryName);
  const city = clean(region, 80);
  if (!country && !city) return null;
  const location = { type: 'approximate' };
  if (country) location.country = country;
  if (city) {
    location.city = city;
    location.region = city;
  }
  return location;
}

function countryCode(countryName) {
  const key = normalizeCountryName(countryName);
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

function normalizeCountryName(value) {
  return String(value || '').trim().toLowerCase();
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

function extractOutputText(data) {
  if (typeof data?.output_text === 'string' && data.output_text.trim()) {
    return data.output_text.trim();
  }
  const parts = [];
  const output = Array.isArray(data?.output) ? data.output : [];
  for (const item of output) {
    if (item?.type !== 'message' || !Array.isArray(item?.content)) continue;
    for (const block of item.content) {
      if (block?.type === 'output_text' && typeof block?.text === 'string') {
        parts.push(block.text);
      }
    }
  }
  return parts.join('\n').trim();
}

function extractJsonObject(text) {
  const raw = String(text || '').trim();
  if (!raw) return {};
  const match = raw.match(/\{[\s\S]*\}/);
  if (!match) return {};
  try {
    return JSON.parse(match[0]);
  } catch {
    return {};
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
        out.push({
          url,
          title: clean(source?.title, 180),
        });
      }
    }
  }
  return dedupeSources(out).slice(0, 12);
}

function dedupeSources(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const key = String(row?.url || '').trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(row);
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

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
    },
  });
}
