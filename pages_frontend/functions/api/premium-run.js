const MAX_PREMIUM_SEARCHES = 3;
const DEFAULT_RERANK_MODEL = 'gpt-4o-mini';
const DEFAULT_SEARCH_MODEL = 'gpt-4o-search-preview';
const TARGET_RESULTS = 12;

const CATEGORY_ROLE_HINTS = {
  'Hardware / RTL / Verification': [
    'Design Verification Engineer',
    'ASIC Verification Engineer',
    'RTL Design Engineer',
    'FPGA Engineer',
    'Digital Design Engineer',
  ],
  'Embedded / Firmware': [
    'Embedded Firmware Engineer',
    'Firmware Engineer',
    'Embedded Software Engineer',
    'Board Bring-Up Engineer',
  ],
  'Software Engineering': ['Software Engineer', 'Backend Engineer', 'C++ Software Engineer', 'Python Engineer'],
  'Data / AI / ML': ['Machine Learning Engineer', 'AI Engineer', 'Data Scientist'],
};

export async function onRequestPost(context) {
  const supabaseUrl = readEnv(context.env, ['SUPABASE_URL']);
  const anonKey = readEnv(context.env, ['SUPABASE_ANON_KEY', 'SUPABASE_PUBLISHABLE_KEY']);
  const secretKey = readEnv(context.env, ['SUPABASE_SECRET_KEY', 'SUPABASE_SERVICE_ROLE_KEY', 'SUPABASE_SERVICE_KEY']);
  const openAiKey = readEnv(context.env, ['OPENAI_API_KEY', 'OPENAI_KEY']);
  const rerankModel = readEnv(context.env, ['OPENAI_MODEL', 'OPENAI_DEFAULT_MODEL']) || DEFAULT_RERANK_MODEL;
  const searchModel = readEnv(context.env, ['OPENAI_WEB_CHAT_MODEL', 'OPENAI_SEARCH_MODEL']) || DEFAULT_SEARCH_MODEL;

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
    if (!clean(resumeContext?.resume_text, 120)) {
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
      searchModel,
      rerankModel,
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
    return json({ error: error?.message || 'Unexpected premium backend error.' }, 500);
  }
}

async function searchLiveJobsWithOpenAI({ apiKey, searchModel, rerankModel, resumeContext, filters }) {
  const requestedCountry = clean(filters?.country || resumeContext?.candidate_country || '', 80);
  const requestedRegion = clean(filters?.region || '', 80);
  const requestedWorkMode = clean(filters?.workMode || '', 40);
  const postedWindow = normalizePosted(filters?.posted || 'all');

  const attempts = buildBroadeningPlan({
    country: requestedCountry,
    region: requestedRegion,
    workMode: requestedWorkMode,
    posted: postedWindow,
  });
  const titleBatches = makeFocusTitleBatches(resumeContext);
  const merged = new Map();
  const sourceMap = new Map();

  for (const attempt of attempts) {
    if (merged.size >= TARGET_RESULTS) break;
    for (const titles of titleBatches) {
      if (merged.size >= TARGET_RESULTS) break;
      const { jobs, sources } = await callChatSearch({
        apiKey,
        model: searchModel,
        resumeContext,
        filters: attempt,
        focusTitles: titles,
        excludeUrls: Array.from(sourceMap.keys()),
      });
      addSources(sourceMap, sources);
      addJobs(merged, jobs, attempt);
    }
  }

  if (!merged.size) {
    const fallback = await callResponsesSearch({
      apiKey,
      model: rerankModel,
      resumeContext,
      filters: attempts[0],
      focusTitles: titleBatches[0] || [],
      excludeUrls: [],
    });
    addSources(sourceMap, fallback.sources);
    addJobs(merged, fallback.jobs, attempts[0]);
  }

  if (!merged.size && sourceMap.size) {
    for (const source of sourceMap.values()) {
      const row = normalizeSourceFallback(source, { country: requestedCountry, workMode: requestedWorkMode });
      if (row?.best_url) merged.set(row.best_url, row);
      if (merged.size >= TARGET_RESULTS) break;
    }
  }

  const candidates = Array.from(merged.values()).slice(0, Math.max(TARGET_RESULTS, 18));
  if (!candidates.length) {
    throw new Error('Premium live web search could not find usable job results.');
  }

  const reranked = await rerankJobsWithOpenAI({
    apiKey,
    model: rerankModel,
    resumeContext,
    jobs: candidates,
  });

  return {
    results: reranked.slice(0, TARGET_RESULTS),
    sources: Array.from(sourceMap.values()).slice(0, TARGET_RESULTS),
  };
}

async function callChatSearch({ apiKey, model, resumeContext, filters, focusTitles, excludeUrls }) {
  const prompt = buildSearchPrompt({ resumeContext, filters, focusTitles, excludeUrls, maxResults: 6 });
  const webSearchOptions = { search_context_size: 'low' };
  const userLocation = buildChatUserLocation(filters?.country, filters?.region);
  if (userLocation) webSearchOptions.user_location = userLocation;

  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      messages: [
        {
          role: 'developer',
          content: 'Find direct live job detail pages only. Do not use any database jobs. Use the resume-derived roles and filters. Avoid generic listings. Return only JSON with a top-level jobs array.',
        },
        { role: 'user', content: prompt },
      ],
      web_search_options: webSearchOptions,
      max_completion_tokens: 1800,
      temperature: 0.2,
    }),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Premium web chat search failed.');
  }

  const data = await response.json().catch(() => ({}));
  const { text, sources } = extractChatTextAndSources(data);
  const jobs = normalizeSearchResults(extractJsonArray(text), filters, sources);
  return { jobs, sources };
}

async function callResponsesSearch({ apiKey, model, resumeContext, filters, focusTitles, excludeUrls }) {
  const prompt = buildSearchPrompt({ resumeContext, filters, focusTitles, excludeUrls, maxResults: 8 });
  const tool = { type: 'web_search' };
  const userLocation = buildResponsesUserLocation(filters?.country, filters?.region);
  if (userLocation) tool.user_location = userLocation;

  const body = {
    model,
    tools: [tool],
    tool_choice: 'auto',
    include: ['web_search_call.action.sources'],
    input: prompt,
    max_output_tokens: 2200,
    store: false,
  };
  if (['gpt-5', 'o4-mini', 'o3'].includes(model)) {
    body.reasoning = { effort: 'low' };
  }

  const response = await fetch('https://api.openai.com/v1/responses', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Premium responses web search failed.');
  }

  const data = await response.json().catch(() => ({}));
  const text = extractOutputText(data);
  const sources = extractSources(data);
  const jobs = normalizeSearchResults(extractJsonArray(text), filters, sources);
  return { jobs, sources };
}

async function rerankJobsWithOpenAI({ apiKey, model, resumeContext, jobs }) {
  const payload = {
    resume: {
      country: clean(resumeContext.candidate_country, 80),
      experience_years: resumeContext.candidate_experience_years ?? null,
      degree_level: clean(resumeContext.candidate_degree_level, 60),
      degree_family: clean(resumeContext.candidate_degree_family, 120),
      degree_fields: Array.isArray(resumeContext.candidate_degree_fields) ? resumeContext.candidate_degree_fields.slice(0, 8) : [],
      function: clean(resumeContext.candidate_function, 120),
      domain: clean(resumeContext.candidate_domain, 120),
      category: clean(resumeContext.candidate_category_key, 120),
      resume_text_excerpt: clean(resumeContext.resume_text, 3000),
    },
    jobs: jobs.map((job) => ({
      job_id: clean(job.job_id, 180),
      title: clean(job.title, 180),
      company: clean(job.company, 140),
      location: clean(job.location, 160),
      country: clean(job.country, 80),
      work_mode: clean(job.work_mode, 40),
      posted_date: clean(job.posted_date_display || job.posted_date, 60),
      description_excerpt: clean(job.description_text, 1200),
      discovered_via_live_web: true,
    })),
    rules: {
      score_range: '0 to 100',
      keep_all_job_ids: true,
      return_json_only: true,
      output_schema: {
        results: [{ job_id: 'string', premium_score: 'number', premium_reason: 'short string <= 18 words' }],
      },
    },
  };

  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      temperature: 0.2,
      messages: [
        {
          role: 'system',
          content: 'You are a strict resume-to-job reranker. Score every job for fit. Return only JSON.',
        },
        {
          role: 'user',
          content: JSON.stringify(payload),
        },
      ],
      max_completion_tokens: 2200,
    }),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Premium rerank failed.');
  }

  const data = await response.json().catch(() => ({}));
  const text = extractChatText(data);
  const parsed = extractJsonObject(text);
  const returned = Array.isArray(parsed?.results) ? parsed.results : [];
  const byId = new Map(returned.map((item) => [clean(item?.job_id, 180), item]));

  return jobs
    .map((job) => {
      const hit = byId.get(clean(job.job_id, 180)) || {};
      const premiumScore = clamp(Number(hit?.premium_score ?? job.final_match_percent ?? 0), 0, 100);
      return {
        ...job,
        raw_match_percent: Math.round(Number(job.final_match_percent || job.raw_match_percent || 0)),
        final_match_percent: Math.round(premiumScore),
        premium_reason: clean(hit?.premium_reason || job.premium_reason, 140),
      };
    })
    .sort((a, b) => (b.final_match_percent || 0) - (a.final_match_percent || 0));
}

function buildSearchPrompt({ resumeContext, filters, focusTitles, excludeUrls, maxResults }) {
  const profile = deriveResumeSearchProfile(resumeContext);
  const requestedTitles = uniqueKeepOrder([...(focusTitles || []), ...(profile.role_titles || [])], 8);
  const keywords = uniqueKeepOrder(profile.keywords || [], 16);
  const negative = uniqueKeepOrder(profile.negative_terms || [], 10);

  return JSON.stringify({
    candidate: {
      category: clean(resumeContext.candidate_category_key, 120),
      function: clean(resumeContext.candidate_function, 120),
      domain: clean(resumeContext.candidate_domain, 120),
      target_titles: requestedTitles,
      skill_keywords: keywords,
      negative_terms: negative,
      resume_text_excerpt: clean(resumeContext.resume_text, 2500),
    },
    filters: {
      country: clean(filters?.country, 80),
      region: clean(filters?.region, 80),
      work_mode: clean(filters?.workMode, 40),
      posted_window: clean(filters?.posted, 60),
      exclude_urls: Array.isArray(excludeUrls) ? excludeUrls.slice(0, 20) : [],
    },
    instructions: {
      live_web_only: true,
      no_database_jobs: true,
      direct_job_pages_only: true,
      max_results: Math.max(4, Number(maxResults || 6)),
      prefer_company_or_ats_pages: true,
      return_shape: {
        jobs: [
          {
            title: 'string',
            company: 'string',
            location: 'string',
            country: 'string',
            work_mode: 'string',
            posted_date: 'string',
            description_text: 'string',
            url: 'string',
            job_function: 'string',
            job_domain: 'string',
            job_category_key: 'string',
          },
        ],
      },
      json_only: true,
    },
  });
}

function deriveResumeSearchProfile(resumeContext) {
  const category = clean(resumeContext?.candidate_category_key, 120);
  const functionName = clean(resumeContext?.candidate_function, 120);
  const domainName = clean(resumeContext?.candidate_domain, 120);
  const textBlob = [
    clean(resumeContext?.summary, 1200),
    clean(resumeContext?.skills, 1400),
    clean(resumeContext?.experience, 1400),
    clean(resumeContext?.projects, 1200),
    clean(resumeContext?.education, 900),
    clean(resumeContext?.resume_text, 2600),
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
    const token = clean(piece, 60);
    if (token) keywords.push(token);
  }
  for (const token of ['systemverilog', 'verilog', 'rtl', 'uvm', 'asic', 'fpga', 'embedded', 'firmware', 'python', 'c++', 'vivado']) {
    if (textBlob.includes(token)) keywords.push(token);
  }

  const negative = [];
  if (category === 'Hardware / RTL / Verification') {
    negative.push('mechanical', 'civil', 'construction', 'power systems', 'controls engineer');
  }

  return {
    role_titles: uniqueKeepOrder(roles, 8),
    keywords: uniqueKeepOrder(keywords, 16),
    negative_terms: uniqueKeepOrder(negative, 10),
  };
}

function makeFocusTitleBatches(resumeContext) {
  const titles = deriveResumeSearchProfile(resumeContext).role_titles || [];
  if (!titles.length) return [['']];
  const batches = [];
  for (let i = 0; i < titles.length; i += 2) {
    batches.push(titles.slice(i, i + 2));
  }
  if (titles[0]) batches.push([titles[0]]);
  return batches.slice(0, 4);
}

function buildBroadeningPlan({ country, region, workMode, posted }) {
  const plan = [
    { country, region, workMode, posted },
    { country, region: '', workMode, posted },
    { country, region: '', workMode: '', posted },
    { country, region: '', workMode: '', posted: 'any recent time' },
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
      description_text: clean(item.description_text || item.description || '', 1600),
      job_function: clean(item.job_function, 120),
      job_domain: clean(item.job_domain, 120),
      job_category_key: clean(item.job_category_key || item.job_category, 120),
      raw_match_percent: 0,
      final_match_percent: 0,
      premium_reason: '',
      penalty_applied: false,
      penalty_points: 0,
    });
  }
  return dedupeJobs(rows);
}

function normalizeSourceFallback(source, filters) {
  const url = clean(source?.url, 500);
  const title = clean(source?.title, 180);
  if (!url || !title) return null;
  const company = clean(inferCompanyFromTitle(title), 140) || 'Unknown company';
  return {
    job_id: makeStableId(`${title}-${company}-${url}`),
    title,
    company,
    location: '',
    country: clean(filters?.country, 80),
    region: '',
    work_mode: normalizeWorkMode(filters?.workMode || 'on-site'),
    posted_date_display: 'Recently posted',
    best_url: url,
    description_text: '',
    job_function: '',
    job_domain: '',
    job_category_key: '',
    raw_match_percent: 0,
    final_match_percent: 0,
    premium_reason: '',
    penalty_applied: false,
    penalty_points: 0,
  };
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
      if (block?.type === 'output_text' && typeof block?.text === 'string') {
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
    if (targetMap.size >= TARGET_RESULTS * 2) break;
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
  const out = { type: 'approximate' };
  if (country) out.country = country;
  if (locality) {
    out.city = locality;
    out.region = locality;
  }
  return out;
}

function buildResponsesUserLocation(countryName, city) {
  const country = countryCode(countryName);
  const locality = clean(city, 80);
  if (!country && !locality) return null;
  const out = { type: 'approximate' };
  if (country) out.country = country;
  if (locality) out.city = locality;
  return out;
}

function countryCode(countryName) {
  const key = String(countryName || '').trim().toLowerCase();
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

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
    },
  });
}
