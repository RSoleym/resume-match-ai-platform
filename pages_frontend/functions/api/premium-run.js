const OPENAI_RESPONSES_URL = 'https://api.openai.com/v1/responses';
const OPENAI_CHAT_URL = 'https://api.openai.com/v1/chat/completions';
const DEFAULT_WEB_MODEL = 'gpt-5';
const DEFAULT_SCORING_MODEL = 'gpt-4o-mini';
const MAX_PREMIUM_SEARCHES = 3;
const TARGET_RESULTS = 5;
const MAX_FETCHED_PAGES = 8;

const COUNTRY_TO_ISO2 = {
  canada: 'CA',
  'united states': 'US',
  usa: 'US',
  us: 'US',
  'united kingdom': 'GB',
  uk: 'GB',
  ireland: 'IE',
  germany: 'DE',
  france: 'FR',
  netherlands: 'NL',
  israel: 'IL',
  india: 'IN',
  china: 'CN',
  japan: 'JP',
  korea: 'KR',
  singapore: 'SG',
  australia: 'AU',
  'new zealand': 'NZ',
  mexico: 'MX',
  brazil: 'BR',
  spain: 'ES',
  italy: 'IT',
  sweden: 'SE',
  norway: 'NO',
  denmark: 'DK',
  finland: 'FI',
  switzerland: 'CH',
  austria: 'AT',
  belgium: 'BE',
  portugal: 'PT',
  taiwan: 'TW',
  'hong kong': 'HK',
  malaysia: 'MY',
  'costa rica': 'CR',
  'czech republic': 'CZ',
  czechia: 'CZ',
};

const ISO2_TO_COUNTRY = Object.fromEntries(Object.entries(COUNTRY_TO_ISO2).map(([name, iso]) => [iso, titleCase(name)]));

const CATEGORY_ROLE_HINTS = {
  'Hardware / RTL / Verification': [
    'Design Verification Engineer',
    'RTL Design Engineer',
    'ASIC Verification Engineer',
    'FPGA Engineer',
    'Digital Design Engineer',
    'Hardware Verification Engineer',
  ],
  'Embedded / Firmware': [
    'Embedded Firmware Engineer',
    'Embedded Software Engineer',
    'Firmware Engineer',
    'Platform Software Engineer',
    'Board Bring-Up Engineer',
  ],
  'Software Engineering': ['Software Engineer', 'Backend Engineer', 'C++ Software Engineer', 'Python Engineer'],
  'Data / AI / ML': ['Machine Learning Engineer', 'AI Engineer', 'Data Scientist', 'NLP Engineer'],
  'Electrical / Power / Controls': ['Electrical Engineer', 'Controls Engineer', 'Power Systems Engineer'],
};

const CATEGORY_CORE_TERMS = {
  'Hardware / RTL / Verification': [
    'systemverilog', 'verilog', 'rtl', 'design verification', 'uvm', 'asic', 'fpga', 'digital design',
    'hardware verification', 'silicon', 'semiconductor', 'cpu', 'gpu', 'timing', 'formal', 'sva',
  ],
  'Embedded / Firmware': [
    'embedded', 'firmware', 'cortex-m', 'microcontroller', 'board bring-up', 'bare metal', 'device driver', 'c', 'c++',
  ],
};

const CATEGORY_AVOID_TERMS = {
  'Hardware / RTL / Verification': [
    'electromechanical', 'electro mechanical', 'mechanical', 'civil', 'construction', 'hvac', 'plc', 'scada',
    'field service', 'technician', 'sales', 'electrical designer', 'power systems',
  ],
  'Embedded / Firmware': ['mechanical', 'civil', 'sales', 'recruiter', 'accounting'],
};

const CRITICAL_SKILL_KEYWORDS = [
  'systemverilog', 'verilog', 'rtl', 'uvm', 'asic', 'fpga', 'embedded', 'firmware', 'cortex-m', 'python', 'c++', 'vivado',
];

const STATIC_JOB_DOMAINS = [
  'linkedin.com',
  'indeed.com',
  'jobbank.gc.ca',
  'boards.greenhouse.io',
  'greenhouse.io',
  'jobs.lever.co',
  'lever.co',
  'myworkdayjobs.com',
  'workdayjobs.com',
  'jobs.smartrecruiters.com',
  'smartrecruiters.com',
  'jobs.ashbyhq.com',
  'ashbyhq.com',
  'apply.workable.com',
  'wellfound.com',
  'builtin.com',
  'ziprecruiter.com',
  'monster.com',
];

const DIRECT_JOB_HOST_HINTS = [
  'greenhouse.io', 'lever.co', 'workdayjobs.com', 'myworkdayjobs.com', 'ashbyhq.com', 'smartrecruiters.com',
  'jobbank.gc.ca', 'linkedin.com', 'indeed.com', 'workable.com', 'wellfound.com',
];

const SEARCH_PAGE_HINTS = [
  '/jobs/search', '/job-search', '/search-jobs', '/findajob', '/jobsearch', '?keywords=', '?q=', '/careers', '/jobs?'
];

export async function onRequestPost(context) {
  const supabaseUrl = String(context.env.SUPABASE_URL || '').trim();
  const anonKey = String(context.env.SUPABASE_ANON_KEY || context.env.SUPABASE_PUBLISHABLE_KEY || '').trim();
  const secretKey = String(context.env.SUPABASE_SECRET_KEY || context.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
  const openAiKey = String(context.env.OPENAI_API_KEY || context.env.OPENAI_KEY || '').trim();
  const webModel = String(context.env.OPENAI_WEB_MODEL || DEFAULT_WEB_MODEL).trim();
  const scoringModel = String(context.env.OPENAI_MODEL || DEFAULT_SCORING_MODEL).trim();

  if (!supabaseUrl || !anonKey || !secretKey || !openAiKey) {
    return json({ error: 'Missing premium backend configuration.' }, 500);
  }

  const startedAt = Date.now();

  try {
    const token = readBearer(context.request);
    const user = await getAuthedUser({ supabaseUrl, anonKey, token });
    const body = await context.request.json().catch(() => ({}));
    const resumeId = clean(body?.resumeId, 120);
    const resumeContext = normalizeResumeContext(body?.resumeContext || {});
    const filters = normalizeFilters(body?.filters || {});

    if (!resumeId) return json({ error: 'Resume ID is required.' }, 400);
    if (!resumeContext.resume_text) {
      return json({ error: 'Premium needs parsed resume text first. Run the free/browser pipeline once.' }, 400);
    }

    const profileState = await getProfile({ supabaseUrl, secretKey, userId: user.id });
    const premiumUnlocked = !!profileState?.premium_access || !!profileState?.premium_admin_access;
    if (!premiumUnlocked) return json({ error: 'Premium is still locked for this account.' }, 403);

    const searchesUsed = Number(profileState?.premium_searches_used || 0);
    const isAdmin = !!profileState?.premium_admin_access;
    if (!isAdmin && searchesUsed >= MAX_PREMIUM_SEARCHES) {
      return json({ error: 'No premium searches remaining.' }, 403);
    }

    const searchStartedAt = Date.now();
    const liveRows = await searchLiveJobsWithOpenAI({
      apiKey: openAiKey,
      webModel,
      scoringModel,
      resumeContext,
      filters,
      maxResults: TARGET_RESULTS,
    });
    const liveSearchMs = Date.now() - searchStartedAt;

    if (!liveRows.length) {
      throw new Error('Premium live web search could not find usable job results.');
    }

    const results = buildPremiumLiveResultRows(resumeId, liveRows, webModel);

    if (!isAdmin) {
      await patchProfile({
        supabaseUrl,
        secretKey,
        userId: user.id,
        patch: {
          premium_searches_used: searchesUsed + 1,
          premium_last_run_at: new Date().toISOString(),
        },
      });
    }

    return json({
      ok: true,
      results,
      filters: { ...filters, source: 'live_web_search', openai_model: webModel },
      timings: {
        total_ms: Date.now() - startedAt,
        live_search_ms: liveSearchMs,
      },
      used: isAdmin ? searchesUsed : searchesUsed + 1,
      remaining: isAdmin ? MAX_PREMIUM_SEARCHES : Math.max(0, MAX_PREMIUM_SEARCHES - (searchesUsed + 1)),
    });
  } catch (error) {
    return json({ error: cleanError(error) }, 500);
  }
}

function normalizeResumeContext(input) {
  const ctx = input && typeof input === 'object' ? input : {};
  return {
    candidate_country: clean(ctx.candidate_country, 120),
    candidate_experience_years: finiteOrNull(ctx.candidate_experience_years),
    candidate_degree_level: clean(ctx.candidate_degree_level, 80) || 'none',
    candidate_degree_family: clean(ctx.candidate_degree_family, 120) || 'General',
    candidate_degree_fields: Array.isArray(ctx.candidate_degree_fields) ? ctx.candidate_degree_fields.map((x) => clean(x, 120)).filter(Boolean) : [],
    candidate_category: clean(ctx.candidate_category, 120) || clean(ctx.candidate_category_key, 120) || 'General',
    candidate_category_key: clean(ctx.candidate_category_key, 120),
    candidate_function: clean(ctx.candidate_function, 120),
    candidate_function_scores: ctx.candidate_function_scores && typeof ctx.candidate_function_scores === 'object' ? ctx.candidate_function_scores : {},
    candidate_domain: clean(ctx.candidate_domain, 120),
    candidate_domain_scores: ctx.candidate_domain_scores && typeof ctx.candidate_domain_scores === 'object' ? ctx.candidate_domain_scores : {},
    summary: clean(ctx.summary, 1400),
    education: clean(ctx.education, 1000),
    skills: clean(ctx.skills, 1600),
    experience: clean(ctx.experience, 1800),
    projects: clean(ctx.projects, 1200),
    resume_text: clean(ctx.resume_text || ctx.parsed_text, 6000),
  };
}

function normalizeFilters(input) {
  const raw = input && typeof input === 'object' ? input : {};
  return {
    country: normalizeCountryName(clean(raw.country, 120)),
    region: clean(raw.region || raw.city || raw.location, 120),
    workMode: canonicalizeWorkMode(clean(raw.workMode || raw.work_mode, 40)),
    posted: canonicalizePostedRange(clean(raw.posted || raw.posted_range, 40) || 'all'),
  };
}

async function searchLiveJobsWithOpenAI({ apiKey, webModel, scoringModel, resumeContext, filters, maxResults }) {
  const target = clamp(Number(maxResults || TARGET_RESULTS), 1, TARGET_RESULTS);
  const attempts = buildSearchAttempts(resumeContext, filters).slice(0, 3);
  const merged = new Map();

  for (const attempt of attempts) {
    if (merged.size >= Math.max(target, 6)) break;

    let rawHits = [];
    try {
      rawHits = await performSearchAttempt({ apiKey, webModel, attempt });
    } catch {
      rawHits = [];
    }
    if (!rawHits.length) continue;

    const normalizedHits = normalizeSearchHits(rawHits, attempt);
    if (!normalizedHits.length) continue;

    const enriched = await enrichHitsWithPages(normalizedHits, attempt);
    const filtered = filterAndRankRows(enriched, resumeContext, attempt.filters);

    for (const row of filtered) {
      const key = String(row.url || row.job_id || '').trim().toLowerCase();
      if (!key || merged.has(key)) continue;
      merged.set(key, row);
      if (merged.size >= Math.max(target, 8)) break;
    }
  }

  let rows = Array.from(merged.values()).sort(compareRows).slice(0, Math.max(target, 8));
  if (!rows.length) return [];

  try {
    const scored = await scoreJobsWithOpenAI({
      apiKey,
      model: scoringModel || DEFAULT_SCORING_MODEL,
      resumeContext,
      jobs: rows.slice(0, Math.max(target, 6)),
    });
    const scoreMap = new Map(scored.map((item) => [String(item.job_id || '').trim(), item]));
    rows = rows.map((row) => {
      const hit = scoreMap.get(String(row.job_id || '').trim());
      if (!hit) return row;
      return {
        ...row,
        match_percentage: clamp(Number(hit.match_percentage || row.match_percentage || 0), 0, 100),
        reason: clean(hit.reason, 160) || row.reason || 'Found from live web search',
      };
    });
  } catch {
    // keep heuristic scores
  }

  return rows
    .sort((a, b) => (Number(b.match_percentage || 0) - Number(a.match_percentage || 0)) || compareRows(a, b))
    .slice(0, target);
}

function buildSearchAttempts(resumeContext, filters) {
  const profile = deriveResumeSearchProfile(resumeContext);
  const roleBatches = [];
  const titles = profile.role_titles.length ? profile.role_titles : ['resume matched engineering jobs'];
  for (let i = 0; i < titles.length && roleBatches.length < 3; i += 2) {
    roleBatches.push(titles.slice(i, i + 2));
  }
  if (!roleBatches.length) roleBatches.push(['resume matched engineering jobs']);

  const plans = [
    { ...filters },
    { ...filters, region: '' },
    { ...filters, region: '', posted: 'all' },
  ];

  const out = [];
  const seen = new Set();
  for (const plan of plans) {
    for (const focusTitles of roleBatches) {
      const key = JSON.stringify({ plan, focusTitles });
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ filters: plan, focusTitles, profile });
    }
  }
  return out;
}

async function performSearchAttempt({ apiKey, webModel, attempt }) {
  const tool = {
    type: 'web_search',
    search_context_size: 'medium',
    filters: {
      allowed_domains: allowedDomainsForAttempt(attempt),
    },
  };
  const location = buildResponsesUserLocation(attempt.filters.country, attempt.filters.region);
  if (location) tool.user_location = location;

  const payload = {
    model: webModel,
    tools: [tool],
    tool_choice: 'required',
    include: ['web_search_call.action.sources', 'web_search_call.results'],
    input: buildSearchPrompt(attempt),
    max_output_tokens: 900,
    store: false,
  };
  if (/^(gpt-5|o3|o4)/i.test(webModel)) payload.reasoning = { effort: 'low' };

  const data = await requestOpenAIJson({
    url: OPENAI_RESPONSES_URL,
    apiKey,
    payload,
    attempts: 2,
  });

  return collectSearchCandidates(data);
}

function buildSearchPrompt(attempt) {
  const { filters, focusTitles, profile } = attempt;
  const titles = uniqueKeepOrder([...(focusTitles || []), ...(profile.role_titles || [])], 6);
  const keywords = uniqueKeepOrder(profile.keywords || [], 10);
  const avoid = uniqueKeepOrder(profile.negative_terms || [], 8);

  return [
    `Use live web search to find current direct job postings for these role titles: ${titles.join(', ') || 'best matching engineering jobs'}.`,
    `Technical keywords: ${keywords.join(', ') || 'resume matched skills'}.`,
    avoid.length ? `Avoid unrelated job families: ${avoid.join(', ')}.` : '',
    `Country filter: ${filters.country || 'any'}.`,
    filters.region ? `City or region filter: ${filters.region}.` : 'City or region filter: any.',
    `Work mode filter: ${filters.workMode || 'any'}.`,
    `Posted range: ${filters.posted || 'all'}.`,
    'Prefer direct ATS pages or direct job-detail pages on major job boards.',
    'Do not prefer company homepages, career homepages, blog posts, or generic search result pages.',
    'Return the best current openings only.',
  ].filter(Boolean).join(' ');
}

function allowedDomainsForAttempt(attempt) {
  const set = new Set(STATIC_JOB_DOMAINS);
  const profile = attempt.profile || {};
  for (const title of profile.role_titles || []) {
    const low = String(title).toLowerCase();
    if (low.includes('verification') || low.includes('rtl') || low.includes('fpga')) {
      for (const domain of ['amd.com', 'nvidia.com', 'qualcomm.com', 'arm.com', 'synopsys.com', 'cadence.com']) set.add(domain);
    }
    if (low.includes('embedded') || low.includes('firmware')) {
      for (const domain of ['microchip.com', 'nxp.com', 'stmicroelectronics.com']) set.add(domain);
    }
  }
  return Array.from(set).slice(0, 30);
}

function collectSearchCandidates(data) {
  const out = [];
  const seen = new Set();

  function add(item) {
    const url = clean(item?.url || item?.link, 500);
    if (!url) return;
    const key = url.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push({
      url,
      title: clean(item?.title || item?.name || item?.headline, 240),
      snippet: clean(item?.snippet || item?.description || item?.summary || item?.text, 1000),
      source_type: clean(item?.type || item?.source_type, 80),
    });
  }

  function walk(node) {
    if (!node) return;
    if (Array.isArray(node)) {
      for (const item of node) walk(item);
      return;
    }
    if (typeof node !== 'object') return;

    if (node.type === 'url_citation') {
      const uc = node.url_citation && typeof node.url_citation === 'object' ? node.url_citation : node;
      add({ url: uc.url, title: uc.title });
    }
    if (Array.isArray(node.sources)) {
      for (const item of node.sources) add(item);
    }
    if (Array.isArray(node.results)) {
      for (const item of node.results) add(item);
    }
    if ((node.url || node.link) && (node.title || node.name || node.headline || node.snippet || node.description)) {
      add(node);
    }

    for (const value of Object.values(node)) walk(value);
  }

  walk(data);

  const outputText = extractTextFromResponsesPayload(data);
  for (const url of extractUrlsFromText(outputText)) add({ url });
  for (const item of extractJsonArray(outputText)) add(item);

  return out;
}

function normalizeSearchHits(hits, attempt) {
  const rows = [];
  for (const hit of Array.isArray(hits) ? hits : []) {
    const url = clean(hit.url, 500);
    if (!url) continue;
    const host = hostFromUrl(url);
    const title = clean(hit.title, 220);
    const snippet = clean(hit.snippet, 1200);
    const [splitTitle, splitCompany] = splitPageTitle(title);
    const inferredTitle = clean(splitTitle || title || humanizePath(url), 220);
    const inferredCompany = clean(splitCompany || host.split('.')[0]?.replace(/[-_]+/g, ' '), 180);
    rows.push({
      job_id: `WEB-${simpleHash(url)}`,
      title: inferredTitle,
      company: inferredCompany ? titleCase(inferredCompany) : '',
      url,
      source_url: url,
      location: clean(extractLocationFromText(snippet), 180),
      country: normalizeCountryName(guessCountryFromText(snippet)),
      work_mode: canonicalizeWorkMode(inferWorkMode(inferredTitle, '', snippet)) || '',
      posted_date: extractPostedDateFromText(snippet),
      description_text: snippet,
      page_text: snippet,
      reason: 'Found from live web search',
      search_model: '',
      relevance_score: 0,
      match_percentage: 0,
      host,
    });
  }
  return rows;
}

async function enrichHitsWithPages(rows, attempt) {
  const out = [];
  let fetched = 0;
  for (const row of rows) {
    let enriched = { ...row };
    const shouldFetch = fetched < MAX_FETCHED_PAGES && looksFetchableJobUrl(row.url);
    if (shouldFetch) {
      fetched += 1;
      const meta = await fetchJobPageMetadata(row.url).catch(() => ({}));
      const mergedText = `${meta.location || ''} ${meta.description_text || ''} ${meta.page_text || ''}`;
      enriched = {
        ...enriched,
        title: clean(meta.title || enriched.title, 220),
        company: clean(meta.company || enriched.company, 180),
        location: clean(meta.location || enriched.location || extractLocationFromText(mergedText), 180),
        country: normalizeCountryName(clean(meta.country || guessCountryFromText(mergedText) || enriched.country, 120)),
        work_mode: canonicalizeWorkMode(meta.work_mode || enriched.work_mode || inferWorkMode(meta.title || enriched.title, meta.location || '', meta.page_text || meta.description_text || '')),
        posted_date: clean(meta.posted_date || extractPostedDateFromText(mergedText) || enriched.posted_date, 80),
        description_text: clean(meta.description_text || enriched.description_text, 1800),
        page_text: clean(meta.page_text || meta.description_text || enriched.page_text || enriched.description_text, 2600),
      };
    }
    if (!enriched.title) continue;
    if (!enriched.company) enriched.company = titleCase(clean(hostFromUrl(enriched.url).split('.')[0]?.replace(/[-_]+/g, ' '), 180));
    out.push(enriched);
  }
  return out;
}

function filterAndRankRows(rows, resumeContext, filters) {
  const profile = deriveResumeSearchProfile(resumeContext);
  const output = [];
  const seen = new Set();

  for (const row of rows) {
    const key = String(row.url || row.job_id || '').trim().toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);

    const title = clean(row.title, 220);
    const descriptionText = clean(row.description_text, 1800);
    const pageText = clean(row.page_text || descriptionText, 2600);
    const location = clean(row.location, 180);
    const country = normalizeCountryName(clean(row.country || guessCountryFromText(`${location} ${pageText.slice(0, 200)}`), 120));
    const workMode = canonicalizeWorkMode(row.work_mode || inferWorkMode(title, location, pageText));
    const postedDate = clean(row.posted_date, 80);

    const signalText = `${location} ${title} ${descriptionText.slice(0, 700)} ${pageText.slice(0, 1200)}`;
    const countryOk = !filters.country || country === filters.country || (!country && textMentionsCountry(signalText, filters.country));
    const regionOk = !filters.region || locationQueryMatch(filters.region, signalText);
    const workOk = !filters.workMode || workMode === filters.workMode || !workMode;
    const postedOk = !postedDate || postedDate === 'Unknown' || dateFilterMatch(postedDate, filters.posted);

    const relevance = jobRelevanceScore(profile, { title, descriptionText, pageText });
    const boosted = relevance + (looksLikeDirectJobUrl(row.url) ? 1.2 : 0) + (countryOk ? 0.4 : -1.4) + (regionOk ? 0.3 : -1.2) + (workOk ? 0.2 : -0.6);

    if (titleTooGeneric(title, pageText)) continue;
    if (!countryOk) continue;
    if (!regionOk) continue;
    if (!workOk) continue;
    if (!postedOk && filters.posted !== 'all') continue;

    output.push({
      ...row,
      title,
      location,
      country,
      work_mode: workMode || 'On-site',
      posted_date: postedDate || 'Unknown',
      description_text: descriptionText,
      page_text: pageText,
      relevance_score: boosted,
      match_percentage: relevanceToPercent(boosted),
      reason: clean(row.reason, 160) || 'Found from live web search',
    });
  }

  return output.sort(compareRows).slice(0, 8);
}

async function scoreJobsWithOpenAI({ apiKey, model, resumeContext, jobs }) {
  const payload = {
    resume: {
      country: clean(resumeContext.candidate_country, 80),
      experience_years: resumeContext.candidate_experience_years,
      degree_level: clean(resumeContext.candidate_degree_level, 80),
      degree_family: clean(resumeContext.candidate_degree_family, 120),
      degree_fields: resumeContext.candidate_degree_fields || [],
      category: clean(resumeContext.candidate_category || resumeContext.candidate_category_key, 120),
      function: clean(resumeContext.candidate_function, 120),
      domain: clean(resumeContext.candidate_domain, 120),
      summary: clean(resumeContext.summary, 900),
      skills: clean(resumeContext.skills, 1200),
      experience: clean(resumeContext.experience, 1200),
      projects: clean(resumeContext.projects, 900),
      resume_text_excerpt: clean(resumeContext.resume_text, 2000),
    },
    jobs: jobs.map((job) => ({
      job_id: clean(job.job_id, 180),
      title: clean(job.title, 180),
      company: clean(job.company, 140),
      location: clean(job.location, 140),
      country: clean(job.country, 80),
      work_mode: clean(job.work_mode, 40),
      posted_date: clean(job.posted_date, 40),
      description_excerpt: clean(job.description_text || job.page_text, 1100),
      local_score: Number(job.match_percentage || 0),
    })),
    rules: {
      output_schema: {
        scores: [{ job_id: 'string', match_percentage: 'number 0-100', reason: 'short string <= 18 words' }],
      },
      must_return_all_job_ids: true,
      json_only: true,
    },
  };

  const data = await requestOpenAIJson({
    url: OPENAI_CHAT_URL,
    apiKey,
    payload: {
      model,
      temperature: 0.2,
      messages: [
        { role: 'system', content: 'You are a strict resume-to-job scoring assistant. Score every provided job from 0 to 100 for overall fit. Return only JSON.' },
        { role: 'user', content: JSON.stringify(payload) },
      ],
      max_completion_tokens: 1200,
    },
    attempts: 2,
  });

  const content = String(data?.choices?.[0]?.message?.content || '');
  const parsed = extractJsonObject(content);
  const scores = Array.isArray(parsed?.scores) ? parsed.scores : [];
  return scores.map((item) => ({
    job_id: clean(item.job_id, 180),
    match_percentage: clamp(Number(item.match_percentage || 0), 0, 100),
    reason: clean(item.reason, 120),
  })).filter((item) => item.job_id);
}

function buildPremiumLiveResultRows(resumeId, liveRows, premiumModel) {
  const rows = liveRows.map((job, index) => {
    const score = clamp(Number(job.match_percentage || 0), 0, 100);
    const location = clean(job.location, 180);
    const country = normalizeCountryName(clean(job.country || guessCountryFromText(`${location} ${job.description_text || ''}`), 120));
    return {
      resume_id: resumeId,
      job_id: clean(job.job_id || job.url || `WEB-${String(index + 1).padStart(5, '0')}`, 260),
      rank: index + 1,
      title: clean(job.title, 220),
      company: clean(job.company, 180),
      location,
      country,
      work_mode: canonicalizeWorkMode(job.work_mode || inferWorkMode(job.title, location, job.description_text)),
      job_category: clean(job.job_category || job.job_function || 'General', 120),
      raw_match_percent: Number(score.toFixed(2)),
      final_match_percent: Number(score.toFixed(2)),
      penalty_applied: false,
      url: clean(job.source_url || job.url, 500),
      posted_date: clean(job.posted_date, 80),
      posted_date_display: clean(job.posted_date, 80) || 'Unknown',
      premium_reason: clean(job.reason, 160),
      premium_model: clean(job.search_model || premiumModel, 120),
      prefilter_score: Number(job.relevance_score || 0),
    };
  });
  rows.sort((a, b) => (Number(b.final_match_percent || 0) - Number(a.final_match_percent || 0)) || (Number(b.prefilter_score || 0) - Number(a.prefilter_score || 0)));
  rows.forEach((row, idx) => { row.rank = idx + 1; });
  return rows.slice(0, TARGET_RESULTS);
}

function deriveResumeSearchProfile(resumeContext) {
  const category = clean(resumeContext.candidate_category || resumeContext.candidate_category_key || 'General', 120);
  const functionName = clean(resumeContext.candidate_function, 120);
  const domainName = clean(resumeContext.candidate_domain, 120);
  const textBlob = [
    clean(resumeContext.summary, 1200),
    clean(resumeContext.skills, 1400),
    clean(resumeContext.experience, 1400),
    clean(resumeContext.projects, 1200),
    clean(resumeContext.education, 800),
    clean(resumeContext.resume_text, 2600),
  ].join(' ').toLowerCase();

  const roleTitles = [...(CATEGORY_ROLE_HINTS[category] || [])];
  for (const [token, title] of [
    ['design verification', 'Design Verification Engineer'],
    ['uvm', 'ASIC Verification Engineer'],
    ['systemverilog', 'Design Verification Engineer'],
    ['rtl', 'RTL Design Engineer'],
    ['verilog', 'RTL Design Engineer'],
    ['fpga', 'FPGA Engineer'],
    ['embedded', 'Embedded Firmware Engineer'],
    ['firmware', 'Firmware Engineer'],
    ['cortex-m', 'Embedded Firmware Engineer'],
    ['board bring-up', 'Embedded Firmware Engineer'],
  ]) {
    if (textBlob.includes(token)) roleTitles.unshift(title);
  }
  if (functionName) roleTitles.unshift(functionName);
  if (domainName) roleTitles.push(domainName);

  let keywords = cleanSkillTerms(resumeContext.skills, 18);
  for (const token of CRITICAL_SKILL_KEYWORDS) if (textBlob.includes(token)) keywords.push(token);
  for (const token of CATEGORY_CORE_TERMS[category] || []) if (textBlob.includes(token)) keywords.push(token);

  return {
    category,
    function: functionName,
    domain: domainName,
    role_titles: uniqueKeepOrder(roleTitles, 8),
    keywords: uniqueKeepOrder(keywords, 18),
    negative_terms: uniqueKeepOrder(CATEGORY_AVOID_TERMS[category] || [], 10),
  };
}

function cleanSkillTerms(skillsText, limit = 14) {
  const raw = clean(skillsText, 1200);
  if (!raw) return [];
  const out = [];
  const seen = new Set();
  for (const part of raw.split(/[,;/|\n]/g)) {
    const token = String(part || '').replace(/\s+/g, ' ').trim().replace(/^[-\s]+|[-\s]+$/g, '');
    if (token.length < 2) continue;
    const low = token.toLowerCase();
    if (seen.has(low)) continue;
    seen.add(low);
    out.push(token);
    if (out.length >= limit) break;
  }
  return out;
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

function jobRelevanceScore(profile, { title, descriptionText, pageText }) {
  const blob = [clean(title, 220), clean(descriptionText, 2000), clean(pageText, 2500)].join(' ').toLowerCase();
  let score = 0;
  for (const role of profile.role_titles || []) {
    if (blob.includes(String(role).toLowerCase())) score += 4.5;
  }
  for (const keyword of profile.keywords || []) {
    const kw = String(keyword).toLowerCase().trim();
    if (!kw) continue;
    if (blob.includes(kw)) score += kw.includes(' ') ? 2.2 : 1.4;
  }
  for (const neg of profile.negative_terms || []) {
    const bad = String(neg).toLowerCase().trim();
    if (bad && blob.includes(bad)) score -= 3.2;
  }
  if (profile.category === 'Hardware / RTL / Verification') {
    if (['systemverilog', 'verilog', 'rtl', 'design verification', 'uvm', 'asic', 'fpga', 'digital design'].some((x) => blob.includes(x))) score += 5;
    if (['electrical designer', 'electromechanical', 'mechanical engineer', 'controls engineer', 'power systems'].some((x) => blob.includes(x))) score -= 5.5;
  }
  if (profile.category === 'Embedded / Firmware') {
    if (['embedded', 'firmware', 'cortex-m', 'microcontroller', 'bare metal'].some((x) => blob.includes(x))) score += 4.5;
  }
  return score;
}

function relevanceToPercent(score) {
  return clamp(Math.round(42 + Number(score || 0) * 5.2), 5, 99);
}

async function fetchJobPageMetadata(url) {
  const response = await fetch(url, {
    headers: {
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'User-Agent': 'Mozilla/5.0',
    },
    redirect: 'follow',
  });
  if (!response.ok) return {};
  const html = await response.text();
  const jsonLd = extractJobPostingFieldsFromJsonLd(html);
  const title = clean(jsonLd.title || extractMeta(html, 'property', 'og:title') || extractMeta(html, 'name', 'og:title') || extractTitleTag(html), 220);
  const descriptionText = clean(jsonLd.description_text || extractMeta(html, 'name', 'description') || extractMeta(html, 'property', 'og:description') || '', 1800);
  const pageText = clean(stripHtml(html), 2600);
  const location = clean(jsonLd.location, 220);
  return {
    title,
    company: clean(jsonLd.company, 180),
    location,
    country: normalizeCountryName(clean(jsonLd.country || guessCountryFromText(location || pageText.slice(0, 300)), 120)),
    work_mode: canonicalizeWorkMode(clean(jsonLd.work_mode, 80)),
    posted_date: parseRelativePostedDate(jsonLd.posted_date),
    description_text: descriptionText || pageText,
    page_text: pageText,
  };
}

function extractJobPostingFieldsFromJsonLd(html) {
  const scripts = [...String(html || '').matchAll(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)].map((m) => m[1]);
  for (const script of scripts) {
    const values = parsePossibleJson(script);
    for (const node of flattenJsonLd(values)) {
      const typeValue = Array.isArray(node['@type']) ? node['@type'].join(' ') : String(node['@type'] || '');
      if (!/JobPosting/i.test(typeValue)) continue;
      const hiringOrganization = node.hiringOrganization && typeof node.hiringOrganization === 'object' ? node.hiringOrganization : {};
      const jobLocation = Array.isArray(node.jobLocation) ? node.jobLocation[0] : node.jobLocation;
      const address = jobLocation && typeof jobLocation === 'object' ? (jobLocation.address && typeof jobLocation.address === 'object' ? jobLocation.address : {}) : {};
      const countryRaw = address.addressCountry || node.applicantLocationRequirements?.address?.addressCountry || '';
      const locality = address.addressLocality || '';
      const region = address.addressRegion || '';
      const location = [locality, region, normalizeCountryName(countryRaw)].filter(Boolean).join(', ');
      return {
        title: clean(node.title || node.name, 220),
        description_text: clean(stripHtml(String(node.description || '')), 1800),
        company: clean(hiringOrganization.name, 180),
        location,
        country: normalizeCountryName(countryRaw),
        work_mode: /telecommute|remote/i.test(String(node.jobLocationType || '')) ? 'Remote' : '',
        posted_date: clean(node.datePosted, 80),
      };
    }
  }
  return {};
}

function parsePossibleJson(raw) {
  const text = String(raw || '').trim();
  if (!text) return [];
  try {
    return [JSON.parse(text)];
  } catch {
    return [];
  }
}

function flattenJsonLd(values) {
  const out = [];
  const stack = [...values];
  while (stack.length) {
    const node = stack.pop();
    if (!node) continue;
    if (Array.isArray(node)) {
      stack.push(...node);
      continue;
    }
    if (typeof node !== 'object') continue;
    out.push(node);
    if (Array.isArray(node['@graph'])) stack.push(...node['@graph']);
  }
  return out;
}

function extractMeta(html, attrName, attrValue) {
  const rx = new RegExp(`<meta[^>]*${attrName}=["']${escapeRegex(attrValue)}["'][^>]*content=["']([\\s\\S]*?)["'][^>]*>`, 'i');
  const match = String(html || '').match(rx);
  return decodeHtml(match?.[1] || '');
}

function extractTitleTag(html) {
  const match = String(html || '').match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return decodeHtml(match?.[1] || '');
}

function stripHtml(html) {
  return decodeHtml(String(html || '').replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ').replace(/<[^>]+>/g, ' '));
}

function decodeHtml(text) {
  return String(text || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>');
}

function extractTextFromResponsesPayload(data) {
  if (typeof data?.output_text === 'string' && data.output_text.trim()) return data.output_text.trim();
  const parts = [];
  for (const item of data?.output || []) {
    if (!item || typeof item !== 'object') continue;
    if (item.type === 'message' && Array.isArray(item.content)) {
      for (const part of item.content) {
        if (!part || typeof part !== 'object') continue;
        if (typeof part.text === 'string') parts.push(part.text);
      }
    }
  }
  return parts.join('\n').trim();
}

function extractJsonArray(text) {
  const raw = String(text || '').trim();
  if (!raw) return [];
  try {
    const value = JSON.parse(raw);
    if (Array.isArray(value)) return value.filter((x) => x && typeof x === 'object');
    if (value && typeof value === 'object') {
      const jobs = value.jobs || value.results || value.suitable_jobs;
      if (Array.isArray(jobs)) return jobs.filter((x) => x && typeof x === 'object');
    }
  } catch {}
  const match = raw.match(/\[(?:.|\n|\r)*\]/);
  if (!match) return [];
  try {
    const value = JSON.parse(match[0]);
    return Array.isArray(value) ? value.filter((x) => x && typeof x === 'object') : [];
  } catch {
    return [];
  }
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

function extractUrlsFromText(text) {
  const matches = String(text || '').match(/https?:\/\/[^\s)\]>"']+/g) || [];
  return matches.map((x) => String(x).replace(/[.,;]+$/g, '')).filter(Boolean);
}

function normalizeCountryName(country) {
  const raw = clean(country, 120);
  if (!raw) return '';
  if (/^[A-Za-z]{2}$/.test(raw)) return ISO2_TO_COUNTRY[raw.toUpperCase()] || raw.toUpperCase();
  return titleCase(raw);
}

function canonicalizeWorkMode(value) {
  const low = clean(value, 40).toLowerCase();
  if (!low) return '';
  if (low.includes('remote')) return 'Remote';
  if (low.includes('hybrid')) return 'Hybrid';
  if (low.includes('on-site') || low.includes('onsite') || low.includes('on site')) return 'On-site';
  return '';
}

function canonicalizePostedRange(value) {
  const low = clean(value, 40).toLowerCase();
  if (!low || low === 'all') return 'all';
  if (['24h', 'today', 'day'].includes(low)) return '24h';
  if (['week', 'past week', '7d'].includes(low)) return 'week';
  if (['month', 'past month', '30d'].includes(low)) return 'month';
  return 'all';
}

function buildResponsesUserLocation(country, region) {
  const countryCode = toIso2(country);
  if (!countryCode && !region) return null;
  return {
    type: 'approximate',
    country: countryCode || undefined,
    city: region || undefined,
  };
}

function toIso2(country) {
  const raw = clean(country, 120);
  if (!raw) return '';
  if (/^[A-Za-z]{2}$/.test(raw)) return raw.toUpperCase();
  return COUNTRY_TO_ISO2[raw.toLowerCase()] || '';
}

function titleCase(text) {
  return String(text || '').toLowerCase().replace(/\b\w/g, (m) => m.toUpperCase());
}

function splitPageTitle(title) {
  const parts = String(title || '').split(/\s+[\-|–|•]\s+/).map((x) => clean(x, 220)).filter(Boolean);
  if (parts.length >= 2) return [parts[0], parts[1]];
  return [clean(title, 220), ''];
}

function humanizePath(url) {
  try {
    const path = new URL(url).pathname.split('/').filter(Boolean).pop() || '';
    return decodeURIComponent(path).replace(/[-_]+/g, ' ');
  } catch {
    return '';
  }
}

function hostFromUrl(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, '');
  } catch {
    return '';
  }
}

function looksFetchableJobUrl(url) {
  const host = hostFromUrl(url);
  if (!host) return false;
  if (DIRECT_JOB_HOST_HINTS.some((x) => host.includes(x))) return true;
  const path = safePath(url);
  return /job|position|career|vacanc|opening|opportunit|requisition|req-|posting/i.test(path);
}

function looksLikeDirectJobUrl(url) {
  const host = hostFromUrl(url);
  const path = safePath(url);
  if (DIRECT_JOB_HOST_HINTS.some((x) => host.includes(x))) return true;
  return /\/jobs?\/view|\/job\/|jobposting|requisition|reqid|gh_jid|lever\.co\/.+\/jobs\//i.test(path);
}

function safePath(url) {
  try { return new URL(url).pathname + new URL(url).search; } catch { return url || ''; }
}

function titleTooGeneric(title, pageText) {
  const low = `${title} ${pageText || ''}`.toLowerCase();
  return [
    'browse jobs', 'job search', 'search results', 'all jobs', 'career opportunities', 'careers at',
  ].some((x) => low.includes(x));
}

function locationQueryMatch(query, location) {
  const q = clean(query, 120).toLowerCase();
  const loc = clean(location, 220).toLowerCase();
  return !q || loc.includes(q);
}

function guessCountryFromText(text) {
  const low = clean(text, 1200).toLowerCase();
  if (!low) return '';
  for (const [name] of Object.entries(COUNTRY_TO_ISO2)) {
    if (low.includes(name)) return titleCase(name);
  }
  if (['toronto', 'ontario', 'vancouver', 'british columbia', 'montreal', 'quebec', 'ottawa', 'calgary', 'edmonton', 'waterloo', 'kitchener', 'mississauga', 'canada'].some((x) => low.includes(x))) return 'Canada';
  if (['united states', 'usa', 'us', 'new york', 'california', 'texas', 'washington', 'massachusetts', 'seattle', 'san francisco', 'austin'].some((x) => low.includes(x))) return 'United States';
  if (['united kingdom', 'uk', 'great britain', 'england', 'scotland', 'wales', 'northern ireland', 'bristol', 'london', 'manchester', 'cambridge', 'oxford', 'glasgow', 'edinburgh'].some((x) => low.includes(x))) return 'United Kingdom';
  return '';
}

function textMentionsCountry(text, expectedCountry) {
  const blob = clean(text, 1600).toLowerCase();
  const country = normalizeCountryName(expectedCountry).toLowerCase();
  if (!blob || !country) return false;
  if (blob.includes(country)) return true;
  if (country === 'canada') return ['canada', 'ontario', 'british columbia', 'alberta', 'quebec', 'toronto', 'vancouver', 'montreal', 'ottawa', 'calgary', 'waterloo'].some((x) => blob.includes(x));
  if (country === 'united states') return ['united states', 'usa', 'u.s.', 'california', 'texas', 'new york', 'washington', 'seattle', 'austin', 'san francisco'].some((x) => blob.includes(x));
  if (country === 'united kingdom') return ['united kingdom', 'uk', 'great britain', 'england', 'scotland', 'wales', 'northern ireland', 'bristol', 'london', 'manchester', 'cambridge', 'oxford'].some((x) => blob.includes(x));
  return false;
}

function extractPostedDateFromText(text) {
  const raw = clean(text, 500).toLowerCase();
  if (!raw) return '';
  const rel = raw.match(/(\d+)\s+(hour|hours|day|days|week|weeks)\s+ago/);
  if (rel) return parseRelativePostedDate(`${rel[1]} ${rel[2]}`);
  if (raw.includes('today') || raw.includes('just posted')) return parseRelativePostedDate('today');
  if (raw.includes('yesterday')) return parseRelativePostedDate('yesterday');
  const iso = raw.match(/\b(20\d{2}-\d{2}-\d{2})\b/);
  if (iso) return iso[1];
  return '';
}

function extractLocationFromText(text) {
  const raw = clean(text, 600);
  if (!raw) return '';
  const patterns = [
    /(?:location|based in)\s*[:\-]\s*([^|•
]+)/i,
    /\b([A-Z][A-Za-z .'-]+,\s*(?:ON|BC|QC|AB|NS|MB|SK|NB|NL|PE|Canada|United States|USA|United Kingdom|UK))\b/,
    /\b([A-Z][A-Za-z .'-]+,\s*[A-Z][A-Za-z .'-]+)\b/,
  ];
  for (const rx of patterns) {
    const match = raw.match(rx);
    const value = clean(match?.[1] || '', 180);
    if (value) return value;
  }
  return '';
}


function inferWorkMode(title, location, text) {
  const low = `${title || ''} ${location || ''} ${text || ''}`.toLowerCase();
  if (low.includes('remote')) return 'Remote';
  if (low.includes('hybrid')) return 'Hybrid';
  return 'On-site';
}

function parseRelativePostedDate(value) {
  const raw = clean(value, 80).toLowerCase();
  if (!raw) return '';
  const now = Date.now();
  const match = raw.match(/(\d+)\s+(day|days|hour|hours|week|weeks)/);
  if (match) {
    const amount = Number(match[1] || 0);
    const unit = match[2] || '';
    const ms = unit.startsWith('hour') ? amount * 3600000 : unit.startsWith('week') ? amount * 7 * 86400000 : amount * 86400000;
    return new Date(now - ms).toISOString().slice(0, 10);
  }
  if (raw.includes('today') || raw.includes('just posted')) return new Date(now).toISOString().slice(0, 10);
  if (raw.includes('yesterday')) return new Date(now - 86400000).toISOString().slice(0, 10);
  return clean(value, 80);
}

function dateFilterMatch(postedDate, filterValue) {
  const mode = canonicalizePostedRange(filterValue);
  if (mode === 'all' || !postedDate || postedDate === 'Unknown') return true;
  const parsed = Date.parse(postedDate);
  if (!Number.isFinite(parsed)) return true;
  const ageDays = (Date.now() - parsed) / 86400000;
  if (mode === '24h') return ageDays <= 1.2;
  if (mode === 'week') return ageDays <= 7.2;
  if (mode === 'month') return ageDays <= 31.5;
  return true;
}

function compareRows(a, b) {
  return (Number(b.relevance_score || 0) - Number(a.relevance_score || 0)) || String(a.title || '').localeCompare(String(b.title || ''));
}

function postedLabelFromFilter(value) {
  const low = clean(value, 40).toLowerCase();
  if (!low || low === 'all') return 'Unknown';
  if (low === '24h') return 'Past 24 hours';
  if (low === 'week') return 'Past week';
  if (low === 'month') return 'Past month';
  return titleCase(low);
}

function simpleHash(value) {
  let hash = 0;
  const text = String(value || '');
  for (let i = 0; i < text.length; i += 1) hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
  return String(Math.abs(hash));
}

function escapeRegex(text) {
  return String(text || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function finiteOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function clean(value, maxChars = 1000) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxChars);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, Number(value || 0)));
}

function cleanError(error) {
  if (!error) return 'Unexpected premium backend error.';
  if (typeof error === 'string') return clean(error, 800);
  return clean(error.message || error.error || 'Unexpected premium backend error.', 800);
}

async function requestOpenAIJson({ url, apiKey, payload, attempts = 2 }) {
  let lastMessage = 'OpenAI request failed.';
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(payload),
    });

    if (response.ok) return response.json();

    const text = await response.text().catch(() => '');
    lastMessage = text || `OpenAI request failed (${response.status}).`;

    if (response.status === 429 && attempt < attempts) {
      await sleep(1200 * attempt);
      continue;
    }
    throw new Error(lastMessage);
  }
  throw new Error(lastMessage);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
    const message = await response.text().catch(() => '');
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
