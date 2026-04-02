const OPENAI_CHAT_URL = 'https://api.openai.com/v1/chat/completions';
const OPENAI_RESPONSES_URL = 'https://api.openai.com/v1/responses';
const DEFAULT_SCORING_MODEL = 'gpt-4o-mini';
const DEFAULT_WEB_MODEL = 'gpt-5.4-mini';
const MAX_PREMIUM_SEARCHES = 3;
const TARGET_RESULTS = 5;

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

const DIRECT_JOB_HOST_ALLOWLIST_HINTS = [
  'greenhouse.io', 'lever.co', 'workdayjobs.com', 'myworkdayjobs.com', 'ashbyhq.com', 'smartrecruiters.com',
];

export async function onRequestPost(context) {
  const supabaseUrl = String(context.env.SUPABASE_URL || '').trim();
  const anonKey = String(context.env.SUPABASE_ANON_KEY || context.env.SUPABASE_PUBLISHABLE_KEY || '').trim();
  const secretKey = String(context.env.SUPABASE_SECRET_KEY || context.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
  const openAiKey = String(context.env.OPENAI_API_KEY || context.env.OPENAI_KEY || '').trim();
  const scoringModel = String(context.env.OPENAI_MODEL || DEFAULT_SCORING_MODEL).trim();
  const webModel = normalizeWebModel(String(context.env.OPENAI_WEB_MODEL || DEFAULT_WEB_MODEL).trim());
  const chatSearchModel = normalizeChatSearchModel(String(context.env.OPENAI_WEB_CHAT_MODEL || '').trim());

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
    if (!resumeContext.resume_text) return json({ error: 'Premium needs parsed resume text first. Run the free/browser pipeline once.' }, 400);

    const profile = await getProfile({ supabaseUrl, secretKey, userId: user.id });
    const premiumUnlocked = !!profile?.premium_access || !!profile?.premium_admin_access;
    if (!premiumUnlocked) return json({ error: 'Premium is still locked for this account.' }, 403);

    const searchesUsed = Number(profile?.premium_searches_used || 0);
    const isAdmin = !!profile?.premium_admin_access;
    if (!isAdmin && searchesUsed >= MAX_PREMIUM_SEARCHES) {
      return json({ error: 'No premium searches remaining.' }, 403);
    }

    const searchStart = Date.now();
    const liveRows = await searchLiveJobsWithOpenAI({
      apiKey: openAiKey,
      webModel,
      chatSearchModel,
      scoringModel,
      resumeContext,
      filters,
      maxResults: TARGET_RESULTS,
      userIdentifier: user.id,
    });
    const searchMs = Date.now() - searchStart;

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
        live_search_ms: searchMs,
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
    region: clean(raw.region, 120),
    workMode: canonicalizeWorkMode(clean(raw.workMode || raw.work_mode, 40)),
    posted: canonicalizePostedRange(clean(raw.posted || raw.posted_range, 40) || 'all'),
  };
}

async function searchLiveJobsWithOpenAI({ apiKey, webModel, resumeContext, filters, maxResults }) {
  const target = Math.max(1, Math.min(Number(maxResults || TARGET_RESULTS), TARGET_RESULTS));
  const profile = deriveResumeSearchProfile(resumeContext);
  const merged = new Map();
  const attempts = broadeningPlan(filters);

  for (const attemptFilters of attempts) {
    if (merged.size >= target) break;
    const requestedCount = Math.max(3, Math.min(target - merged.size, target));
    const jobs = await callOpenAILiveSearchOnce({
      apiKey,
      webModel,
      resumeContext,
      filters: attemptFilters,
      requestedCount,
    }).catch(() => []);

    const normalized = jobs
      .map((item) => normalizeLiveJobRow(item))
      .filter((row) => row && row.url && row.title);

    const filtered = filterLiveRows(normalized, attemptFilters);
    const selected = (filtered.length ? filtered : normalized)
      .map((row) => {
        const relevance = Number.isFinite(Number(row.relevance_score))
          ? Number(row.relevance_score)
          : jobRelevanceScore(profile, {
              title: row.title,
              descriptionText: row.description_text,
              pageText: row.page_text || row.description_text,
            });
        const pct = clamp(Number(row.match_percentage || 0) || relevanceToPercent(relevance), 0, 100);
        return {
          ...row,
          relevance_score: relevance,
          match_percentage: pct,
          reason: clean(row.reason, 160) || 'Live web match',
          search_model: clean(row.search_model, 120) || webModel,
        };
      })
      .sort((a, b) => (Number(b.match_percentage || 0) - Number(a.match_percentage || 0)) || compareRows(a, b));

    for (const row of selected) {
      const key = String(row.url || row.job_id || '').trim().toLowerCase();
      if (!key || merged.has(key)) continue;
      merged.set(key, row);
      if (merged.size >= target) break;
    }
  }

  return [...merged.values()]
    .sort((a, b) => (Number(b.match_percentage || 0) - Number(a.match_percentage || 0)) || compareRows(a, b))
    .slice(0, target);
}


async function callOpenAILiveSearchOnce({ apiKey, webModel, resumeContext, filters, requestedCount }) {
  const toolEntry = {
    type: 'web_search',
    search_context_size: 'medium',
  };
  const userLocation = buildResponsesUserLocation(filters.country, filters.region);
  if (userLocation) toolEntry.user_location = userLocation;

  const strictPrompt = makeCompactLiveSearchPrompt({
    resumeContext,
    filters,
    requestedCount,
  });

  const strictPayload = {
    model: webModel,
    tools: [toolEntry],
    tool_choice: 'auto',
    include: ['web_search_call.action.sources'],
    input: strictPrompt,
    text: {
      format: {
        type: 'json_schema',
        name: 'premium_live_jobs',
        strict: true,
        schema: buildLiveJobsSchema(requestedCount),
      },
    },
    max_output_tokens: 1200,
    reasoning: { effort: 'low' },
    store: false,
  };

  let data = await requestOpenAIJson({
    url: OPENAI_RESPONSES_URL,
    apiKey,
    payload: strictPayload,
    attempts: 2,
  }).catch(() => null);

  let jobs = extractJobsFromResponsesPayload(data, webModel);
  if (jobs.length) return jobs;

  const fallbackPayload = {
    model: webModel,
    tools: [toolEntry],
    tool_choice: 'auto',
    include: ['web_search_call.action.sources'],
    input: `${strictPrompt}

If there are only a few good matches, return fewer jobs instead of none. Return ONLY compact JSON with a top-level jobs array.`,
    max_output_tokens: 1000,
    reasoning: { effort: 'low' },
    store: false,
  };

  data = await requestOpenAIJson({
    url: OPENAI_RESPONSES_URL,
    apiKey,
    payload: fallbackPayload,
    attempts: 1,
  }).catch(() => null);

  jobs = extractJobsFromResponsesPayload(data, webModel);
  if (jobs.length) return jobs;

  const sourceRows = sourceRowsFromSearchSources({
    sources: collectSourceUrls(data),
    filters,
    resumeContext,
    searchModel: webModel,
    requestedCount,
  });
  if (sourceRows.length) return sourceRows;

  return [];
}



async function chatSearchForSourceRows({ apiKey, chatSearchModel, resumeContext, filters, requestedCount, excludeUrls, focusTitles }) {
  const payload = {
    model: chatSearchModel,
    messages: [
      { role: 'developer', content: 'Find direct live job-detail pages only. Ignore generic list pages and return only the best direct sources.' },
      {
        role: 'user',
        content: makeLiveSourcesPrompt({ resumeContext, filters, requestedCount, excludeUrls, focusTitles }),
      },
    ],
    web_search_options: { search_context_size: 'low' },
    max_completion_tokens: 900,
  };
  const chatLocation = buildChatUserLocation(filters.country, filters.region);
  if (chatLocation) payload.web_search_options.user_location = chatLocation;

  const data = await requestOpenAIJson({
    url: OPENAI_CHAT_URL,
    apiKey,
    payload,
    attempts: 1,
  });

  const content = String(data?.choices?.[0]?.message?.content || '');
  const annotationSources = extractChatAnnotations(data);
  const rows = await sourceRowsToJobs({
    sources: annotationSources.concat(extractUrlsFromText(content)),
    filters,
    resumeContext,
    searchModel: chatSearchModel,
  });
  if (rows.length) return rows;
  return extractJsonArray(content).map((item) => ({ ...item, search_model: chatSearchModel }));
}

function sourceRowsFromSearchSources({ sources, filters, resumeContext, searchModel, requestedCount }) {
  const profile = deriveResumeSearchProfile(resumeContext);
  const rows = [];
  const seen = new Set();

  for (const source of Array.isArray(sources) ? sources : []) {
    if (rows.length >= Math.max(1, Number(requestedCount || TARGET_RESULTS))) break;
    const url = clean(source?.url, 500);
    if (!url || seen.has(url)) continue;
    seen.add(url);
    if (isGenericCareersUrl(url)) continue;

    const titleText = clean(source?.title, 240);
    const [splitTitle, splitCompany] = splitPageTitle(titleText);
    const title = clean(splitTitle || titleText, 220);
    if (!title || looksLikeSearchResultPage(url, title, titleText)) continue;

    const host = hostFromUrl(url).replace(/^www\./i, '');
    const company = clean(splitCompany || host.split('.')[0]?.replace(/[-_]+/g, ' '), 180);
    const location = clean([filters.region, filters.country].filter(Boolean).join(', '), 180);
    const country = normalizeCountryName(clean(filters.country || guessCountryFromText(location || titleText), 120));
    const descriptionText = clean(titleText, 900);
    const relevance = jobRelevanceScore(profile, {
      title,
      descriptionText,
      pageText: `${title} ${company} ${location}`,
    });

    rows.push({
      job_id: `WEB-${simpleHash(url)}`,
      title,
      company: company ? titleCase(company) : '',
      url,
      source_url: url,
      location,
      country,
      work_mode: canonicalizeWorkMode(filters.workMode || inferWorkMode(title, location, titleText)) || 'On-site',
      posted_date: postedLabelFromFilter(filters.posted),
      description_text: descriptionText,
      reason: 'Found from live web search',
      search_model: searchModel,
      match_percentage: relevanceToPercent(relevance),
      relevance_score: relevance,
    });
  }

  return rows
    .filter((row) => row.url && row.title)
    .sort((a, b) => (Number(b.match_percentage || 0) - Number(a.match_percentage || 0)) || compareRows(a, b));
}


async function fetchJobPageMetadata(url) {
  try {
    const response = await fetch(url, {
      headers: { Accept: 'text/html,application/xhtml+xml' },
      redirect: 'follow',
    });
    if (!response.ok) return {};
    const html = await response.text();
    const ld = extractJobPostingFieldsFromJsonLd(html);
    const title = clean(ld.title || extractMeta(html, 'property', 'og:title') || extractMeta(html, 'name', 'og:title') || extractTitleTag(html), 220);
    const desc = clean(ld.description_text || extractMeta(html, 'name', 'description') || extractMeta(html, 'property', 'og:description'), 1800);
    const text = clean(stripHtml(html), 2600);
    const location = clean(ld.location, 220);
    const country = normalizeCountryName(clean(ld.country || guessCountryFromText(location || text.slice(0, 300)), 120));
    return {
      title,
      company: clean(ld.company, 180),
      location,
      country,
      work_mode: canonicalizeWorkMode(clean(ld.work_mode, 80)),
      posted_date: parseRelativePostedDate(ld.posted_date),
      description_text: desc || text,
      page_text: text,
    };
  } catch {
    return {};
  }
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
        title: clean(node.title, 220),
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

function compareRows(a, b) {
  return (Number(b.relevance_score || 0) - Number(a.relevance_score || 0)) || String(a.title || '').localeCompare(String(b.title || ''));
}

function broadeningPlan(filters) {
  const plan = [
    { ...filters },
    { ...filters, region: '' },
    { ...filters, region: '', workMode: '' },
    { ...filters, region: '', workMode: '', posted: 'all' },
  ];
  const seen = new Set();
  return plan.filter((item) => {
    const key = JSON.stringify(item);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function makeFocusTitleBatches(resumeContext) {
  const titles = deriveResumeSearchProfile(resumeContext).role_titles;
  if (!titles.length) return [['']];
  const batches = [];
  for (let i = 0; i < titles.length; i += 2) {
    batches.push(titles.slice(i, i + 2));
  }
  batches.push([titles[0]]);
  return batches.slice(0, 5);
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

  const roleTitles = [];
  for (const role of CATEGORY_ROLE_HINTS[category] || []) roleTitles.push(role);

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
  for (const token of CRITICAL_SKILL_KEYWORDS) {
    if (textBlob.includes(token)) keywords.push(token);
  }
  for (const token of CATEGORY_CORE_TERMS[category] || []) {
    if (textBlob.includes(token)) keywords.push(token);
  }

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
  const raw = clean(skillsText, 1000);
  if (!raw) return [];
  const items = [];
  const seen = new Set();
  for (const part of raw.split(/[,;/|\n]/g)) {
    const token = part.replace(/\s+/g, ' ').trim().replace(/^[-\s]+|[-\s]+$/g, '');
    if (token.length < 2) continue;
    const low = token.toLowerCase();
    if (seen.has(low)) continue;
    seen.add(low);
    items.push(token);
    if (items.length >= limit) break;
  }
  return items;
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
    if (role && blob.includes(String(role).toLowerCase())) score += 4.5;
  }
  for (const keyword of profile.keywords || []) {
    const kw = String(keyword).toLowerCase().trim();
    if (!kw) continue;
    score += blob.includes(kw) ? (kw.includes(' ') ? 2.2 : 1.4) : 0;
  }
  for (const neg of profile.negative_terms || []) {
    const bad = String(neg).toLowerCase().trim();
    if (bad && blob.includes(bad)) score -= 3.2;
  }
  if (profile.category === 'Hardware / RTL / Verification') {
    if (['systemverilog', 'verilog', 'rtl', 'design verification', 'uvm', 'asic', 'fpga', 'digital design'].some((term) => blob.includes(term))) score += 5;
    if (['electrical designer', 'electromechanical', 'mechanical engineer', 'controls engineer', 'power systems'].some((term) => blob.includes(term))) score -= 5.5;
  }
  if (profile.category === 'Embedded / Firmware') {
    if (['embedded', 'firmware', 'cortex-m', 'microcontroller', 'bare metal'].some((term) => blob.includes(term))) score += 4.5;
  }
  return score;
}

function looksLikeSearchResultPage(url, title, pageText) {
  const lowUrl = String(url || '').toLowerCase();
  const lowTitle = clean(title, 220).toLowerCase();
  const lowText = clean(pageText, 1800).toLowerCase();
  if (DIRECT_JOB_HOST_ALLOWLIST_HINTS.some((hint) => lowUrl.includes(hint))) return false;
  if (['search - job bank', 'jobs and work opportunities', 'discover ', 'browse jobs', 'job search', 'search results', 'all jobs'].some((pattern) => lowTitle.includes(pattern))) return true;
  const host = hostFromUrl(url);
  if (host.includes('linkedin') && lowUrl.includes('/jobs/search')) return true;
  if (host.includes('indeed') && /\/jobs|jobs\?/.test(lowUrl)) return true;
  return lowText.includes('search results') && lowText.includes('jobs');
}

function isGenericCareersUrl(url) {
  const low = String(url || '').toLowerCase().trim();
  if (!low) return true;
  const good = ['/job/', 'jobid=', 'job_id=', 'gh_jid=', 'requisition', 'req=', 'reqid=', '/positions/', '/jobs/view/', '/vacancy/', '/posting/', '/opportunity/'];
  if (good.some((x) => low.includes(x))) return false;
  const bad = ['/careers/', '/jobs/', '/job-search', '/search-jobs'];
  return bad.some((x) => low.includes(x));
}

function splitPageTitle(rawTitle) {
  let title = clean(rawTitle, 220);
  if (!title) return ['', ''];
  const low = title.toLowerCase();
  for (const token of [' job details', ' careers', ' career', ' jobs', ' job opening', ' application']) {
    if (low.endsWith(token)) {
      title = title.slice(0, -token.length).trim().replace(/[\-\|—–]+$/g, '').trim();
      break;
    }
  }
  for (const sep of [' | ', ' — ', ' – ', ' - ']) {
    if (title.includes(sep)) {
      const [left, right] = title.split(sep, 2);
      return [clean(left, 180), clean(right, 180)];
    }
  }
  const match = title.match(/^(.+?)\s+at\s+(.+)$/i);
  if (match) return [clean(match[1], 180), clean(match[2], 180)];
  return [title, ''];
}

function guessCountryFromText(locationText) {
  const low = clean(locationText, 220).toLowerCase();
  if (!low) return '';
  for (const [name, iso] of Object.entries(COUNTRY_TO_ISO2)) {
    if (low.includes(name)) return titleCase(name.length > 2 ? name : ISO2_TO_COUNTRY[iso] || iso);
  }
  if (['toronto', 'markham', 'ontario', ', on', 'canada'].some((x) => low.includes(x))) return 'Canada';
  if (['united states', ', ca', ', tx', ', ny', ', wa'].some((x) => low.includes(x))) return 'United States';
  return '';
}

function inferWorkMode(title, location, description) {
  const blob = `${title || ''} ${location || ''} ${description || ''}`.toLowerCase();
  if (blob.includes('hybrid')) return 'Hybrid';
  if (blob.includes('remote')) return 'Remote';
  return 'On-site';
}

function canonicalizeWorkMode(value) {
  const low = clean(value, 40).toLowerCase();
  if (!low) return '';
  if (low.includes('hybrid')) return 'Hybrid';
  if (low.includes('remote')) return 'Remote';
  if (low.includes('site') || low.includes('office') || low.includes('onsite') || low.includes('on-site')) return 'On-site';
  if (low === 'all work modes') return '';
  return titleCase(low);
}

function canonicalizePostedRange(value) {
  const low = clean(value, 40).toLowerCase();
  if (!low || low === 'all' || low === 'any time') return 'all';
  if (low.includes('24')) return '24h';
  if (low.includes('week')) return 'week';
  if (low.includes('month')) return 'month';
  return low;
}

function normalizeCountryName(value) {
  const raw = clean(value, 120);
  if (!raw) return '';
  const low = raw.toLowerCase();
  if (COUNTRY_TO_ISO2[low]) return titleCase(low);
  const iso = raw.toUpperCase();
  if (ISO2_TO_COUNTRY[iso]) return ISO2_TO_COUNTRY[iso];
  return raw;
}

function parseRelativePostedDate(value) {
  const raw = clean(value, 120);
  if (!raw) return '';
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0, 10);
  const low = raw.toLowerCase();
  if (low.includes('today') || low.includes('just posted') || low.includes('few hours') || low.includes('hour')) return new Date().toISOString().slice(0, 10);
  const dayMatch = low.match(/(\d+)\s+day/);
  const weekMatch = low.match(/(\d+)\s+week/);
  const monthMatch = low.match(/(\d+)\s+month/);
  let days = null;
  if (dayMatch) days = Number(dayMatch[1]);
  if (weekMatch) days = Number(weekMatch[1]) * 7;
  if (monthMatch) days = Number(monthMatch[1]) * 30;
  if (days != null && Number.isFinite(days)) {
    const dt = new Date(Date.now() - days * 86400000);
    return dt.toISOString().slice(0, 10);
  }
  return raw;
}

function dateFilterMatch(postedValue, postedRange) {
  const range = canonicalizePostedRange(postedRange);
  if (!postedValue || range === 'all') return true;
  const parsed = parseRelativePostedDate(postedValue);
  const dt = new Date(parsed);
  if (Number.isNaN(dt.getTime())) return true;
  const ageMs = Date.now() - dt.getTime();
  if (range === '24h') return ageMs <= 86400000 * 1.25;
  if (range === 'week') return ageMs <= 86400000 * 8;
  if (range === 'month') return ageMs <= 86400000 * 31;
  return true;
}

function locationQueryMatch(query, inferredRegion, location) {
  const q = clean(query, 120).toLowerCase();
  if (!q) return true;
  const blob = `${inferredRegion || ''} ${location || ''}`.toLowerCase();
  return blob.includes(q);
}

function inferRegion(location, country) {
  const raw = clean(location, 200);
  if (!raw) return '';
  const parts = raw.split(',').map((x) => x.trim()).filter(Boolean);
  if (parts.length >= 2) return parts.slice(0, -1).join(', ');
  if (country && raw.toLowerCase().includes(country.toLowerCase())) return raw.replace(new RegExp(country, 'i'), '').replace(/^,|,$/g, '').trim();
  return raw;
}

function filterLiveRows(rows, filters) {
  const filtered = [];
  const relaxed = [];
  for (const row of rows) {
    const rowCountry = normalizeCountryName(row.country || guessCountryFromText(row.location || ''));
    const rowWorkMode = canonicalizeWorkMode(row.work_mode || inferWorkMode(row.title, row.location, row.description_text));
    const postedValue = row.posted_date || '';
    const regionOk = !filters.region || locationQueryMatch(filters.region, inferRegion(row.location || '', rowCountry), row.location || '');
    const countryOk = !filters.country || rowCountry === filters.country || rowWorkMode === 'Remote' || !rowCountry;
    const workOk = !filters.workMode || rowWorkMode === filters.workMode || !row.work_mode;
    const postedOk = !postedValue || dateFilterMatch(postedValue, filters.posted);

    row.country = rowCountry;
    row.work_mode = rowWorkMode;

    if (countryOk && workOk) relaxed.push(row);
    if (countryOk && workOk && regionOk && postedOk) filtered.push(row);
  }
  return filtered.length ? filtered : relaxed;
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
      summary: clean(resumeContext.summary, 1400),
      skills: clean(resumeContext.skills, 1600),
      experience: clean(resumeContext.experience, 1800),
      projects: clean(resumeContext.projects, 1200),
      education: clean(resumeContext.education, 1000),
      resume_text_excerpt: clean(resumeContext.resume_text, 2600),
    },
    jobs: jobs.map((job) => ({
      job_id: clean(job.job_id, 180),
      title: clean(job.title, 180),
      company: clean(job.company, 140),
      location: clean(job.location, 140),
      country: clean(job.country, 80),
      work_mode: clean(job.work_mode, 40),
      posted_date: clean(job.posted_date, 40),
      job_function: clean(job.job_function, 120),
      job_domain: clean(job.job_domain, 120),
      job_category: clean(job.job_category, 120),
      description_excerpt: clean(job.description_text || job.page_text, 1200),
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
    const country = normalizeCountryName(clean(job.country || guessCountryFromText(location), 120));
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



async function sourceRowsToJobs({ sources, filters, resumeContext, searchModel }) {
  return sourceRowsFromSearchSources({
    sources,
    filters,
    resumeContext,
    searchModel,
    requestedCount: TARGET_RESULTS,
  });
}

function buildLiveJobsSchema(requestedCount) {
  return {
    type: 'object',
    additionalProperties: false,
    properties: {
      jobs: {
        type: 'array',
        maxItems: Math.max(1, Math.min(Number(requestedCount || TARGET_RESULTS), TARGET_RESULTS)),
        items: {
          type: 'object',
          additionalProperties: false,
          properties: {
            title: { type: 'string' },
            company: { type: 'string' },
            url: { type: 'string' },
            location: { type: 'string' },
            country: { type: 'string' },
            work_mode: { type: 'string' },
            posted_date: { type: 'string' },
            description_text: { type: 'string' },
            match_percentage: { type: 'number' },
            reason: { type: 'string' },
          },
          required: ['title', 'url', 'match_percentage', 'reason'],
        },
      },
    },
    required: ['jobs'],
  };
}

function makeCompactLiveSearchPrompt({ resumeContext, filters, requestedCount }) {
  const profile = deriveResumeSearchProfile(resumeContext);
  const titles = uniqueKeepOrder(profile.role_titles || [], 6);
  const keywords = uniqueKeepOrder(profile.keywords || [], 14);
  const avoidTerms = uniqueKeepOrder(profile.negative_terms || [], 10);
  const resumePayload = {
    country: clean(resumeContext.candidate_country, 120),
    experience_years: resumeContext.candidate_experience_years,
    degree_level: clean(resumeContext.candidate_degree_level, 80),
    degree_family: clean(resumeContext.candidate_degree_family, 120),
    degree_fields: Array.isArray(resumeContext.candidate_degree_fields) ? resumeContext.candidate_degree_fields.slice(0, 8) : [],
    category: clean(resumeContext.candidate_category || resumeContext.candidate_category_key, 120),
    function: clean(resumeContext.candidate_function, 120),
    domain: clean(resumeContext.candidate_domain, 120),
    summary: clean(resumeContext.summary, 1000),
    skills: clean(resumeContext.skills, 1200),
    experience: clean(resumeContext.experience, 1200),
    projects: clean(resumeContext.projects, 800),
    education: clean(resumeContext.education, 800),
    resume_text_excerpt: clean(resumeContext.resume_text, 2200),
  };
  const requested = Math.max(1, Number(requestedCount || TARGET_RESULTS));
  return [
    `Use live web search to find up to ${requested} CURRENT job-posting detail pages that fit this resume.`,
    'Use only the resume-derived role titles, technical signals, and user filters below.',
    'Prefer employer ATS or direct job-detail pages. If there are too few, a specific job-detail page on a major job board is acceptable.',
    'Do not return category pages, search results pages, homepages, or broad directory pages.',
    'If the selected city is too strict, broaden within the selected country and still return the best relevant jobs instead of none.',
    `User filters: ${JSON.stringify({ country: filters.country || 'any', city: filters.region || 'any', work_mode: filters.workMode || 'any', posted_range: filters.posted || 'all' })}`,
    `Resume-derived role titles: ${JSON.stringify(titles)}`,
    `Strong resume keywords: ${JSON.stringify(keywords)}`,
    avoidTerms.length ? `Avoid unrelated families: ${JSON.stringify(avoidTerms)}` : '',
    `Candidate summary: ${JSON.stringify(resumePayload)}`,
    'Return only valid JSON in this exact shape: {"jobs":[{"title":"string","company":"string","url":"string","location":"string","country":"string","work_mode":"string","posted_date":"string","description_text":"string","match_percentage":0,"reason":"short string"}]}',
    'Never invent links. Omit closed jobs. It is okay to return fewer than requested.'
  ].filter(Boolean).join(' ');
}

function extractJobsFromResponsesPayload(data, searchModel = '') {
  if (!data || typeof data !== 'object') return [];
  const parsed = extractResponsesJsonPayload(data);
  const direct = Array.isArray(parsed?.jobs)
    ? parsed.jobs.filter((item) => item && typeof item === 'object')
    : extractJsonArray(extractTextFromResponsesPayload(data));
  return direct.map((item) => ({ ...item, search_model: clean(item.search_model, 120) || searchModel }));
}


function postedLabelFromFilter(value) {
  const low = clean(value, 40).toLowerCase();
  if (!low || low === 'all') return 'Unknown';
  if (low === '24h') return 'Past 24 hours';
  if (low === 'week') return 'Past week';
  if (low === 'month') return 'Past month';
  return titleCase(low);
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

function extractTextFromResponsesPayload(data) {
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

function collectSourceUrls(obj) {
  const out = [];
  const seen = new Set();
  const walk = (node) => {
    if (!node) return;
    if (Array.isArray(node)) {
      for (const item of node) walk(item);
      return;
    }
    if (typeof node !== 'object') return;
    if (Array.isArray(node.sources)) {
      for (const item of node.sources) {
        const url = clean(item?.url || item?.link, 500);
        const title = clean(item?.title || item?.name, 240);
        if (url && !seen.has(url)) {
          seen.add(url);
          out.push({ url, title });
        }
      }
    }
    if (node.type === 'url_citation') {
      const uc = node.url_citation && typeof node.url_citation === 'object' ? node.url_citation : node;
      const url = clean(uc.url, 500);
      const title = clean(uc.title, 240);
      if (url && !seen.has(url)) {
        seen.add(url);
        out.push({ url, title });
      }
    }
    for (const value of Object.values(node)) walk(value);
  };
  walk(obj);
  return out;
}

function extractChatAnnotations(data) {
  const out = [];
  const seen = new Set();
  const annotations = data?.choices?.[0]?.message?.annotations;
  if (!Array.isArray(annotations)) return out;
  for (const item of annotations) {
    const uc = item?.url_citation && typeof item.url_citation === 'object' ? item.url_citation : item;
    const url = clean(uc?.url, 500);
    const title = clean(uc?.title, 240);
    if (url && !seen.has(url)) {
      seen.add(url);
      out.push({ url, title });
    }
  }
  return out;
}

function extractUrlsFromText(text) {
  const matches = String(text || '').match(/https?:\/\/[^\s)\]>"']+/g) || [];
  const seen = new Set();
  const out = [];
  for (const match of matches) {
    const url = clean(String(match).replace(/[.,;]+$/g, ''), 500);
    if (url && !seen.has(url)) {
      seen.add(url);
      out.push({ url, title: '' });
    }
  }
  return out;
}

function makeLiveSearchPrompt({ resumeContext, filters, requestedCount, excludeUrls, focusTitles }) {
  const profile = deriveResumeSearchProfile(resumeContext);
  return [
    `Find ${requestedCount} direct live job-detail pages that best match this candidate.`,
    'Use live internet search and favor direct job-detail pages over generic search results or company careers homepages.',
    'Return ONLY a JSON array. Each item should have: url, title, company, location, country, work_mode, posted_date, description_text, reason.',
    `Candidate category: ${profile.category || 'General'}`,
    `Candidate function: ${profile.function || 'Unknown'}`,
    `Candidate domain: ${profile.domain || 'Unknown'}`,
    `Preferred role titles: ${(focusTitles && focusTitles.filter(Boolean).length ? focusTitles : profile.role_titles).join(', ') || 'best matching jobs'}`,
    `Important keywords: ${(profile.keywords || []).join(', ') || 'resume-derived skills'}`,
    `Avoid jobs about: ${(profile.negative_terms || []).join(', ') || 'none'}`,
    `Country filter: ${filters.country || 'any'}`,
    `City/region filter: ${filters.region || 'any'}`,
    `Work mode filter: ${filters.workMode || 'any'}`,
    `Posted range filter: ${filters.posted || 'all'}`,
    excludeUrls?.length ? `Do not repeat these URLs: ${excludeUrls.slice(0, 25).join(', ')}` : '',
    `Resume summary: ${clean(resumeContext.summary, 900)}`,
    `Resume skills: ${clean(resumeContext.skills, 1200)}`,
    `Resume experience: ${clean(resumeContext.experience, 1200)}`,
    `Resume projects: ${clean(resumeContext.projects, 900)}`,
    `Resume text excerpt: ${clean(resumeContext.resume_text, 2200)}`,
  ].filter(Boolean).join('\n');
}

function makeLiveSourcesPrompt({ resumeContext, filters, requestedCount, excludeUrls, focusTitles }) {
  const profile = deriveResumeSearchProfile(resumeContext);
  return [
    `Find ${requestedCount} direct live job-detail pages for this resume.`,
    'Do not return generic search pages. Prefer direct application or job posting pages.',
    `Role focus: ${(focusTitles && focusTitles.filter(Boolean).length ? focusTitles : profile.role_titles).join(', ') || 'resume-matched jobs'}`,
    `Keywords: ${(profile.keywords || []).join(', ') || 'resume-derived skills'}`,
    `Avoid: ${(profile.negative_terms || []).join(', ') || 'none'}`,
    `Country: ${filters.country || 'any'}`,
    `City/region: ${filters.region || 'any'}`,
    `Work mode: ${filters.workMode || 'any'}`,
    `Posted range: ${filters.posted || 'all'}`,
    excludeUrls?.length ? `Exclude URLs: ${excludeUrls.slice(0, 25).join(', ')}` : '',
    `Resume excerpt: ${clean(resumeContext.resume_text, 2200)}`,
  ].filter(Boolean).join('\n');
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

function buildChatUserLocation(country, region) {
  const countryCode = toIso2(country);
  if (!countryCode && !region) return null;
  return {
    approximate: {
      country: countryCode || undefined,
      city: region || undefined,
    },
  };
}

function toIso2(country) {
  const raw = clean(country, 120);
  if (!raw) return '';
  if (/^[A-Za-z]{2}$/.test(raw)) return raw.toUpperCase();
  return COUNTRY_TO_ISO2[raw.toLowerCase()] || '';
}

function normalizeLiveJobRow(job) {
  const url = clean(job.url || job.link || job.source_url, 500);
  if (!url) return {};
  const location = clean(job.location, 180);
  const description = clean(job.description_text || job.page_text, 1800);
  const title = clean(job.title || job.job_title, 220);
  const country = normalizeCountryName(clean(job.country || guessCountryFromText(location), 120));
  return {
    job_id: clean(job.job_id || url, 260),
    title,
    company: clean(job.company, 180),
    url,
    source_url: url,
    location,
    country,
    work_mode: canonicalizeWorkMode(clean(job.work_mode, 60) || inferWorkMode(title, location, description)),
    posted_date: clean(job.posted_date || job.days_posted, 80),
    job_function: clean(job.job_function, 120),
    job_domain: clean(job.job_domain, 120),
    job_category: clean(job.job_category, 120),
    description_text: description,
    reason: clean(job.reason, 160),
    search_model: clean(job.search_model, 120),
    match_percentage: clamp(Number(job.match_percentage || 0), 0, 100),
    relevance_score: Number.isFinite(Number(job.relevance_score)) ? Number(job.relevance_score) : jobRelevanceScore(deriveResumeSearchProfile({ candidate_category: clean(job.job_category,120), candidate_function: clean(job.job_function,120), candidate_domain: clean(job.job_domain,120), skills: description, resume_text: description }), { title, descriptionText: description, pageText: description }),
  };
}

async function requestOpenAIJson({ url, apiKey, payload, attempts = 1 }) {
  let lastError = null;
  for (let attempt = 1; attempt <= Math.max(1, attempts); attempt += 1) {
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        const text = await response.text();
        const error = new Error(text || `OpenAI request failed with ${response.status}`);
        if (response.status === 429 && attempt < attempts) {
          await sleep(900 * attempt);
          lastError = error;
          continue;
        }
        throw error;
      }
      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt >= attempts) throw error;
      await sleep(900 * attempt);
    }
  }
  throw lastError || new Error('OpenAI request failed.');
}

function relevanceToPercent(score) {
  return clamp(Math.round(52 + Number(score || 0) * 6), 35, 92);
}


function normalizeWebModel(model) {
  const raw = clean(model, 120);
  if (!raw) return DEFAULT_WEB_MODEL;
  if (['gpt-4o-search-preview', 'gpt-4o-mini-search-preview', 'gpt-5-search-api'].includes(raw)) return DEFAULT_WEB_MODEL;
  return raw;
}

function normalizeChatSearchModel(model) {
  const raw = clean(model, 120);
  if (!raw) return '';
  if (['gpt-4o-search-preview', 'gpt-4o-mini-search-preview', 'gpt-5-search-api'].includes(raw)) return raw;
  return '';
}

function clean(value, maxChars) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxChars);
}

function finiteOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, Number.isFinite(value) ? value : min));
}

function titleCase(value) {
  return String(value || '').replace(/\w\S*/g, (word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase());
}

function hostFromUrl(url) {
  try { return new URL(url).hostname || ''; } catch { return ''; }
}

function simpleHash(value) {
  let hash = 0;
  const text = String(value || '');
  for (let i = 0; i < text.length; i += 1) hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
  return Math.abs(hash);
}

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cleanError(error) {
  const raw = String(error?.message || error || 'Unexpected premium backend error.').trim();
  try {
    const parsed = JSON.parse(raw);
    const message = parsed?.error?.message || parsed?.message || parsed?.error || '';
    if (message) return clean(message, 500);
  } catch {}
  return clean(raw, 500);
}

function readBearer(request) {
  const header = request.headers.get('authorization') || '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) throw new Error('Missing session token. Sign in again.');
  return match[1].trim();
}

async function getAuthedUser({ supabaseUrl, anonKey, token }) {
  const response = await fetch(`${supabaseUrl}/auth/v1/user`, {
    headers: { apikey: anonKey, Authorization: `Bearer ${token}` },
  });
  if (!response.ok) throw new Error('Your session expired. Sign in again.');
  return response.json();
}

async function getProfile({ supabaseUrl, secretKey, userId }) {
  const url = `${supabaseUrl}/rest/v1/profiles?id=eq.${encodeURIComponent(userId)}&select=premium_access,premium_admin_access,premium_searches_used`;
  const response = await fetch(url, {
    headers: { apikey: secretKey, Authorization: `Bearer ${secretKey}` },
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
      Prefer: 'return=minimal',
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
