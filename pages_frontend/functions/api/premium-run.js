const DEFAULT_MODEL = 'gpt-4o-mini';
const MAX_PREMIUM_SEARCHES = 5;

export async function onRequestPost(context) {
  const supabaseUrl = context.env.SUPABASE_URL || '';
  const anonKey = context.env.SUPABASE_ANON_KEY || '';
  const secretKey = context.env.SUPABASE_SECRET_KEY || '';
  const openAiKey = context.env.OPENAI_API_KEY || '';
  const openAiModel = context.env.OPENAI_MODEL || DEFAULT_MODEL;

  if (!supabaseUrl || !anonKey || !secretKey || !openAiKey) {
    return json({ error: 'Missing premium backend configuration.' }, 500);
  }

  try {
    const token = readBearer(context.request);
    const user = await getAuthedUser({ supabaseUrl, anonKey, token });
    const body = await context.request.json().catch(() => ({}));
    const resumeContext = body?.resumeContext || {};
    const filters = body?.filters || {};
    const resumeId = String(body?.resumeId || '').trim();
    const candidateJobs = Array.isArray(body?.candidateJobs) ? body.candidateJobs.slice(0, 30) : [];

    if (!resumeId) return json({ error: 'Resume ID is required.' }, 400);
    if (!candidateJobs.length) return json({ error: 'No candidate jobs were sent for premium reranking.' }, 400);

    const profile = await getProfile({ supabaseUrl, secretKey, userId: user.id });
    const premiumUnlocked = !!profile?.premium_access || !!profile?.premium_admin_access;
    if (!premiumUnlocked) return json({ error: 'Premium is still locked for this account.' }, 403);

    const searchesUsed = Number(profile?.premium_searches_used || 0);
    const isAdmin = !!profile?.premium_admin_access;
    if (!isAdmin && searchesUsed >= MAX_PREMIUM_SEARCHES) {
      return json({ error: 'No premium searches remaining.' }, 403);
    }

    const results = await rerankWithOpenAI({
      apiKey: openAiKey,
      model: openAiModel,
      resumeContext,
      jobs: candidateJobs,
    });

    if (!isAdmin) {
      await patchProfile({
        supabaseUrl,
        secretKey,
        userId: user.id,
        patch: { premium_searches_used: searchesUsed + 1 },
      });
    }

    return json({ ok: true, results, filters, used: isAdmin ? searchesUsed : searchesUsed + 1 });
  } catch (error) {
    return json({ error: error?.message || 'Unexpected premium backend error.' }, 500);
  }
}

async function rerankWithOpenAI({ apiKey, model, resumeContext, jobs }) {
  const prompt = {
    resume: {
      country: clean(resumeContext.candidate_country, 80),
      experience_years: resumeContext.candidate_experience_years ?? null,
      degree_level: clean(resumeContext.candidate_degree_level, 60),
      degree_family: clean(resumeContext.candidate_degree_family, 120),
      function: clean(resumeContext.candidate_function, 120),
      domain: clean(resumeContext.candidate_domain, 120),
      category: clean(resumeContext.candidate_category_key, 120),
      resume_text_excerpt: clean(resumeContext.resume_text, 2400),
    },
    jobs: jobs.map((job) => ({
      job_id: clean(job.job_id, 180),
      title: clean(job.title, 180),
      company: clean(job.company, 140),
      location: clean(job.location, 140),
      country: clean(job.country, 80),
      work_mode: clean(job.work_mode, 40),
      posted_date: clean(job.posted_date_display || job.posted_date, 40),
      description_excerpt: clean(job.description_text || job.description || '', 1200),
      local_score: Number(job.final_match_percent || 0),
      raw_score: Number(job.raw_match_percent || 0),
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
          content: JSON.stringify(prompt),
        },
      ],
      max_completion_tokens: 1800,
    }),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'OpenAI premium rerank failed.');
  }

  const data = await response.json();
  const content = data?.choices?.[0]?.message?.content || '';
  const parsed = extractJsonObject(content);
  const returned = Array.isArray(parsed?.results) ? parsed.results : [];
  const byId = new Map(returned.map((item) => [String(item.job_id || ''), item]));

  return jobs.map((job) => {
    const hit = byId.get(String(job.job_id || '')) || {};
    const premiumScore = clamp(Number(hit.premium_score ?? job.final_match_percent ?? 0), 0, 100);
    return {
      ...job,
      raw_match_percent: Math.round(Number(job.final_match_percent || job.raw_match_percent || 0)),
      final_match_percent: Math.round(premiumScore),
      premium_reason: clean(hit.premium_reason, 120),
    };
  }).sort((a, b) => (b.final_match_percent || 0) - (a.final_match_percent || 0));
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

function clean(value, maxChars) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxChars);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
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
