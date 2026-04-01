const DEFAULT_LIMIT = 800;
const MAX_LIMIT = 1200;

export async function onRequestGet(context) {
  const supabaseUrl = context.env.SUPABASE_URL || '';
  const secretKey = context.env.SUPABASE_SECRET_KEY || '';

  if (!supabaseUrl || !secretKey) {
    return json({ error: 'Missing Supabase server config.' }, 500);
  }

  try {
    const url = new URL(context.request.url);
    const requestedLimit = Number(url.searchParams.get('limit') || DEFAULT_LIMIT);
    const limit = Number.isFinite(requestedLimit) ? Math.max(50, Math.min(MAX_LIMIT, requestedLimit)) : DEFAULT_LIMIT;
    const countryMode = (url.searchParams.get('countryMode') || 'all').trim().toLowerCase();
    const selectedCountries = (url.searchParams.get('selectedCountries') || '')
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean);
    const singleCountry = (url.searchParams.get('country') || '').trim();
    const workMode = (url.searchParams.get('workMode') || '').trim().toLowerCase();
    const posted = (url.searchParams.get('posted') || 'all').trim().toLowerCase();

    const restUrl = new URL(`${supabaseUrl}/rest/v1/jobs`);
    restUrl.searchParams.set('select', [
      'job_id',
      'title',
      'company',
      'location',
      'country',
      'work_mode',
      'description_text',
      'posted_date',
      'job_function',
      'job_domain',
      'job_category_key',
      'degree_level_min',
      'degree_family',
      'degree_fields',
      'experience_needed_years',
      'source_url',
    ].join(','));
    restUrl.searchParams.set('order', 'posted_date.desc.nullslast,title.asc');

    const upstream = await fetch(restUrl.toString(), {
      headers: {
        apikey: secretKey,
        Authorization: `Bearer ${secretKey}`,
        Range: `0-${Math.max(limit * 2, 1200) - 1}`,
      },
    });

    if (!upstream.ok) {
      const message = await upstream.text();
      return json({ error: message || 'Jobs query failed.' }, upstream.status);
    }

    const raw = await upstream.json();
    const baseRows = Array.isArray(raw) ? raw : [];
    const wantedCountries = new Set(singleCountry ? [singleCountry] : selectedCountries);
    const minPostedDate = computeMinPostedDate(posted);

    const rows = baseRows.filter((row) => {
      const rowCountry = String(row?.country || '').trim();
      const rowWorkMode = normalizeWorkMode(row?.work_mode || '');
      const rowPosted = row?.posted_date ? new Date(row.posted_date) : null;

      if (wantedCountries.size) {
        const inCountry = wantedCountries.has(rowCountry);
        const allowRemote = rowWorkMode === 'remote';
        if (!inCountry && !allowRemote) return false;
      }

      if (countryMode === 'current' && !wantedCountries.size && !rowCountry && rowWorkMode !== 'remote') {
        return false;
      }

      if (workMode && rowWorkMode !== workMode) {
        return false;
      }

      if (minPostedDate && rowPosted instanceof Date && !Number.isNaN(rowPosted.valueOf()) && rowPosted < minPostedDate) {
        return false;
      }

      return true;
    }).slice(0, limit);

    return json({ jobs: rows });
  } catch (error) {
    return json({ error: error?.message || 'Unexpected error.' }, 500);
  }
}

function normalizeWorkMode(value) {
  const text = String(value || '').toLowerCase();
  if (text.includes('remote')) return 'remote';
  if (text.includes('hybrid')) return 'hybrid';
  if (text.includes('on-site') || text.includes('onsite')) return 'on-site';
  return text || 'on-site';
}

function computeMinPostedDate(posted) {
  const now = new Date();
  if (posted === 'week') return new Date(now.getTime() - (7 * 24 * 60 * 60 * 1000));
  if (posted === 'month') return new Date(now.getTime() - (31 * 24 * 60 * 60 * 1000));
  return null;
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
