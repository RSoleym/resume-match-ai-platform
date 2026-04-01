export async function onRequestGet(context) {
  const supabaseUrl = context.env.SUPABASE_URL || '';
  const secretKey = context.env.SUPABASE_SECRET_KEY || '';

  if (!supabaseUrl || !secretKey) {
    return jsonResponse({ jobsCount: null, error: 'Missing Supabase server config.' }, 500);
  }

  try {
    const endpoint = `${supabaseUrl}/rest/v1/jobs?select=job_id`;
    const response = await fetch(endpoint, {
      method: 'GET',
      headers: {
        apikey: secretKey,
        Authorization: `Bearer ${secretKey}`,
        Prefer: 'count=exact',
        Range: '0-0',
      },
    });

    if (!response.ok) {
      const message = await response.text();
      return jsonResponse({ jobsCount: null, error: message || 'Jobs count request failed.' }, response.status);
    }

    const contentRange = response.headers.get('content-range') || '';
    let jobsCount = null;
    if (contentRange.includes('/')) {
      const raw = Number(contentRange.split('/').pop());
      if (Number.isFinite(raw)) jobsCount = raw;
    }

    return jsonResponse({ jobsCount });
  } catch (error) {
    return jsonResponse({ jobsCount: null, error: error?.message || 'Unexpected error.' }, 500);
  }
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
    },
  });
}
