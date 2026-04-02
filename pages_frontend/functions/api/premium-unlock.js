const MAX_PREMIUM_SEARCHES = 5;
const DEFAULT_ONE_TIME_PREMIUM_CODE = 'Capstone2026';
const DEFAULT_ADMIN_PREMIUM_CODE = 'JBisADemon%92';

export async function onRequestPost(context) {
  const supabaseUrl = readEnv(context.env, ['SUPABASE_URL']);
  const anonKey = readEnv(context.env, ['SUPABASE_ANON_KEY', 'SUPABASE_PUBLISHABLE_KEY']);
  const secretKey = readEnv(context.env, ['SUPABASE_SECRET_KEY', 'SUPABASE_SERVICE_ROLE_KEY', 'SUPABASE_SERVICE_KEY']);
  const premiumCode = readEnv(context.env, ['PREMIUM_ACCESS_CODE']) || DEFAULT_ONE_TIME_PREMIUM_CODE;
  const premiumAdminCode = readEnv(context.env, ['PREMIUM_ADMIN_CODE']) || DEFAULT_ADMIN_PREMIUM_CODE;

  const missing = [];
  if (!supabaseUrl) missing.push('SUPABASE_URL');
  if (!anonKey) missing.push('SUPABASE_ANON_KEY');
  if (!secretKey) missing.push('SUPABASE_SECRET_KEY');

  if (missing.length) {
    return json({ error: `Missing Cloudflare backend config: ${missing.join(', ')}.` }, 500);
  }

  try {
    const token = readBearer(context.request);
    const user = await getAuthedUser({ supabaseUrl, anonKey, token });
    const body = await context.request.json().catch(() => ({}));
    const code = String(body?.code || '').trim();
    if (!code) return json({ error: 'Enter a premium code.' }, 400);

    let patch = null;
    const now = new Date().toISOString();

    if (code === premiumAdminCode) {
      patch = {
        premium_access: true,
        premium_admin_access: true,
        premium_source: 'cloudflare-admin-code',
        premium_admin_source: 'cloudflare-admin-code',
        premium_granted_at: now,
        premium_admin_granted_at: now,
        premium_searches_used: 0,
      };
    } else if (code === premiumCode) {
      patch = {
        premium_access: true,
        premium_admin_access: false,
        premium_source: 'cloudflare-one-time-code',
        premium_granted_at: now,
        premium_searches_used: Math.max(0, MAX_PREMIUM_SEARCHES - 1),
      };
    }

    if (!patch) {
      return json({ error: 'That premium code is not valid.' }, 403);
    }

    const updateUrl = `${supabaseUrl}/rest/v1/profiles?id=eq.${encodeURIComponent(user.id)}`;
    const response = await fetch(updateUrl, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        apikey: secretKey,
        Authorization: `Bearer ${secretKey}`,
        Prefer: 'return=representation',
      },
      body: JSON.stringify(patch),
    });

    if (!response.ok) {
      const message = await response.text();
      return json({ error: message || 'Could not unlock premium.' }, response.status);
    }

    const rows = await response.json().catch(() => []);
    const row = Array.isArray(rows) ? rows[0] : null;
    return json({ ok: true, profile: row || patch });
  } catch (error) {
    return json({ error: error?.message || 'Unexpected error.' }, 500);
  }
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

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
    },
  });
}
