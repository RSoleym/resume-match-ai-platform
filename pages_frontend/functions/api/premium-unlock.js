const DEFAULT_PREMIUM_ACCESS_CODE = 'Capstone2026';
const DEFAULT_PREMIUM_ADMIN_CODE = 'JBisADemon%92';
const MAX_PREMIUM_SEARCHES = 3;

export async function onRequestPost(context) {
  const supabaseUrl = String(context.env.SUPABASE_URL || '').trim();
  const anonKey = String(context.env.SUPABASE_ANON_KEY || context.env.SUPABASE_PUBLISHABLE_KEY || '').trim();
  const secretKey = String(context.env.SUPABASE_SECRET_KEY || context.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
  const premiumCode = String(context.env.PREMIUM_ACCESS_CODE || DEFAULT_PREMIUM_ACCESS_CODE).trim();
  const premiumAdminCode = String(context.env.PREMIUM_ADMIN_CODE || DEFAULT_PREMIUM_ADMIN_CODE).trim();

  if (!supabaseUrl || !anonKey || !secretKey) {
    return json({ error: 'Missing Cloudflare backend config.' }, 500);
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
        premium_source: 'cloudflare-code',
        premium_granted_at: now,
        premium_searches_used: 0,
      };
    }

    if (!patch) return json({ error: 'That premium code is not valid.' }, 403);

    const response = await fetch(`${supabaseUrl}/rest/v1/profiles?id=eq.${encodeURIComponent(user.id)}`, {
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
    const row = Array.isArray(rows) ? rows[0] || null : null;
    return json({
      ok: true,
      max_searches: MAX_PREMIUM_SEARCHES,
      profile: row || patch,
    });
  } catch (error) {
    return json({ error: String(error?.message || error || 'Unexpected error.').slice(0, 500) }, 500);
  }
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

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
    },
  });
}
