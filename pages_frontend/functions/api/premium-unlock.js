export async function onRequestPost(context) {
  const supabaseUrl = context.env.SUPABASE_URL || '';
  const anonKey = context.env.SUPABASE_ANON_KEY || '';
  const secretKey = context.env.SUPABASE_SECRET_KEY || '';
  const premiumCode = (context.env.PREMIUM_ACCESS_CODE || '').trim();
  const premiumAdminCode = (context.env.PREMIUM_ADMIN_CODE || '').trim();

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
    if (premiumAdminCode && code === premiumAdminCode) {
      patch = {
        premium_access: true,
        premium_admin_access: true,
        premium_source: 'cloudflare-code',
        premium_admin_source: 'cloudflare-admin-code',
        premium_granted_at: new Date().toISOString(),
        premium_admin_granted_at: new Date().toISOString(),
      };
    } else if (premiumCode && code === premiumCode) {
      patch = {
        premium_access: true,
        premium_source: 'cloudflare-code',
        premium_granted_at: new Date().toISOString(),
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
