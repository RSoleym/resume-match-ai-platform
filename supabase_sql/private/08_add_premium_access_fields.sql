alter table public.profiles
  add column if not exists premium_access boolean not null default false,
  add column if not exists premium_granted_at timestamptz,
  add column if not exists premium_source text,
  add column if not exists premium_searches_used integer not null default 0,
  add column if not exists premium_last_run_at timestamptz,
  add column if not exists premium_admin_access boolean not null default false,
  add column if not exists premium_admin_granted_at timestamptz,
  add column if not exists premium_admin_source text;

create index if not exists idx_profiles_premium_access
  on public.profiles (premium_access);

create index if not exists idx_profiles_premium_admin_access
  on public.profiles (premium_admin_access);
