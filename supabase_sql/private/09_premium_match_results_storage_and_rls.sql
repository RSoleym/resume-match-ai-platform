create table if not exists public.premium_match_results (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  resume_id uuid references public.resumes(id) on delete cascade,
  filters_json jsonb not null default '{}'::jsonb,
  results_json jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);

alter table public.premium_match_results enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'premium_match_results' and policyname = 'users can view own premium match results'
  ) then
    create policy "users can view own premium match results"
    on public.premium_match_results
    for select
    to authenticated
    using (auth.uid() = user_id);
  end if;

  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'premium_match_results' and policyname = 'users can insert own premium match results'
  ) then
    create policy "users can insert own premium match results"
    on public.premium_match_results
    for insert
    to authenticated
    with check (auth.uid() = user_id);
  end if;

  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'premium_match_results' and policyname = 'users can update own premium match results'
  ) then
    create policy "users can update own premium match results"
    on public.premium_match_results
    for update
    to authenticated
    using (auth.uid() = user_id)
    with check (auth.uid() = user_id);
  end if;

  if not exists (
    select 1 from pg_policies
    where schemaname = 'public' and tablename = 'premium_match_results' and policyname = 'users can delete own premium match results'
  ) then
    create policy "users can delete own premium match results"
    on public.premium_match_results
    for delete
    to authenticated
    using (auth.uid() = user_id);
  end if;
end $$;

create index if not exists idx_premium_match_results_user_created
  on public.premium_match_results (user_id, created_at desc);

create index if not exists idx_premium_match_results_resume
  on public.premium_match_results (resume_id);
