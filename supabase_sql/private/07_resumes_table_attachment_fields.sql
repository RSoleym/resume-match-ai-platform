alter table public.resumes
  add column if not exists candidate_country text,
  add column if not exists candidate_experience_years double precision,
  add column if not exists candidate_degree_level text,
  add column if not exists candidate_degree_family text,
  add column if not exists candidate_degree_fields jsonb,
  add column if not exists candidate_function text,
  add column if not exists candidate_domain text,
  add column if not exists candidate_category_key text,
  add column if not exists candidate_category_confidence double precision,
  add column if not exists candidate_category_scores jsonb;

create index if not exists idx_resumes_user_id
  on public.resumes (user_id);

create index if not exists idx_resumes_candidate_country
  on public.resumes (candidate_country);

create index if not exists idx_resumes_candidate_function
  on public.resumes (candidate_function);

create index if not exists idx_resumes_candidate_domain
  on public.resumes (candidate_domain);

create index if not exists idx_resumes_candidate_category_key
  on public.resumes (candidate_category_key);

create index if not exists idx_resumes_candidate_degree_level
  on public.resumes (candidate_degree_level);

create index if not exists idx_resumes_candidate_degree_family
  on public.resumes (candidate_degree_family);

create index if not exists idx_resumes_candidate_experience_years
  on public.resumes (candidate_experience_years);
