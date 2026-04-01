alter table public.jobs
  add column if not exists country text,
  add column if not exists work_mode text,
  add column if not exists job_function text,
  add column if not exists job_domain text,
  add column if not exists job_category_key text,
  add column if not exists job_category_confidence double precision,
  add column if not exists job_category_scores jsonb,
  add column if not exists experience_needed_years double precision,
  add column if not exists degree_level_min text,
  add column if not exists degree_family text,
  add column if not exists degree_fields jsonb;

create index if not exists idx_jobs_country
  on public.jobs (country);

create index if not exists idx_jobs_work_mode
  on public.jobs (work_mode);

create index if not exists idx_jobs_function
  on public.jobs (job_function);

create index if not exists idx_jobs_domain
  on public.jobs (job_domain);

create index if not exists idx_jobs_category_key
  on public.jobs (job_category_key);

create index if not exists idx_jobs_degree_level_min
  on public.jobs (degree_level_min);

create index if not exists idx_jobs_degree_family
  on public.jobs (degree_family);

create index if not exists idx_jobs_experience_needed_years
  on public.jobs (experience_needed_years);
