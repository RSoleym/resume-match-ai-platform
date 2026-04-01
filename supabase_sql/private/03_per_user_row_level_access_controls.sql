create policy "users can view own profile"
on public.profiles
for select
to authenticated
using (auth.uid() = id);

create policy "users can insert own profile"
on public.profiles
for insert
to authenticated
with check (auth.uid() = id);

create policy "users can update own profile"
on public.profiles
for update
to authenticated
using (auth.uid() = id)
with check (auth.uid() = id);

create policy "users can view own resumes"
on public.resumes
for select
to authenticated
using (auth.uid() = user_id);

create policy "users can insert own resumes"
on public.resumes
for insert
to authenticated
with check (auth.uid() = user_id);

create policy "users can update own resumes"
on public.resumes
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "users can delete own resumes"
on public.resumes
for delete
to authenticated
using (auth.uid() = user_id);

create policy "users can view own match results"
on public.match_results
for select
to authenticated
using (auth.uid() = user_id);

create policy "users can insert own match results"
on public.match_results
for insert
to authenticated
with check (auth.uid() = user_id);

create policy "users can update own match results"
on public.match_results
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "users can delete own match results"
on public.match_results
for delete
to authenticated
using (auth.uid() = user_id);
