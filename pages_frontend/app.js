const CONFIG_ENDPOINT = '/api/public-config';
const DASHBOARD_STATS_ENDPOINT = '/api/dashboard-stats';
const MAX_PREMIUM_SEARCHES = 3;
const COUNTRY_OPTIONS = [
  'Australia', 'Canada', 'China', 'Costa Rica', 'France', 'Germany', 'India', 'Ireland', 'Israel', 'Japan',
  'Korea, Republic of', 'Malaysia', 'Mexico', 'Netherlands', 'Poland', 'Singapore', 'Taiwan', 'USA', 'United Kingdom'
];

let supabaseClient = null;
let activeSession = null;
let resumesCache = [];
let freeResultsCache = [];
let premiumResultsCache = [];
let profileCache = null;

const dom = {
  topbarSubtitle: document.getElementById('topbarSubtitle'),
  topbarEmailBadge: document.getElementById('topbarEmailBadge'),
  topSignOutButton: document.getElementById('topSignOutButton'),
  topSignInLink: document.getElementById('topSignInLink'),
  topCreateAccountLink: document.getElementById('topCreateAccountLink'),
  globalMessage: document.getElementById('globalMessage'),
  navLinks: Array.from(document.querySelectorAll('.nav-link')),
  sections: Array.from(document.querySelectorAll('.view-section')),
  authLinks: Array.from(document.querySelectorAll('[data-auth-link="logged-out"]')),
  sidebarTip: document.getElementById('sidebarTip'),
  dashboardSignedOutBanner: document.getElementById('dashboardSignedOutBanner'),
  statResumesUploaded: document.getElementById('statResumesUploaded'),
  statResumesScanned: document.getElementById('statResumesScanned'),
  statJobsScraped: document.getElementById('statJobsScraped'),
  statMatchRows: document.getElementById('statMatchRows'),
  dashboardResumeEmpty: document.getElementById('dashboardResumeEmpty'),
  dashboardResumeList: document.getElementById('dashboardResumeList'),
  uploadForm: document.getElementById('uploadForm'),
  uploadButton: document.getElementById('uploadButton'),
  uploadMessage: document.getElementById('uploadMessage'),
  fileInput: document.getElementById('fileInput'),
  previewBox: document.getElementById('previewBox'),
  imgPreview: document.getElementById('imgPreview'),
  fileName: document.getElementById('fileName'),
  uploadListHeading: document.getElementById('uploadListHeading'),
  resumeListEmpty: document.getElementById('resumeListEmpty'),
  resumeList: document.getElementById('resumeList'),
  loginForm: document.getElementById('loginForm'),
  loginButton: document.getElementById('loginButton'),
  loginEmail: document.getElementById('loginEmail'),
  loginPassword: document.getElementById('loginPassword'),
  loginMessage: document.getElementById('loginMessage'),
  signupForm: document.getElementById('signupForm'),
  signupButton: document.getElementById('signupButton'),
  signupEmail: document.getElementById('signupEmail'),
  signupPassword: document.getElementById('signupPassword'),
  signupConfirmPassword: document.getElementById('signupConfirmPassword'),
  signupMessage: document.getElementById('signupMessage'),
  freeRunForm: document.getElementById('freeRunForm'),
  freeRunButton: document.getElementById('freeRunButton'),
  freeRunMessage: document.getElementById('freeRunMessage'),
  freeRunStatus: document.getElementById('freeRunStatus'),
  freeRunStage: document.getElementById('freeRunStage'),
  freeRunElapsed: document.getElementById('freeRunElapsed'),
  freeRunProgressFill: document.getElementById('freeRunProgressFill'),
  premiumRunForm: document.getElementById('premiumRunForm'),
  premiumRunButton: document.getElementById('premiumRunButton'),
  premiumRunMessage: document.getElementById('premiumRunMessage'),
  premiumRunStatus: document.getElementById('premiumRunStatus'),
  premiumRunStage: document.getElementById('premiumRunStage'),
  premiumRunElapsed: document.getElementById('premiumRunElapsed'),
  premiumRunProgressFill: document.getElementById('premiumRunProgressFill'),
  runTabs: Array.from(document.querySelectorAll('.run-tab-btn')),
  runPanels: Array.from(document.querySelectorAll('.run-tab-panel')),
  locationMode: document.getElementById('locationMode'),
  selectedCountriesWrap: document.getElementById('selectedCountriesWrap'),
  countrySearchInput: document.getElementById('countrySearchInput'),
  countryPicker: document.getElementById('countryPicker'),
  addCountryBtn: document.getElementById('addCountryBtn'),
  selectedCountryChips: document.getElementById('selectedCountryChips'),
  resultsFilterForm: document.getElementById('resultsFilterForm'),
  resultsResumeFilter: document.getElementById('resultsResumeFilter'),
  resultsCountryFilter: document.getElementById('resultsCountryFilter'),
  resultsRegionFilter: document.getElementById('resultsRegionFilter'),
  resultsWorkModeFilter: document.getElementById('resultsWorkModeFilter'),
  resultsPostedFilter: document.getElementById('resultsPostedFilter'),
  resultsPageFilter: document.getElementById('resultsPageFilter'),
  resultsShowing: document.getElementById('resultsShowing'),
  resultsTotal: document.getElementById('resultsTotal'),
  resultsEmptyState: document.getElementById('resultsEmptyState'),
  resultsList: document.getElementById('resultsList'),
  premiumUnlockedBadge: document.getElementById('premiumUnlockedBadge'),
  premiumUsedBadge: document.getElementById('premiumUsedBadge'),
  premiumRemainingBadge: document.getElementById('premiumRemainingBadge'),
  premiumAdminBadge: document.getElementById('premiumAdminBadge'),
  premiumUnlockCard: document.getElementById('premiumUnlockCard'),
  premiumUnlockForm: document.getElementById('premiumUnlockForm'),
  premiumCodeInput: document.getElementById('premiumCodeInput'),
  premiumUnlockMessage: document.getElementById('premiumUnlockMessage'),
  premiumCountryInput: document.getElementById('premiumCountryInput'),
  premiumRegionInput: document.getElementById('premiumRegionInput'),
  premiumWorkModeInput: document.getElementById('premiumWorkModeInput'),
  premiumPostedInput: document.getElementById('premiumPostedInput'),
  premiumFilterForm: document.getElementById('premiumFilterForm'),
  premiumCountryFilter: document.getElementById('premiumCountryFilter'),
  premiumRegionFilter: document.getElementById('premiumRegionFilter'),
  premiumWorkModeFilter: document.getElementById('premiumWorkModeFilter'),
  premiumPostedFilter: document.getElementById('premiumPostedFilter'),
  premiumPageFilter: document.getElementById('premiumPageFilter'),
  premiumSavedCount: document.getElementById('premiumSavedCount'),
  premiumShowing: document.getElementById('premiumShowing'),
  premiumEmptyState: document.getElementById('premiumEmptyState'),
  premiumList: document.getElementById('premiumList')
};

function clearMessage(el) {
  if (!el) return;
  el.textContent = '';
  el.classList.add('hidden');
  el.classList.remove('message-success', 'message-error', 'message-info');
}

function showMessage(el, text, kind = 'info') {
  if (!el) return;
  el.textContent = text;
  el.classList.remove('hidden', 'message-success', 'message-error', 'message-info');
  el.classList.add(kind === 'success' ? 'message-success' : kind === 'error' ? 'message-error' : 'message-info');
}

function setBusy(button, busy, idleLabel, busyLabel) {
  if (!button) return;
  button.disabled = !!busy;
  button.textContent = busy ? busyLabel : idleLabel;
}

function updatePipelineUi(statusEl, fillEl, message, progress = null) {
  if (statusEl && message) statusEl.textContent = message;
  if (fillEl && progress !== null && Number.isFinite(Number(progress))) {
    fillEl.style.width = `${Math.max(0, Math.min(100, Number(progress)))}%`;
  }
}

const pipelineUiState = {
  free: {
    statusEl: dom.freeRunStatus,
    stageEl: dom.freeRunStage,
    elapsedEl: dom.freeRunElapsed,
    fillEl: dom.freeRunProgressFill,
    idleMessage: 'Browser pipeline idle.',
    timerId: null,
    startedAt: null,
  },
  premium: {
    statusEl: dom.premiumRunStatus,
    stageEl: dom.premiumRunStage,
    elapsedEl: dom.premiumRunElapsed,
    fillEl: dom.premiumRunProgressFill,
    idleMessage: 'Premium pipeline idle.',
    timerId: null,
    startedAt: null,
  }
};

function formatElapsed(ms) {
  const totalSeconds = Math.max(0, Math.floor(Number(ms || 0) / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, '0')}`;
}

function stopPipelineTimer(kind) {
  const ui = pipelineUiState[kind];
  if (!ui) return;
  if (ui.timerId) {
    window.clearInterval(ui.timerId);
    ui.timerId = null;
  }
}

function setPipelineElapsed(kind, elapsedMs) {
  const ui = pipelineUiState[kind];
  if (!ui?.elapsedEl) return;
  ui.elapsedEl.textContent = formatElapsed(elapsedMs);
}

function updatePipelineDisplay(kind, message, progress = null, stage = null) {
  const ui = pipelineUiState[kind];
  if (!ui) return;
  updatePipelineUi(ui.statusEl, ui.fillEl, message, progress);
  if (ui.stageEl) ui.stageEl.textContent = stage || message || 'Idle';
  if (ui.startedAt) setPipelineElapsed(kind, Date.now() - ui.startedAt);
}

function startPipelineDisplay(kind, message, progress = 0) {
  const ui = pipelineUiState[kind];
  if (!ui) return;
  stopPipelineTimer(kind);
  ui.startedAt = Date.now();
  setPipelineElapsed(kind, 0);
  updatePipelineDisplay(kind, message, progress, message);
  ui.timerId = window.setInterval(() => {
    if (ui.startedAt) setPipelineElapsed(kind, Date.now() - ui.startedAt);
  }, 250);
}

function finishPipelineDisplay(kind, message, progress = 100, stage = 'Complete') {
  updatePipelineDisplay(kind, message, progress, stage);
  stopPipelineTimer(kind);
}

function resetPipelineDisplay(kind) {
  const ui = pipelineUiState[kind];
  if (!ui) return;
  stopPipelineTimer(kind);
  ui.startedAt = null;
  updatePipelineUi(ui.statusEl, ui.fillEl, ui.idleMessage, 0);
  if (ui.stageEl) ui.stageEl.textContent = 'Idle';
  setPipelineElapsed(kind, 0);
}

async function getSupabaseClient() {
  const res = await fetch(CONFIG_ENDPOINT, { cache: 'no-store' });
  if (!res.ok) throw new Error('Could not load config.');
  const cfg = await res.json();
  if (!cfg.supabaseUrl || !cfg.supabaseAnonKey) throw new Error('Supabase config is missing.');
  return window.supabase.createClient(cfg.supabaseUrl, cfg.supabaseAnonKey, {
    auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: true }
  });
}

function currentViewFromHash() {
  const raw = (window.location.hash || '#dashboard').replace('#', '').trim().toLowerCase();
  const allowed = ['dashboard', 'upload', 'run', 'results', 'premium', 'login', 'signup'];
  return allowed.includes(raw) ? raw : 'dashboard';
}

function switchView(view) {
  dom.sections.forEach((section) => {
    section.classList.toggle('hidden', section.dataset.section !== view);
  });
  dom.navLinks.forEach((link) => {
    const isActive = link.dataset.view === view;
    link.classList.toggle('active-nav', isActive);
    link.classList.toggle('bg-white/5', isActive);
  });
  if (window.location.hash !== `#${view}`) {
    window.history.replaceState(null, '', `#${view}`);
  }
}

function fmtRelative(dateString) {
  if (!dateString) return 'Unknown';
  const ms = Date.now() - new Date(dateString).getTime();
  if (!Number.isFinite(ms)) return dateString;
  const sec = Math.round(ms / 1000);
  if (sec < 60) return 'Just now';
  const min = Math.round(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.round(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.round(hr / 24);
  if (day < 30) return `${day}d ago`;
  return new Date(dateString).toLocaleDateString();
}

function safeFileName(name) {
  return String(name || 'resume').replace(/[^a-zA-Z0-9._-]+/g, '-');
}

function normalizePercent(value) {
  if (value === null || value === undefined || value === '') return 0;
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return Math.max(0, Math.min(100, num <= 1 ? num * 100 : num));
}

function normalizeResult(item, type = 'free') {
  const title = item.title || item.job_title || item.role || 'Untitled role';
  const company = item.company || item.company_name || 'Unknown company';
  const location = item.location || item.region || item.city || 'Unknown location';
  const country = item.country || '';
  const region = item.region || item.city || '';
  const workMode = item.work_mode || item.workMode || '—';
  const postedDate = item.posted_date_display || item.posted_date || item.posted || 'Unknown';
  const finalPct = Math.round(normalizePercent(item.final_match_percent ?? item.match_percent ?? item.score));
  const rawPct = Math.round(normalizePercent(item.raw_match_percent ?? item.raw_score ?? item.semantic_score ?? item.score));
  return {
    type,
    job_id: item.job_id || item.id || `${title}-${company}-${location}`,
    title,
    company,
    location,
    country,
    region,
    work_mode: workMode,
    posted_date_display: postedDate,
    final_match_percent: finalPct,
    raw_match_percent: rawPct,
    penalty_applied: !!item.penalty_applied,
    premium_reason: item.premium_reason || '',
    best_url: item.best_url || item.url || item.source_url || ''
  };
}

function resultCardHtml(r, premium = false) {
  const pct = r.final_match_percent;
  const raw = r.raw_match_percent;
  return `
    <div class="result-card rounded-3xl p-5 ring-soft">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div class="text-lg font-semibold text-slate-100">${escapeHtml(r.title)}</div>
          <div class="mt-1 text-sm text-slate-400">${escapeHtml(r.company)} • ${escapeHtml(r.location)}</div>
          <div class="mt-2 flex flex-wrap gap-2 text-xs">
            <span class="rounded-full bg-white/10 px-3 py-1 ring-soft">${premium ? 'Premium' : 'Final'}: <b class="text-slate-100">${pct}%</b></span>
            <span class="rounded-full bg-white/10 px-3 py-1 ring-soft">Raw: <b class="text-slate-100">${raw}%</b></span>
            ${r.country ? `<span class="rounded-full bg-white/10 px-3 py-1 ring-soft">Country: <b class="text-slate-100">${escapeHtml(r.country)}</b></span>` : ''}
            ${r.region ? `<span class="rounded-full bg-white/10 px-3 py-1 ring-soft">City: <b class="text-slate-100">${escapeHtml(r.region)}</b></span>` : ''}
            <span class="rounded-full bg-white/10 px-3 py-1 ring-soft">Work mode: <b class="text-slate-100">${escapeHtml(r.work_mode)}</b></span>
            <span class="rounded-full bg-white/10 px-3 py-1 ring-soft">Posted: <b class="text-slate-100">${escapeHtml(r.posted_date_display)}</b></span>
            ${r.penalty_applied ? `<span class="rounded-full bg-rose-500/15 px-3 py-1 text-rose-200 ring-soft">Penalty applied</span>` : ''}
            ${r.premium_reason ? `<span class="rounded-full bg-indigo-500/15 px-3 py-1 text-indigo-200 ring-soft">${escapeHtml(r.premium_reason)}</span>` : ''}
          </div>
        </div>
        <div class="flex items-center gap-2">
          ${r.best_url ? `<a href="${escapeAttr(r.best_url)}" target="_blank" rel="noopener noreferrer" class="rounded-2xl bg-white/10 px-4 py-2 text-sm font-semibold hover:bg-white/15 ring-soft">Open posting</a>` : `<span class="rounded-2xl bg-white/5 px-4 py-2 text-sm text-slate-400 ring-soft">No URL</span>`}
        </div>
      </div>
      <div class="mt-4">
        <div class="h-2 w-full rounded-full bg-black/30 ring-soft"><div class="h-2 rounded-full bg-gradient-to-r from-indigo-500 to-fuchsia-500" style="width:${pct}%"></div></div>
      </div>
    </div>`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function escapeAttr(value) {
  return escapeHtml(value);
}

function initCountryPicker() {
  dom.countryPicker.innerHTML = '<option value="">Choose a country</option>';
  COUNTRY_OPTIONS.forEach((country) => {
    const opt = document.createElement('option');
    opt.value = country;
    opt.textContent = country;
    dom.countryPicker.appendChild(opt);
  });
  dom.premiumCountryFilter.innerHTML = '<option value="">All countries</option>' + COUNTRY_OPTIONS.map((c) => `<option value="${c}">${c}</option>`).join('');
  if (dom.premiumCountryInput) {
    dom.premiumCountryInput.innerHTML = '<option value="">All countries</option>' + COUNTRY_OPTIONS.map((c) => `<option value="${c}">${c}</option>`).join('');
  }
}

let selectedCountries = [];

function renderSelectedCountries() {
  dom.selectedCountryChips.innerHTML = '';
  if (!selectedCountries.length) {
    dom.selectedCountryChips.innerHTML = '<div class="text-sm text-slate-500">No countries chosen yet.</div>';
  } else {
    selectedCountries.forEach((country) => {
      const wrap = document.createElement('div');
      wrap.className = 'inline-flex items-center gap-2 rounded-full bg-white/10 px-3 py-1.5 text-sm ring-soft';
      wrap.innerHTML = `<span>${escapeHtml(country)}</span>`;
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'text-slate-300 hover:text-white';
      btn.textContent = '×';
      btn.addEventListener('click', () => {
        selectedCountries = selectedCountries.filter((c) => c !== country);
        renderSelectedCountries();
      });
      wrap.appendChild(btn);
      dom.selectedCountryChips.appendChild(wrap);
    });
  }
}

function syncCountryMode() {
  dom.selectedCountriesWrap.classList.toggle('hidden', dom.locationMode.value !== 'selected');
}

function previewSelectedFile() {
  const file = dom.fileInput.files?.[0];
  if (!file) return;
  dom.fileName.textContent = file.name;
  dom.previewBox.classList.remove('hidden');
  if (file.type && file.type.startsWith('image/')) {
    const objectUrl = URL.createObjectURL(file);
    dom.imgPreview.src = objectUrl;
    dom.imgPreview.classList.remove('hidden');
  } else {
    dom.imgPreview.removeAttribute('src');
    dom.imgPreview.classList.add('hidden');
  }
}

function updateSessionUi(session) {
  activeSession = session || null;
  const email = session?.user?.email || '';
  const loggedIn = !!email;
  dom.topbarSubtitle.textContent = loggedIn ? `Signed in as ${email}` : 'Sign in to keep resumes separate per user';
  dom.topbarEmailBadge.textContent = email;
  dom.topbarEmailBadge.classList.toggle('hidden', !loggedIn);
  dom.topSignOutButton.classList.toggle('hidden', !loggedIn);
  dom.topSignInLink.classList.toggle('hidden', loggedIn);
  dom.topCreateAccountLink.classList.toggle('hidden', loggedIn);
  dom.authLinks.forEach((link) => link.classList.toggle('hidden', loggedIn));
  dom.dashboardSignedOutBanner.classList.toggle('hidden', loggedIn);
  dom.sidebarTip.textContent = loggedIn
    ? 'Upload a resume, then use the run and results pages from this account.'
    : 'Create an account, verify your email if needed, then sign in to keep your resumes separate.';
}

async function refreshSession() {
  const { data, error } = await supabaseClient.auth.getSession();
  if (error) throw error;
  updateSessionUi(data.session || null);
}

async function fetchCount(table, applyFilter) {
  try {
    let query = supabaseClient.from(table).select('*', { count: 'exact', head: true });
    if (typeof applyFilter === 'function') {
      query = applyFilter(query);
    }
    const { count, error } = await query;
    if (error) throw error;
    return count ?? 0;
  } catch {
    return null;
  }
}

async function fetchServerJobsCount() {
  try {
    const res = await fetch(DASHBOARD_STATS_ENDPOINT, { cache: 'no-store' });
    if (!res.ok) throw new Error('Could not load dashboard stats.');
    const data = await res.json();
    const jobsCount = Number(data?.jobsCount);
    return Number.isFinite(jobsCount) ? jobsCount : null;
  } catch {
    return null;
  }
}


async function loadProfile() {
  profileCache = null;
  if (!activeSession?.user?.id) {
    updatePremiumBadges();
    return;
  }
  try {
    const { data, error } = await supabaseClient
      .from('profiles')
      .select('premium_access,premium_granted_at,premium_source,premium_searches_used,premium_admin_access,premium_admin_granted_at,premium_admin_source')
      .eq('id', activeSession.user.id)
      .maybeSingle();
    if (!error && data) profileCache = data;
  } catch {}
  updatePremiumBadges();
}

function updatePremiumBadges() {
  const used = Number(profileCache?.premium_searches_used || 0);
  const remaining = Math.max(0, MAX_PREMIUM_SEARCHES - used);
  const unlocked = !!profileCache?.premium_access;
  const admin = !!profileCache?.premium_admin_access;
  dom.premiumUnlockedBadge.textContent = unlocked ? 'Unlocked' : 'Locked';
  dom.premiumUsedBadge.textContent = String(used);
  dom.premiumRemainingBadge.textContent = String(remaining);
  dom.premiumAdminBadge.textContent = admin ? 'Yes' : 'No';
  const showUnlockCard = !admin && (!unlocked || remaining <= 0);
  dom.premiumUnlockCard.classList.toggle('hidden', !showUnlockCard);
}

function renderResumeLists() {
  dom.uploadListHeading.textContent = `Current resumes in your account (${resumesCache.length}/1)`;
  dom.resumeList.innerHTML = '';
  dom.dashboardResumeList.innerHTML = '';
  const hasResumes = resumesCache.length > 0;
  dom.resumeListEmpty.classList.toggle('hidden', hasResumes);
  dom.dashboardResumeEmpty.classList.toggle('hidden', hasResumes);
  if (!hasResumes) {
    dom.resultsResumeFilter.innerHTML = '<option value="">All resumes</option>';
    return;
  }

  resumesCache.forEach((resume) => {
    const deleteId = escapeAttr(resume.id || '');
    const html = `
      <div class="resume-row flex items-center justify-between rounded-2xl px-3 py-2 text-sm ring-soft gap-3">
        <div class="min-w-0">
          <div class="truncate text-slate-200">${escapeHtml(resume.file_name || 'Resume')}</div>
          <div class="mt-1 text-xs text-slate-500">${escapeHtml(fmtRelative(resume.uploaded_at))}</div>
        </div>
        <div class="flex items-center gap-2 shrink-0">
          <span class="text-xs text-slate-500">PDF</span>
          <button type="button" data-delete-resume-id="${deleteId}" class="inline-flex h-8 w-8 items-center justify-center rounded-full bg-white/5 text-slate-300 ring-soft hover:bg-rose-500/15 hover:text-rose-200" aria-label="Delete resume">×</button>
        </div>
      </div>`;
    dom.resumeList.insertAdjacentHTML('beforeend', html);
    dom.dashboardResumeList.insertAdjacentHTML('beforeend', html);
  });

  const resumeOptions = ['<option value="">All resumes</option>'].concat(
    resumesCache.map((r) => `<option value="${escapeAttr(r.id)}">${escapeHtml(r.file_name || r.id)}</option>`)
  );
  dom.resultsResumeFilter.innerHTML = resumeOptions.join('');
}


async function loadResumes() {
  resumesCache = [];
  if (!activeSession?.user?.id) {
    renderResumeLists();
    return;
  }
  const { data, error } = await supabaseClient
    .from('resumes')
    .select('id,file_name,storage_path,uploaded_at,parsed_text')
    .eq('user_id', activeSession.user.id)
    .order('uploaded_at', { ascending: false });
  if (error) throw error;
  resumesCache = data || [];
  renderResumeLists();
}

async function loadDashboardStats() {
  const resumeCount = resumesCache.length;
  const scannedCount = resumesCache.filter((r) => (r.parsed_text || '').trim().length > 0).length;
  dom.statResumesUploaded.textContent = String(resumeCount);
  dom.statResumesScanned.textContent = String(scannedCount);

  if (!activeSession?.user?.id) {
    dom.statMatchRows.textContent = '0';
    return;
  }
  const matchCount = await fetchCount('match_results', (query) => query.eq('user_id', activeSession.user.id));
  dom.statMatchRows.textContent = matchCount == null ? '—' : String(matchCount);
  const jobsCount = await fetchServerJobsCount();
  dom.statJobsScraped.textContent = jobsCount == null ? '—' : String(jobsCount);
}

function collectAllResults(rows, key, type) {
  const all = [];
  (rows || []).forEach((row) => {
    const arr = Array.isArray(row[key]) ? row[key] : [];
    arr.forEach((item) => {
      const normalized = normalizeResult(item, type);
      normalized.resume_id = row.resume_id || '';
      normalized.created_at = row.created_at || '';
      all.push(normalized);
    });
  });
  return all;
}

async function loadResultsData() {
  freeResultsCache = [];
  premiumResultsCache = [];
  if (!activeSession?.user?.id) {
    renderResults();
    renderPremiumResults();
    return;
  }

  try {
    const { data: matchRows } = await supabaseClient
      .from('match_results')
      .select('resume_id,results_json,created_at')
      .eq('user_id', activeSession.user.id)
      .order('created_at', { ascending: false })
      .limit(10);
    freeResultsCache = collectAllResults(matchRows, 'results_json', 'free');
  } catch {
    freeResultsCache = [];
  }

  try {
    const { data: premiumRows } = await supabaseClient
      .from('premium_match_results')
      .select('resume_id,results_json,created_at')
      .eq('user_id', activeSession.user.id)
      .order('created_at', { ascending: false })
      .limit(10);
    premiumResultsCache = collectAllResults(premiumRows, 'results_json', 'premium');
  } catch {
    premiumResultsCache = [];
  }

  renderResults();
  renderPremiumResults();
}

function fillSelectFromValues(select, values, placeholder) {
  const current = select.value;
  const uniq = [...new Set(values.filter(Boolean))].sort((a, b) => String(a).localeCompare(String(b)));
  select.innerHTML = [`<option value="">${placeholder}</option>`, ...uniq.map((v) => `<option value="${escapeAttr(v)}">${escapeHtml(v)}</option>`)].join('');
  if (uniq.includes(current)) select.value = current;
}

function applyDateFilter(items, mode) {
  if (mode === 'all') return items;
  const now = Date.now();
  const limitDays = mode === 'week' ? 7 : 31;
  return items.filter((item) => {
    const parsed = Date.parse(item.posted_date_display);
    if (!Number.isFinite(parsed)) return true;
    return (now - parsed) / 86400000 <= limitDays;
  });
}

function paginate(items, pageSelect) {
  const pageSize = 10;
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
  const current = Math.min(totalPages, Math.max(1, Number(pageSelect.value || 1)));
  pageSelect.innerHTML = Array.from({ length: totalPages }, (_, idx) => {
    const start = idx * pageSize + 1;
    const end = Math.min((idx + 1) * pageSize, items.length || 0);
    return `<option value="${idx + 1}">${start}-${end || 0}</option>`;
  }).join('') || '<option value="1">0-0</option>';
  pageSelect.value = String(current);
  const startIndex = (current - 1) * pageSize;
  return {
    pageItems: items.slice(startIndex, startIndex + pageSize),
    start: items.length ? startIndex + 1 : 0,
    end: Math.min(startIndex + pageSize, items.length)
  };
}

function renderResults() {
  fillSelectFromValues(dom.resultsCountryFilter, freeResultsCache.map((r) => r.country), 'All countries');
  fillSelectFromValues(dom.resultsWorkModeFilter, freeResultsCache.map((r) => r.work_mode), 'All work modes');

  let filtered = freeResultsCache.slice();
  if (dom.resultsResumeFilter.value) filtered = filtered.filter((r) => r.resume_id === dom.resultsResumeFilter.value);
  if (dom.resultsCountryFilter.value) filtered = filtered.filter((r) => r.country === dom.resultsCountryFilter.value);
  if (dom.resultsWorkModeFilter.value) filtered = filtered.filter((r) => r.work_mode === dom.resultsWorkModeFilter.value);
  const region = dom.resultsRegionFilter.value.trim().toLowerCase();
  if (region) filtered = filtered.filter((r) => `${r.location} ${r.region}`.toLowerCase().includes(region));
  filtered = applyDateFilter(filtered, dom.resultsPostedFilter.value || 'all');
  filtered.sort((a, b) => b.final_match_percent - a.final_match_percent || b.raw_match_percent - a.raw_match_percent);

  const { pageItems, start, end } = paginate(filtered, dom.resultsPageFilter);
  dom.resultsShowing.textContent = `${start}-${end}`;
  dom.resultsTotal.textContent = String(filtered.length);
  dom.resultsList.innerHTML = pageItems.map((r) => resultCardHtml(r, false)).join('');
  dom.resultsEmptyState.classList.toggle('hidden', filtered.length > 0);
}

function renderPremiumResults() {
  fillSelectFromValues(dom.premiumCountryFilter, premiumResultsCache.map((r) => r.country), 'All countries');
  fillSelectFromValues(dom.premiumWorkModeFilter, premiumResultsCache.map((r) => r.work_mode), 'All work modes');

  let filtered = premiumResultsCache.slice();
  if (dom.premiumCountryFilter.value) filtered = filtered.filter((r) => r.country === dom.premiumCountryFilter.value);
  if (dom.premiumWorkModeFilter.value) filtered = filtered.filter((r) => r.work_mode === dom.premiumWorkModeFilter.value);
  const region = dom.premiumRegionFilter.value.trim().toLowerCase();
  if (region) filtered = filtered.filter((r) => `${r.location} ${r.region}`.toLowerCase().includes(region));
  filtered = applyDateFilter(filtered, dom.premiumPostedFilter.value || 'all');
  filtered.sort((a, b) => b.final_match_percent - a.final_match_percent || b.raw_match_percent - a.raw_match_percent);

  const { pageItems, start, end } = paginate(filtered, dom.premiumPageFilter);
  dom.premiumSavedCount.textContent = String(filtered.length);
  dom.premiumShowing.textContent = `${start}-${end}`;
  dom.premiumList.innerHTML = pageItems.map((r) => resultCardHtml(r, true)).join('');
  dom.premiumEmptyState.classList.toggle('hidden', filtered.length > 0);
}

async function refreshAll() {
  clearMessage(dom.globalMessage);
  await refreshSession();
  await loadProfile();
  await loadResumes();
  await loadDashboardStats();
  await loadResultsData();
}

async function handleLogin(event) {
  event.preventDefault();
  clearMessage(dom.loginMessage);
  setBusy(dom.loginButton, true, 'Sign in', 'Signing in...');
  try {
    const { error } = await supabaseClient.auth.signInWithPassword({
      email: dom.loginEmail.value.trim(),
      password: dom.loginPassword.value
    });
    if (error) throw error;
    await refreshAll();
    showMessage(dom.loginMessage, 'Signed in.', 'success');
    switchView('dashboard');
  } catch (err) {
    showMessage(dom.loginMessage, err.message || 'Sign in failed.', 'error');
  } finally {
    setBusy(dom.loginButton, false, 'Sign in', 'Signing in...');
  }
}

async function handleSignup(event) {
  event.preventDefault();
  clearMessage(dom.signupMessage);
  const email = dom.signupEmail.value.trim();
  const password = dom.signupPassword.value;
  const confirm = dom.signupConfirmPassword.value;
  if (password !== confirm) {
    showMessage(dom.signupMessage, 'Passwords do not match.', 'error');
    return;
  }
  setBusy(dom.signupButton, true, 'Create account', 'Creating account...');
  try {
    const redirectTo = `${window.location.origin}/`;
    const { error } = await supabaseClient.auth.signUp({ email, password, options: { emailRedirectTo: redirectTo } });
    if (error) throw error;
    showMessage(dom.signupMessage, 'Account created. Check your email if confirmation is enabled.', 'success');
  } catch (err) {
    showMessage(dom.signupMessage, err.message || 'Create account failed.', 'error');
  } finally {
    setBusy(dom.signupButton, false, 'Create account', 'Creating account...');
  }
}

async function handleUpload(event) {
  event.preventDefault();
  clearMessage(dom.uploadMessage);
  setBusy(dom.uploadButton, true, 'Upload', 'Uploading...');
  try {
    if (!activeSession?.user?.id) throw new Error('Sign in before uploading a resume.');
    const file = dom.fileInput.files?.[0];
    if (!file) throw new Error('Choose a file first.');
    if (file.size > 1024 * 1024) throw new Error('File is over 1 MB.');

    const { count: existingCount, error: countError } = await supabaseClient
      .from('resumes')
      .select('*', { count: 'exact', head: true })
      .eq('user_id', activeSession.user.id);
    if (countError) throw countError;
    if ((existingCount ?? 0) >= 1) throw new Error('Only 1 resume is allowed for now. Delete the current one first.');

    const storagePath = `${activeSession.user.id}/${Date.now()}-${safeFileName(file.name)}`;
    const { error: uploadError } = await supabaseClient.storage.from('resumes').upload(storagePath, file, {
      cacheControl: '3600',
      upsert: false,
    });
    if (uploadError) throw uploadError;

    const { error: dbError } = await supabaseClient.from('resumes').insert({
      user_id: activeSession.user.id,
      file_name: file.name,
      storage_path: storagePath,
      parsed_text: null,
    });
    if (dbError) {
      await supabaseClient.storage.from('resumes').remove([storagePath]).catch(() => {});
      throw dbError;
    }

    dom.uploadForm.reset();
    dom.previewBox.classList.add('hidden');
    dom.imgPreview.removeAttribute('src');
    dom.imgPreview.classList.add('hidden');
    dom.fileName.textContent = '';
    await refreshAll();
    showMessage(dom.uploadMessage, `Uploaded ${file.name}.`, 'success');
  } catch (err) {
    showMessage(dom.uploadMessage, err.message || 'Upload failed.', 'error');
  } finally {
    setBusy(dom.uploadButton, false, 'Upload', 'Uploading...');
  }
}

async function deleteResume(resumeId) {
  if (!activeSession?.user?.id) {
    showMessage(dom.globalMessage, 'Sign in before deleting a resume.', 'error');
    return;
  }
  const resume = resumesCache.find((item) => item.id === resumeId);
  if (!resume) {
    showMessage(dom.globalMessage, 'Resume not found.', 'error');
    return;
  }
  const ok = window.confirm(`Delete ${resume.file_name || 'this resume'}?`);
  if (!ok) return;

  try {
    if (resume.storage_path) {
      const { error: storageError } = await supabaseClient.storage.from('resumes').remove([resume.storage_path]);
      if (storageError && !/not\s+found/i.test(storageError.message || '')) {
        throw storageError;
      }
    }

    const { error: deleteError } = await supabaseClient
      .from('resumes')
      .delete()
      .eq('id', resume.id)
      .eq('user_id', activeSession.user.id);
    if (deleteError) throw deleteError;

    await refreshAll();
    showMessage(dom.globalMessage, 'Resume deleted.', 'success');
  } catch (err) {
    showMessage(dom.globalMessage, err.message || 'Could not delete the resume.', 'error');
  }
}

function handleResumeListClick(event) {
  const button = event.target.closest('[data-delete-resume-id]');
  if (!button) return;
  event.preventDefault();
  const resumeId = button.getAttribute('data-delete-resume-id') || '';
  if (!resumeId) return;
  deleteResume(resumeId);
}

async function handleSignOut() {
  try {
    const { error } = await supabaseClient.auth.signOut();
    if (error) throw error;
    await refreshAll();
    showMessage(dom.globalMessage, 'Signed out.', 'success');
    switchView('dashboard');
  } catch (err) {
    showMessage(dom.globalMessage, err.message || 'Sign out failed.', 'error');
  }
}

function setRunTab(tab) {
  dom.runTabs.forEach((btn) => {
    const active = btn.dataset.runTab === tab;
    btn.classList.toggle('bg-indigo-600', active);
    btn.classList.toggle('text-white', active);
    btn.classList.toggle('bg-white/5', !active);
    btn.classList.toggle('text-slate-300', !active);
  });
  dom.runPanels.forEach((panel) => {
    panel.classList.toggle('hidden', panel.id !== (tab === 'premium' ? 'runTabPremium' : 'runTabFree'));
  });
}

async function handleFreeRun(event) {
  event.preventDefault();
  clearMessage(dom.freeRunMessage);
  startPipelineDisplay('free', 'Checking browser pipeline…', 4);
  setBusy(dom.freeRunButton, true, 'Start', 'Running browser pipeline...');
  try {
    if (!window.ResumeBrowserPipeline) throw new Error('Browser pipeline client is missing from the frontend.');
    if (!activeSession?.user?.id) throw new Error('Sign in before running the browser pipeline.');
    const resume = resumesCache[0];
    if (!resume?.id) throw new Error('Upload a resume first.');

    const result = await window.ResumeBrowserPipeline.runFreePipeline({
      supabaseClient,
      session: activeSession,
      resumeRow: resume,
      locationMode: dom.locationMode.value,
      selectedCountries,
      onProgress: ({ message, progress }) => updatePipelineDisplay('free', message, progress, message),
    });

    await refreshAll();
    showMessage(dom.freeRunMessage, `Saved ${result.results.length} browser-scored matches.`, 'success');
    finishPipelineDisplay('free', 'Browser pipeline finished. Results are ready.', 100, 'Complete');
    switchView('results');
  } catch (err) {
    showMessage(dom.freeRunMessage, err.message || 'Free browser pipeline failed.', 'error');
    finishPipelineDisplay('free', err.message || 'Free browser pipeline failed.', 0, 'Failed');
  } finally {
    setBusy(dom.freeRunButton, false, 'Start', 'Running browser pipeline...');
  }
}

async function handlePremiumRun(event) {
  event.preventDefault();
  clearMessage(dom.premiumRunMessage);
  startPipelineDisplay('premium', 'Checking premium backend…', 4);
  setBusy(dom.premiumRunButton, true, 'Run', 'Running premium...');
  try {
    if (!window.ResumeBrowserPipeline) throw new Error('Browser pipeline client is missing from the frontend.');
    if (!activeSession?.user?.id) throw new Error('Sign in before running premium.');
    const resume = resumesCache[0];
    if (!resume?.id) throw new Error('Upload a resume first.');

    const result = await window.ResumeBrowserPipeline.runPremiumPipeline({
      supabaseClient,
      session: activeSession,
      resumeRow: resume,
      filters: {
        country: dom.premiumCountryInput?.value || '',
        region: dom.premiumRegionInput?.value?.trim() || '',
        workMode: dom.premiumWorkModeInput?.value || '',
        posted: dom.premiumPostedInput?.value || 'all',
      },
      onProgress: ({ message, progress }) => updatePipelineDisplay('premium', message, progress, message),
    });

    await refreshAll();
    showMessage(dom.premiumRunMessage, `Saved ${Array.isArray(result.results) ? result.results.length : 0} premium matches.`, 'success');
    finishPipelineDisplay('premium', 'Premium run finished. Saved to the Premium page.', 100, 'Complete');
    switchView('premium');
  } catch (err) {
    showMessage(dom.premiumRunMessage, err.message || 'Premium run failed.', 'error');
    finishPipelineDisplay('premium', err.message || 'Premium run failed.', 0, 'Failed');
  } finally {
    setBusy(dom.premiumRunButton, false, 'Run', 'Running premium...');
  }
}

async function handlePremiumUnlock(event) {
  event.preventDefault();
  const code = dom.premiumCodeInput.value.trim();
  if (!code) {
    showMessage(dom.premiumUnlockMessage, 'Enter a premium code.', 'error');
    return;
  }
  try {
    if (!window.ResumeBrowserPipeline) throw new Error('Browser pipeline client is missing from the frontend.');
    await window.ResumeBrowserPipeline.unlockPremium({ supabaseClient, code });
    dom.premiumCodeInput.value = '';
    await refreshAll();
    showMessage(dom.premiumUnlockMessage, 'Premium unlocked.', 'success');
  } catch (err) {
    showMessage(dom.premiumUnlockMessage, err.message || 'Premium unlock failed.', 'error');
  }
}

function attachEvents() {

  window.addEventListener('hashchange', () => switchView(currentViewFromHash()));
  dom.fileInput?.addEventListener('change', previewSelectedFile);
  dom.loginForm?.addEventListener('submit', handleLogin);
  dom.signupForm?.addEventListener('submit', handleSignup);
  dom.uploadForm?.addEventListener('submit', handleUpload);
  dom.resumeList?.addEventListener('click', handleResumeListClick);
  dom.dashboardResumeList?.addEventListener('click', handleResumeListClick);
  dom.topSignOutButton?.addEventListener('click', handleSignOut);
  dom.freeRunForm?.addEventListener('submit', handleFreeRun);
  dom.premiumRunForm?.addEventListener('submit', handlePremiumRun);
  dom.premiumUnlockForm?.addEventListener('submit', handlePremiumUnlock);
  dom.resultsFilterForm?.addEventListener('submit', (e) => { e.preventDefault(); renderResults(); });
  dom.premiumFilterForm?.addEventListener('submit', (e) => { e.preventDefault(); renderPremiumResults(); });
  dom.resultsPageFilter?.addEventListener('change', renderResults);
  dom.premiumPageFilter?.addEventListener('change', renderPremiumResults);
  dom.resultsResumeFilter?.addEventListener('change', renderResults);
  dom.resultsCountryFilter?.addEventListener('change', renderResults);
  dom.resultsRegionFilter?.addEventListener('input', renderResults);
  dom.resultsWorkModeFilter?.addEventListener('change', renderResults);
  dom.resultsPostedFilter?.addEventListener('change', renderResults);
  dom.premiumCountryFilter?.addEventListener('change', renderPremiumResults);
  dom.premiumRegionFilter?.addEventListener('input', renderPremiumResults);
  dom.premiumWorkModeFilter?.addEventListener('change', renderPremiumResults);
  dom.premiumPostedFilter?.addEventListener('change', renderPremiumResults);
  dom.locationMode?.addEventListener('change', syncCountryMode);
  dom.runTabs.forEach((btn) => btn.addEventListener('click', () => setRunTab(btn.dataset.runTab || 'free')));
  dom.countrySearchInput?.addEventListener('input', () => {
    const query = dom.countrySearchInput.value.trim().toLowerCase();
    const selectedSet = new Set(selectedCountries);
    dom.countryPicker.innerHTML = '<option value="">Choose a country</option>';
    COUNTRY_OPTIONS.filter((country) => !selectedSet.has(country) && country.toLowerCase().includes(query)).forEach((country) => {
      const opt = document.createElement('option');
      opt.value = country;
      opt.textContent = country;
      dom.countryPicker.appendChild(opt);
    });
  });
  dom.addCountryBtn?.addEventListener('click', () => {
    const value = dom.countryPicker.value;
    if (!value || selectedCountries.includes(value)) return;
    selectedCountries = [...selectedCountries, value].sort((a, b) => a.localeCompare(b));
    dom.countrySearchInput.value = '';
    initCountryPicker();
    renderSelectedCountries();
    syncCountryMode();
  });
}

async function boot() {
  resetPipelineDisplay('free');
  resetPipelineDisplay('premium');
  initCountryPicker();
  renderSelectedCountries();
  syncCountryMode();
  setRunTab('free');
  switchView(currentViewFromHash());
  attachEvents();

  try {
    supabaseClient = await getSupabaseClient();
    await refreshAll();
    supabaseClient.auth.onAuthStateChange(async (_event, session) => {
      updateSessionUi(session || null);
      await loadProfile();
      await loadResumes();
      await loadDashboardStats();
      await loadResultsData();
    });
  } catch (err) {
    showMessage(dom.globalMessage, err.message || 'Supabase is not ready yet.', 'error');
  }
}

boot();
