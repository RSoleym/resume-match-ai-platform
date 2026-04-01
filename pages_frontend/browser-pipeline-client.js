(function () {
  const WORKER_URL = '/browser-pipeline-worker.js';
  const JOBS_ENDPOINT = '/api/jobs-candidate-set';
  const PREMIUM_UNLOCK_ENDPOINT = '/api/premium-unlock';
  const PREMIUM_RUN_ENDPOINT = '/api/premium-run';
  const PDFJS_URL = 'https://cdn.jsdelivr.net/npm/pdfjs-dist@4.4.168/build/pdf.min.mjs';
  const PDFJS_WORKER_URL = 'https://cdn.jsdelivr.net/npm/pdfjs-dist@4.4.168/build/pdf.worker.min.mjs';

  let browserWorker = null;
  let pdfjsPromise = null;
  let requestId = 0;
  const pending = new Map();

  function getWorker() {
    if (browserWorker) return browserWorker;
    browserWorker = new Worker(WORKER_URL, { type: 'module' });
    browserWorker.onmessage = (event) => {
      const { id, type, payload, error } = event.data || {};
      const entry = pending.get(id);
      if (!entry) return;
      if (type === 'progress') {
        entry.onProgress?.(payload || {});
        return;
      }
      pending.delete(id);
      if (type === 'result') {
        entry.resolve(payload);
      } else {
        entry.reject(new Error(error || 'Worker request failed.'));
      }
    };
    return browserWorker;
  }

  function callWorker(type, payload, onProgress) {
    return new Promise((resolve, reject) => {
      const id = `req_${Date.now()}_${++requestId}`;
      pending.set(id, { resolve, reject, onProgress });
      getWorker().postMessage({ id, type, payload });
    });
  }

  async function loadPdfJs() {
    if (!pdfjsPromise) {
      pdfjsPromise = import(PDFJS_URL).then((pdfjsLib) => {
        pdfjsLib.GlobalWorkerOptions.workerSrc = PDFJS_WORKER_URL;
        return pdfjsLib;
      });
    }
    return pdfjsPromise;
  }

  async function blobToDataUrl(blob) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ''));
      reader.onerror = () => reject(reader.error || new Error('Could not read file.'));
      reader.readAsDataURL(blob);
    });
  }

  async function prepareResumePayload(blob, fileName, onProgress) {
    const lowerName = String(fileName || '').toLowerCase();
    const fileType = blob.type || '';

    if (fileType.startsWith('image/') || /\.(png|jpe?g|webp|tiff?)$/i.test(lowerName)) {
      onProgress?.({ message: 'Preparing image for browser OCR…', progress: 12 });
      const imageDataUrl = await blobToDataUrl(blob);
      return { directText: '', ocrImages: [imageDataUrl], kind: 'image' };
    }

    if (/\.pdf$/i.test(lowerName) || fileType === 'application/pdf') {
      onProgress?.({ message: 'Reading PDF in the browser…', progress: 10 });
      return extractPdfPayload(blob, onProgress);
    }

    throw new Error('Unsupported resume file type. Upload a PDF or image.');
  }

  async function extractPdfPayload(blob, onProgress) {
    const pdfjsLib = await loadPdfJs();
    const bytes = new Uint8Array(await blob.arrayBuffer());
    const pdf = await pdfjsLib.getDocument({ data: bytes }).promise;
    const textChunks = [];
    const ocrImages = [];
    const maxOcrPages = Math.min(3, pdf.numPages);

    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
      onProgress?.({ message: `Reading PDF page ${pageNumber} of ${pdf.numPages}…`, progress: 10 + Math.round((pageNumber / Math.max(1, pdf.numPages)) * 18) });
      const page = await pdf.getPage(pageNumber);
      const textContent = await page.getTextContent();
      const pageText = textContent.items.map((item) => item.str || '').join(' ');
      textChunks.push(pageText);
    }

    const directText = textChunks.join('\n').replace(/\s+/g, ' ').trim();
    if (directText.length >= 350) {
      return { directText, ocrImages, kind: 'pdf-text' };
    }

    for (let pageNumber = 1; pageNumber <= maxOcrPages; pageNumber += 1) {
      onProgress?.({ message: `Rendering PDF page ${pageNumber} for browser OCR…`, progress: 30 + Math.round((pageNumber / Math.max(1, maxOcrPages)) * 12) });
      const page = await pdf.getPage(pageNumber);
      const viewport = page.getViewport({ scale: 1.7 });
      const canvas = document.createElement('canvas');
      canvas.width = Math.ceil(viewport.width);
      canvas.height = Math.ceil(viewport.height);
      const context = canvas.getContext('2d', { willReadFrequently: true });
      await page.render({ canvasContext: context, viewport }).promise;
      ocrImages.push(canvas.toDataURL('image/png'));
    }

    return { directText, ocrImages, kind: 'pdf-ocr' };
  }

  async function downloadResumeBlob(supabaseClient, resumeRow) {
    if (!resumeRow?.storage_path) throw new Error('Resume storage path is missing.');
    const { data, error } = await supabaseClient.storage.from('resumes').download(resumeRow.storage_path);
    if (error) throw error;
    if (!data) throw new Error('Resume download returned no file.');
    return data;
  }

  async function analyzeResumeFromStorage({ supabaseClient, resumeRow, onProgress }) {
    const existingText = String(resumeRow?.parsed_text || '').trim();
    const existingProfile = {
      candidate_country: resumeRow?.candidate_country || '',
      candidate_experience_years: resumeRow?.candidate_experience_years ?? null,
      candidate_degree_level: resumeRow?.candidate_degree_level || '',
      candidate_degree_family: resumeRow?.candidate_degree_family || '',
      candidate_degree_fields: resumeRow?.candidate_degree_fields || [],
      candidate_function: resumeRow?.candidate_function || '',
      candidate_domain: resumeRow?.candidate_domain || '',
      candidate_category_key: resumeRow?.candidate_category_key || '',
      candidate_category_confidence: resumeRow?.candidate_category_confidence ?? null,
      candidate_category_scores: resumeRow?.candidate_category_scores || {},
    };

    const hasStoredProfile = existingText.length > 200 && (existingProfile.candidate_function || existingProfile.candidate_domain || existingProfile.candidate_category_key);
    if (hasStoredProfile) {
      onProgress?.({ message: 'Using saved resume text and profile.', progress: 18 });
      return { parsedText: existingText, profile: existingProfile, diagnostics: { reusedStoredResume: true } };
    }

    const blob = await downloadResumeBlob(supabaseClient, resumeRow);
    const prepared = await prepareResumePayload(blob, resumeRow.file_name || resumeRow.storage_path || 'resume.pdf', onProgress);
    return callWorker('analyze_resume', prepared, onProgress);
  }

  async function saveResumeAnalysis({ supabaseClient, resumeRow, parsedText, profile }) {
    const payload = {
      parsed_text: parsedText,
      candidate_country: profile.candidate_country || null,
      candidate_experience_years: profile.candidate_experience_years ?? null,
      candidate_degree_level: profile.candidate_degree_level || null,
      candidate_degree_family: profile.candidate_degree_family || null,
      candidate_degree_fields: Array.isArray(profile.candidate_degree_fields) ? profile.candidate_degree_fields : [],
      candidate_function: profile.candidate_function || null,
      candidate_domain: profile.candidate_domain || null,
      candidate_category_key: profile.candidate_category_key || null,
      candidate_category_confidence: profile.candidate_category_confidence ?? null,
      candidate_category_scores: profile.candidate_category_scores || profile.candidate_function_scores || {},
    };

    const { error } = await supabaseClient.from('resumes').update(payload).eq('id', resumeRow.id);
    if (error) throw error;
  }

  async function fetchCandidateJobs({ filters, onProgress }) {
    const params = new URLSearchParams();
    if (filters.countryMode) params.set('countryMode', filters.countryMode);
    if (filters.selectedCountries?.length) params.set('selectedCountries', filters.selectedCountries.join(','));
    if (filters.country) params.set('country', filters.country);
    if (filters.workMode) params.set('workMode', filters.workMode);
    if (filters.posted) params.set('posted', filters.posted);
    if (filters.limit) params.set('limit', String(filters.limit));

    onProgress?.({ message: 'Fetching candidate jobs…', progress: 52 });
    const response = await fetch(`${JOBS_ENDPOINT}?${params.toString()}`, { cache: 'no-store' });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload?.error || 'Could not load candidate jobs.');
    }
    return Array.isArray(payload.jobs) ? payload.jobs : [];
  }

  async function saveFreeResults({ supabaseClient, session, resumeId, results, filters }) {
    await supabaseClient.from('match_results').delete().eq('resume_id', resumeId).eq('user_id', session.user.id);
    const { error } = await supabaseClient.from('match_results').insert({
      user_id: session.user.id,
      resume_id: resumeId,
      results_json: results,
      filters_json: filters || {},
    });
    if (error) {
      const fallback = await supabaseClient.from('match_results').insert({
        user_id: session.user.id,
        resume_id: resumeId,
        results_json: results,
      });
      if (fallback.error) throw fallback.error;
    }
  }

  async function savePremiumResults({ supabaseClient, session, resumeId, results, filters }) {
    await supabaseClient.from('premium_match_results').delete().eq('resume_id', resumeId).eq('user_id', session.user.id);
    const { error } = await supabaseClient.from('premium_match_results').insert({
      user_id: session.user.id,
      resume_id: resumeId,
      filters_json: filters || {},
      results_json: results,
    });
    if (error) throw error;
  }

  async function getAccessToken(supabaseClient) {
    const { data, error } = await supabaseClient.auth.getSession();
    if (error) throw error;
    const token = data?.session?.access_token || '';
    if (!token) throw new Error('Sign in again before running this.');
    return token;
  }

  async function runFreePipeline({ supabaseClient, session, resumeRow, locationMode, selectedCountries, onProgress }) {
    if (!session?.user?.id) throw new Error('Sign in before running the browser pipeline.');
    if (!resumeRow?.id) throw new Error('Upload a resume first.');

    const analysis = await analyzeResumeFromStorage({ supabaseClient, resumeRow, onProgress });
    await saveResumeAnalysis({ supabaseClient, resumeRow, parsedText: analysis.parsedText, profile: analysis.profile });

    const countryMode = locationMode || 'current';
    let scopeCountries = [];
    if (countryMode === 'selected') {
      scopeCountries = Array.isArray(selectedCountries) ? selectedCountries.slice() : [];
      if (!scopeCountries.length) throw new Error('Choose at least one country.');
    } else if (countryMode === 'current' && analysis.profile?.candidate_country) {
      scopeCountries = [analysis.profile.candidate_country];
    }

    const jobs = await fetchCandidateJobs({
      filters: { countryMode, selectedCountries: scopeCountries, limit: 800 },
      onProgress,
    });

    const scoring = await callWorker('match_jobs', {
      resumeText: analysis.parsedText,
      resumeProfile: analysis.profile,
      jobs,
      topK: 100,
      semanticTopK: 40,
    }, onProgress);

    await saveFreeResults({
      supabaseClient,
      session,
      resumeId: resumeRow.id,
      results: scoring.results,
      filters: { countryMode, selectedCountries: scopeCountries },
    });

    return {
      results: scoring.results,
      resumeProfile: analysis.profile,
      diagnostics: scoring.diagnostics,
    };
  }

  async function unlockPremium({ supabaseClient, code }) {
    const token = await getAccessToken(supabaseClient);
    const response = await fetch(PREMIUM_UNLOCK_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ code }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload?.error || 'Premium unlock failed.');
    return payload;
  }

  async function runPremiumPipeline({ supabaseClient, session, resumeRow, filters, onProgress }) {
    if (!session?.user?.id) throw new Error('Sign in before running premium.');
    if (!resumeRow?.id) throw new Error('Upload a resume first.');

    const analysis = await analyzeResumeFromStorage({ supabaseClient, resumeRow, onProgress });
    await saveResumeAnalysis({ supabaseClient, resumeRow, parsedText: analysis.parsedText, profile: analysis.profile });

    const jobs = await fetchCandidateJobs({
      filters: {
        countryMode: filters.country ? 'selected' : 'all',
        selectedCountries: filters.country ? [filters.country] : [],
        country: filters.country || '',
        workMode: filters.workMode || '',
        posted: filters.posted || 'all',
        limit: 500,
      },
      onProgress,
    });

    const regionQuery = String(filters.region || '').trim().toLowerCase();
    const scopedJobs = regionQuery
      ? jobs.filter((job) => `${job.location || ''} ${job.title || ''}`.toLowerCase().includes(regionQuery))
      : jobs;

    const localPreRank = await callWorker('match_jobs', {
      jobs: scopedJobs,
      resumeText: analysis.parsedText,
      resumeProfile: analysis.profile,
      topK: 30,
      semanticTopK: 20,
    }, onProgress);

    const token = await getAccessToken(supabaseClient);
    onProgress?.({ message: 'Sending top jobs to the premium backend…', progress: 88 });
    const response = await fetch(PREMIUM_RUN_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        resumeId: resumeRow.id,
        filters,
        resumeContext: {
          ...analysis.profile,
          resume_text: analysis.parsedText,
        },
        candidateJobs: localPreRank.results,
      }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload?.error || 'Premium run failed.');

    await savePremiumResults({
      supabaseClient,
      session,
      resumeId: resumeRow.id,
      results: Array.isArray(payload.results) ? payload.results : [],
      filters,
    });

    onProgress?.({ message: 'Premium results saved.', progress: 100 });
    return payload;
  }

  window.ResumeBrowserPipeline = {
    runFreePipeline,
    unlockPremium,
    runPremiumPipeline,
  };
})();
