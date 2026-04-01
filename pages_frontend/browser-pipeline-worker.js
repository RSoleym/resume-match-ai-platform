let semanticExtractor = null;
let semanticExtractorPromise = null;

const STOP_WORDS = new Set([
  'a','an','and','are','as','at','be','but','by','for','from','has','have','if','in','into','is','it','its','of','on','or','our','that','the','their','this','to','was','we','with','you','your','will','can','may','using','use','used','work','working','experience','project','projects','skills','skill'
]);

const FUNCTION_KEYWORDS = {
  'Hardware / RTL / Verification': ['systemverilog','verilog','rtl','uvm','asic','fpga','design verification','digital design','vivado','quartus','timing','sva','formal','silicon','cpu','gpu','pcie','serdes'],
  'Embedded / Firmware': ['embedded','firmware','microcontroller','bare metal','rtos','device driver','cortex-m','stm32','uart','spi','i2c','freertos'],
  'Software Engineering': ['python','java','c++','typescript','javascript','react','node','backend','frontend','full stack','api','sql'],
  'Data / AI / ML': ['machine learning','deep learning','pytorch','tensorflow','nlp','computer vision','data science','transformer','embedding','llm'],
  'Electrical / Power / Controls': ['power systems','substation','scada','plc','controls','protection','hv','high voltage','grid'],
  'Finance / Accounting': ['accounts payable','accounts receivable','reconciliation','journal entry','general ledger','quickbooks','invoice','accounting']
};

const DOMAIN_KEYWORDS = {
  'Semiconductors / Silicon / ASIC / FPGA': ['asic','fpga','silicon','cpu','gpu','pcie','serdes','vlsi','soc','verification','rtl'],
  'Datacenter / Cloud / Infrastructure': ['cloud','distributed systems','infrastructure','linux','server','kubernetes','docker'],
  'Web / Mobile / Product Applications': ['react','frontend','backend','node','mobile','ios','android','web'],
  'AI / ML / Data': ['machine learning','data science','nlp','llm','analytics','vision','embedding'],
  'Power / Energy / Industrial Controls': ['power systems','grid','renewable','solar','substation','scada','plc'],
  'Finance / Accounting / ERP': ['accounting','finance','erp','quickbooks','payroll','audit'],
  'General': []
};

const DEGREE_KEYWORDS = {
  phd: ['phd','ph.d','doctor of philosophy','doctoral'],
  masters: ['master of','m.eng','m.sc','msc','masc','meng','ms '],
  bachelors: ['bachelor of','b.eng','bsc','b.sc','basc','b.eng.','bs '],
  diploma: ['diploma','college diploma','technician diploma']
};

const SKILL_BANK = [
  'systemverilog','verilog','uvm','rtl','asic','fpga','vivado','quartus','python','c++','c','javascript','typescript','react','node','sql','linux',
  'machine learning','pytorch','tensorflow','nlp','opencv','matlab','git','docker','kubernetes','pcb','firmware','embedded','microcontroller','uart','spi','i2c',
  'quickbooks','excel','reconciliation','accounts payable','accounts receivable','journal entries','power systems','scada','plc'
];

self.onmessage = async (event) => {
  const { id, type, payload } = event.data || {};
  if (!id || !type) return;
  try {
    if (type === 'analyze_resume') {
      const result = await analyzeResume(payload || {}, (message, progress) => emitProgress(id, message, progress));
      postMessage({ id, type: 'result', payload: result });
      return;
    }
    if (type === 'match_jobs') {
      const result = await matchJobs(payload || {}, (message, progress) => emitProgress(id, message, progress));
      postMessage({ id, type: 'result', payload: result });
      return;
    }
    throw new Error(`Unknown worker task: ${type}`);
  } catch (error) {
    postMessage({ id, type: 'error', error: error?.message || 'Worker task failed.' });
  }
};

function emitProgress(id, message, progress = null) {
  postMessage({ id, type: 'progress', payload: { message, progress } });
}

async function analyzeResume(payload, progress) {
  const directText = String(payload.directText || '').trim();
  let parsedText = directText;
  let ocrUsed = false;

  if (parsedText.length < 200 && Array.isArray(payload.ocrImages) && payload.ocrImages.length) {
    progress('Running OCR in the browser…', 22);
    const ocrText = await runBrowserOcr(payload.ocrImages, progress);
    if (ocrText.trim().length > parsedText.length) {
      parsedText = ocrText.trim();
      ocrUsed = true;
    }
  }

  if (!parsedText.trim()) {
    throw new Error('Could not read any resume text from this file.');
  }

  progress('Inferring resume profile…', 48);
  const profile = inferResumeProfile(parsedText);
  return {
    parsedText,
    ocrUsed,
    profile,
    diagnostics: {
      textLength: parsedText.length,
      ocrPageCount: Array.isArray(payload.ocrImages) ? payload.ocrImages.length : 0,
    },
  };
}

async function runBrowserOcr(images, progress) {
  const mod = await import('https://cdn.jsdelivr.net/npm/tesseract.js@5/+esm');
  const createWorker = mod.createWorker || mod.default?.createWorker;
  if (typeof createWorker !== 'function') {
    throw new Error('Could not load the browser OCR engine.');
  }

  const worker = await createWorker('eng');
  let text = '';
  try {
    for (let i = 0; i < images.length; i += 1) {
      progress(`OCR page ${i + 1} of ${images.length}…`, 24 + Math.round(((i + 1) / Math.max(1, images.length)) * 18));
      const result = await worker.recognize(images[i]);
      text += `\n${result?.data?.text || ''}`;
    }
  } finally {
    if (worker?.terminate) {
      await worker.terminate();
    }
  }
  return text;
}

function inferResumeProfile(text) {
  const normalized = normalizeText(text);
  const functionScores = keywordScores(normalized, FUNCTION_KEYWORDS);
  const domainScores = keywordScores(normalized, DOMAIN_KEYWORDS);
  const candidateFunction = bestLabel(functionScores) || 'Software Engineering';
  const candidateDomain = bestLabel(domainScores) || 'General';
  const candidateCategoryKey = candidateFunction;
  const candidateCategoryConfidence = Math.round((Math.max(...Object.values(functionScores), 0)) * 1000) / 1000;
  const candidateDegreeLevel = inferDegreeLevel(normalized);
  const candidateDegreeFamily = inferDegreeFamily(normalized);
  const candidateDegreeFields = inferDegreeFields(normalized);
  const candidateExperienceYears = inferExperienceYears(normalized);
  const candidateCountry = inferCountry(normalized);
  const skills = extractSkills(normalized);

  return {
    candidate_country: candidateCountry,
    candidate_experience_years: candidateExperienceYears,
    candidate_degree_level: candidateDegreeLevel,
    candidate_degree_family: candidateDegreeFamily,
    candidate_degree_fields: candidateDegreeFields,
    candidate_function: candidateFunction,
    candidate_domain: candidateDomain,
    candidate_category_key: candidateCategoryKey,
    candidate_category_confidence: candidateCategoryConfidence,
    candidate_category_scores: functionScores,
    candidate_function_scores: functionScores,
    candidate_domain_scores: domainScores,
    skills,
    summary_excerpt: textSummary(normalized),
  };
}

function keywordScores(text, map) {
  const out = {};
  for (const [label, terms] of Object.entries(map)) {
    let score = 0;
    for (const term of terms) {
      if (text.includes(term)) score += term.includes(' ') ? 2.0 : 1.0;
    }
    out[label] = score;
  }
  const max = Math.max(...Object.values(out), 0);
  if (max <= 0) return out;
  for (const key of Object.keys(out)) {
    out[key] = Number((out[key] / max).toFixed(6));
  }
  return out;
}

function bestLabel(scoreMap) {
  let best = '';
  let bestScore = -1;
  for (const [label, score] of Object.entries(scoreMap || {})) {
    if (Number(score) > bestScore) {
      best = label;
      bestScore = Number(score);
    }
  }
  return best;
}

function inferDegreeLevel(text) {
  for (const [label, variants] of Object.entries(DEGREE_KEYWORDS)) {
    if (variants.some((variant) => text.includes(variant))) return label;
  }
  return '';
}

function inferDegreeFamily(text) {
  const families = {
    'Electrical / Computer Engineering': ['electrical engineering','computer engineering','electrical and computer engineering','ee'],
    'Computer Science / Software': ['computer science','software engineering','software development'],
    'Accounting / Finance': ['accounting','finance','bookkeeping'],
    'Mechanical / Civil': ['mechanical engineering','civil engineering','construction engineering'],
  };
  for (const [family, terms] of Object.entries(families)) {
    if (terms.some((term) => text.includes(term))) return family;
  }
  return '';
}

function inferDegreeFields(text) {
  const fields = ['electrical engineering','computer engineering','computer science','software engineering','accounting','finance','mechanical engineering','civil engineering'];
  return fields.filter((field) => text.includes(field));
}

function inferExperienceYears(text) {
  const explicit = [...text.matchAll(/(\d{1,2})\+?\s+years?/g)].map((match) => Number(match[1])).filter(Number.isFinite);
  if (explicit.length) return Math.max(...explicit);
  const yearMatches = [...text.matchAll(/\b(20\d{2})\b/g)].map((match) => Number(match[1])).filter((year) => year >= 2000 && year <= new Date().getFullYear());
  if (!yearMatches.length) return null;
  const earliest = Math.min(...yearMatches);
  const guessed = Math.max(0, new Date().getFullYear() - earliest);
  return guessed > 12 ? 12 : guessed;
}

function inferCountry(text) {
  const aliases = [
    ['Canada', [' canada',' toronto','ontario','vancouver','calgary','montreal']],
    ['USA', [' united states',' usa',' u.s.','new york','california','texas','seattle']],
    ['United Kingdom', [' united kingdom',' uk ',' london','manchester']],
    ['India', [' india',' bengaluru','bangalore','hyderabad']],
    ['Germany', [' germany',' munich','berlin']],
    ['Israel', [' israel',' tel aviv']],
  ];
  const padded = ` ${text} `;
  for (const [country, terms] of aliases) {
    if (terms.some((term) => padded.includes(term))) return country;
  }
  return '';
}

function extractSkills(text) {
  return SKILL_BANK.filter((skill) => text.includes(skill));
}

function textSummary(text) {
  return text.replace(/\s+/g, ' ').slice(0, 500);
}

async function matchJobs(payload, progress) {
  const jobs = Array.isArray(payload.jobs) ? payload.jobs : [];
  if (!jobs.length) {
    return { results: [], diagnostics: { semanticUsed: false, candidateCount: 0 } };
  }

  const resumeText = normalizeText(payload.resumeText || '');
  const resumeProfile = payload.resumeProfile || {};
  const resumeTokens = tokenBag(resumeText);
  const resumeSkills = new Set(Array.isArray(resumeProfile.skills) ? resumeProfile.skills : []);

  progress(`Scoring ${jobs.length} jobs locally…`, 56);
  const scored = jobs.map((job, index) => scoreJob(job, index, resumeText, resumeTokens, resumeProfile, resumeSkills));
  scored.sort((a, b) => (b.prefilter_score || 0) - (a.prefilter_score || 0));

  const semanticLimit = Math.min(Number(payload.semanticTopK || 40), scored.length);
  let semanticUsed = false;
  if (semanticLimit > 0) {
    try {
      progress(`Loading local semantic model for top ${semanticLimit} jobs…`, 70);
      const reranked = await applySemanticRerank(resumeText, scored.slice(0, semanticLimit), progress);
      const byId = new Map(reranked.map((row) => [row.job_id, row]));
      for (let i = 0; i < scored.length; i += 1) {
        const hit = byId.get(scored[i].job_id);
        if (hit) scored[i] = hit;
      }
      semanticUsed = true;
    } catch (_error) {
      semanticUsed = false;
    }
  }

  scored.sort((a, b) => (b.final_match_percent || 0) - (a.final_match_percent || 0));
  const topK = Math.min(Number(payload.topK || 100), scored.length);
  const results = scored.slice(0, topK).map((row, rank) => ({ ...row, rank: rank + 1 }));

  progress('Browser scoring complete.', 100);
  return {
    results,
    diagnostics: {
      semanticUsed,
      candidateCount: jobs.length,
      returnedCount: results.length,
    },
  };
}

function scoreJob(job, index, resumeText, resumeTokens, resumeProfile, resumeSkills) {
  const text = jobText(job);
  const tokens = tokenBag(text);
  const lexical = cosineFromTokenBags(resumeTokens, tokens);
  const overlap = overlapScore(resumeTokens, tokens);
  const skillScore = skillOverlapScore(resumeSkills, text);
  const categoryScore = categoryCompatibility(resumeProfile, job);
  const degreeScore = degreeCompatibility(resumeProfile, job);
  const experienceScore = experienceCompatibility(resumeProfile, job);

  const raw = clamp((lexical * 0.45) + (overlap * 0.15) + (skillScore * 0.16) + (categoryScore * 0.12) + (degreeScore * 0.06) + (experienceScore * 0.06), 0, 1);
  let penaltyPoints = 0;

  if (degreeScore < 0.35 && String(job.degree_level_min || '').trim()) penaltyPoints += 8;
  if (experienceScore < 0.35 && job.experience_needed_years !== null && job.experience_needed_years !== undefined && job.experience_needed_years !== '') penaltyPoints += 10;
  if (categoryScore < 0.22 && String(job.job_category_key || job.job_function || '').trim()) penaltyPoints += 14;

  const finalPct = clamp((raw * 100) - penaltyPoints, 0, 100);
  return {
    job_id: String(job.job_id || `${job.title || 'job'}-${index}`),
    title: String(job.title || 'Untitled role'),
    company: String(job.company || 'Unknown company'),
    location: String(job.location || ''),
    country: String(job.country || ''),
    region: inferRegion(job.location || ''),
    work_mode: normalizeWorkMode(job.work_mode || ''),
    posted_date_display: String(job.posted_date || 'Unknown'),
    best_url: String(job.source_url || job.url || job.job_url || ''),
    description_text: String(job.description_text || job.description || '').slice(0, 1600),
    job_function: String(job.job_function || ''),
    job_domain: String(job.job_domain || ''),
    job_category_key: String(job.job_category_key || ''),
    raw_match_percent: Math.round(raw * 100),
    final_match_percent: Math.round(finalPct),
    penalty_applied: penaltyPoints > 0,
    penalty_points: penaltyPoints,
    prefilter_score: raw,
    lexical_score: lexical,
    skill_score: skillScore,
    category_score: categoryScore,
    degree_score: degreeScore,
    experience_score: experienceScore,
    title_text: text,
  };
}

async function applySemanticRerank(resumeText, rows, progress) {
  const extractor = await getSemanticExtractor();
  if (!extractor) throw new Error('Semantic model unavailable.');

  const candidateTexts = rows.map((row) => row.title_text.slice(0, 2500));
  progress('Running local embedding rerank…', 82);
  const [resumeVec] = await embedTexts(extractor, [resumeText.slice(0, 2500)]);
  const jobVecs = await embedTexts(extractor, candidateTexts);

  return rows.map((row, idx) => {
    const semantic = cosineVectors(resumeVec, jobVecs[idx]);
    const finalPct = clamp((row.prefilter_score * 35) + (semantic * 65) - (row.penalty_points || 0), 0, 100);
    return {
      ...row,
      raw_match_percent: Math.round(semantic * 100),
      final_match_percent: Math.round(finalPct),
    };
  });
}

async function getSemanticExtractor() {
  if (semanticExtractor) return semanticExtractor;
  if (semanticExtractorPromise) return semanticExtractorPromise;
  semanticExtractorPromise = (async () => {
    const mod = await import('https://cdn.jsdelivr.net/npm/@xenova/transformers@2.17.2');
    mod.env.allowLocalModels = false;
    mod.env.useBrowserCache = true;
    const extractor = await mod.pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
    semanticExtractor = extractor;
    return extractor;
  })();
  try {
    return await semanticExtractorPromise;
  } finally {
    semanticExtractorPromise = null;
  }
}

async function embedTexts(extractor, texts) {
  const output = await extractor(texts, { pooling: 'mean', normalize: true });
  if (typeof output.tolist === 'function') {
    return output.tolist();
  }
  if (Array.isArray(output)) return output;
  if (output?.data && Array.isArray(output.dims) && output.dims.length >= 2) {
    const [rows, cols] = output.dims;
    const vectors = [];
    for (let r = 0; r < rows; r += 1) {
      vectors.push(Array.from(output.data.slice(r * cols, (r + 1) * cols)));
    }
    return vectors;
  }
  throw new Error('Unexpected embedding output.');
}

function jobText(job) {
  return normalizeText([
    job.title,
    job.company,
    job.location,
    job.country,
    job.work_mode,
    job.job_function,
    job.job_domain,
    job.job_category_key,
    job.degree_level_min,
    job.degree_family,
    job.description_text || job.description,
  ].filter(Boolean).join(' '));
}

function tokenBag(text) {
  const out = new Map();
  for (const token of tokenize(text)) {
    out.set(token, (out.get(token) || 0) + 1);
  }
  return out;
}

function tokenize(text) {
  return normalizeText(text)
    .split(/[^a-z0-9+#.-]+/)
    .map((token) => token.trim())
    .filter((token) => token && token.length > 1 && !STOP_WORDS.has(token));
}

function cosineFromTokenBags(a, b) {
  let dot = 0;
  let magA = 0;
  let magB = 0;
  for (const value of a.values()) magA += value * value;
  for (const value of b.values()) magB += value * value;
  for (const [token, valueA] of a.entries()) {
    const valueB = b.get(token) || 0;
    dot += valueA * valueB;
  }
  if (!magA || !magB) return 0;
  return dot / (Math.sqrt(magA) * Math.sqrt(magB));
}

function overlapScore(a, b) {
  let overlap = 0;
  for (const token of a.keys()) {
    if (b.has(token)) overlap += 1;
  }
  return overlap / Math.max(20, a.size || 1);
}

function skillOverlapScore(resumeSkills, jobTextValue) {
  if (!resumeSkills.size) return 0;
  let hits = 0;
  for (const skill of resumeSkills) {
    if (jobTextValue.includes(skill)) hits += 1;
  }
  return hits / Math.max(4, resumeSkills.size);
}

function categoryCompatibility(profile, job) {
  const resumeCategory = String(profile.candidate_category_key || profile.candidate_function || '').trim();
  const jobCategory = String(job.job_category_key || job.job_function || '').trim();
  if (!resumeCategory || !jobCategory) return 0.55;
  if (resumeCategory === jobCategory) return 1;
  const resumeDomain = String(profile.candidate_domain || '').trim();
  const jobDomain = String(job.job_domain || '').trim();
  if (resumeDomain && jobDomain && resumeDomain === jobDomain) return 0.72;
  return 0.18;
}

function degreeCompatibility(profile, job) {
  const wanted = normalizeDegree(String(job.degree_level_min || ''));
  if (!wanted) return 0.65;
  const have = normalizeDegree(String(profile.candidate_degree_level || ''));
  if (!have) return 0.4;
  const rank = { diploma: 1, bachelors: 2, masters: 3, phd: 4 };
  return (rank[have] || 0) >= (rank[wanted] || 0) ? 1 : 0.15;
}

function normalizeDegree(value) {
  const text = normalizeText(value);
  if (!text) return '';
  if (text.includes('phd') || text.includes('doctoral')) return 'phd';
  if (text.includes('master')) return 'masters';
  if (text.includes('bachelor')) return 'bachelors';
  if (text.includes('diploma')) return 'diploma';
  return '';
}

function experienceCompatibility(profile, job) {
  const required = Number(job.experience_needed_years);
  if (!Number.isFinite(required)) return 0.7;
  const have = Number(profile.candidate_experience_years);
  if (!Number.isFinite(have)) return 0.45;
  if (have >= required) return 1;
  if (have + 1 >= required) return 0.72;
  if (have + 2 >= required) return 0.54;
  return 0.12;
}

function inferRegion(location) {
  const text = String(location || '').trim();
  if (!text) return '';
  return text.split(',')[0].trim();
}

function normalizeWorkMode(value) {
  const text = normalizeText(value);
  if (text.includes('remote')) return 'remote';
  if (text.includes('hybrid')) return 'hybrid';
  if (text.includes('on-site') || text.includes('onsite')) return 'on-site';
  return text ? value : 'on-site';
}

function normalizeText(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function cosineVectors(a, b) {
  let dot = 0;
  let magA = 0;
  let magB = 0;
  for (let i = 0; i < Math.min(a.length, b.length); i += 1) {
    const av = Number(a[i]) || 0;
    const bv = Number(b[i]) || 0;
    dot += av * bv;
    magA += av * av;
    magB += bv * bv;
  }
  if (!magA || !magB) return 0;
  return dot / (Math.sqrt(magA) * Math.sqrt(magB));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}
