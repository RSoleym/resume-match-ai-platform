# Pages Frontend

This folder is the deployed website path.

## What runs here now

### Browser-side heavy pipeline
- `browser-pipeline-client.js` coordinates browser PDF reading, OCR, local scoring, and saving results
- `browser-pipeline-worker.js` runs the heavy local worker tasks on the user's device

### Thin backend endpoints
- `/api/public-config` — frontend Supabase config
- `/api/dashboard-stats` — dashboard stats
- `/api/jobs-candidate-set` — lightweight candidate job fetch for browser scoring
- `/api/premium-unlock` — secure premium code unlock
- `/api/premium-run` — secure premium rerank using backend secrets

## Deployment notes

Cloudflare Pages root directory:
- `pages_frontend`

Build command:
- leave blank

Build output directory:
- `.`
