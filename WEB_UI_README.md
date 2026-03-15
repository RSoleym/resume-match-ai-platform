# Job Finder – Updated Website v5

Changes:
- Start runs Resume OCR + Job Matcher together
- Job scraping removed from the website UI
- Multiple resumes are preserved; results can be switched by resume on Results page
- Smooth loading bar kept
- Fixed server hang from lock re-entry
- Added required Python packages for resume_scraper.py + job_matcher.py inside the website venv
- If a run fails, the page now shows the actual stderr error

Important:
- resume_scraper.py still needs Tesseract OCR and Poppler installed on Windows.
- If those are missing, the run page will now show the exact error.

Run: double-click RUN_WEBSITE.bat
Open: http://127.0.0.1:5000

- Results page now supports posted date filters (Past week / Past month / All time)
- Results page now supports viewing match ranges 1-10, 11-20, ... up to 500
