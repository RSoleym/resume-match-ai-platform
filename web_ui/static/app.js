(function () {
  const fileInput = document.getElementById("fileInput");
  const previewBox = document.getElementById("previewBox");
  const imgPreview = document.getElementById("imgPreview");
  const fileName = document.getElementById("fileName");

  if (fileInput && previewBox && imgPreview && fileName) {
    fileInput.addEventListener("change", () => {
      const f = fileInput.files && fileInput.files[0];
      if (!f) return;
      fileName.textContent = f.name;
      const isImage = f.type && f.type.startsWith("image/");
      if (isImage) {
        const url = URL.createObjectURL(f);
        imgPreview.src = url;
        imgPreview.classList.remove("hidden");
      } else {
        imgPreview.removeAttribute("src");
        imgPreview.classList.add("hidden");
      }
      previewBox.classList.remove("hidden");
    });
  }

  const elMsg = document.getElementById("statusMessage");
  const elStep = document.getElementById("statusStep");
  const elElapsed = document.getElementById("statusElapsed");
  const elPct = document.getElementById("statusPct");
  const elBar = document.getElementById("statusBar");
  const elHint = document.getElementById("statusHint");
  const errBox = document.getElementById("statusErrorBox");
  const errText = document.getElementById("statusErrorText");

  function fmtElapsed(s) {
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    const r = s % 60;
    return `${m}m ${r}s`;
  }

  function setBarAlive(isRunning) {
    if (!elBar) return;
    if (isRunning) elBar.classList.add("animate-pulse");
    else elBar.classList.remove("animate-pulse");
  }

  async function pollStatus() {
    try {
      const s = await fetch("/api/status", { cache: "no-store" }).then(r => r.json());

      const running = !!s.running;
      const msg = s.message || (running ? "Running" : "Idle");
      const step = s.current_step || "—";
      const elapsed = s.elapsed_s || 0;

      let pct = typeof s.progress_pct_smooth === "number" ? s.progress_pct_smooth : 0;
      if (!Number.isFinite(pct)) pct = 0;
      pct = Math.max(0, Math.min(100, pct));
      if (running && pct >= 100) pct = 99;

      if (elMsg) elMsg.textContent = msg;
      if (elStep) elStep.textContent = step;
      if (elElapsed) elElapsed.textContent = fmtElapsed(elapsed);
      if (elPct) elPct.textContent = `${Math.floor(pct)}%`;
      if (elBar) elBar.style.width = `${pct}%`;
      setBarAlive(running);

      if (elHint) {
        if (running) {
          elHint.textContent = "Running Resume OCR + Job Matcher… you’ll be redirected to Results when it finishes.";
        } else {
          elHint.textContent = "";
        }
      }

      if (!running && typeof msg === "string" && msg.toLowerCase().startsWith("failed")) {
        if (errBox) errBox.classList.remove("hidden");
        const detail = (s.error_detail || "").trim();
        if (errText) errText.textContent = detail ? detail : (s.error || "Unknown error");
      } else {
        if (errBox) errBox.classList.add("hidden");
        if (errText) errText.textContent = "";
      }

      if (!running && typeof msg === "string" && msg.toLowerCase().startsWith("finished")) {
        if (elHint) {
          elHint.textContent = "Finished. Open the Results page when you are ready.";
        }
      }
    } catch (e) {}
  }

  if (elMsg && elBar) {
    pollStatus();
    setInterval(pollStatus, 900);
  }
})();