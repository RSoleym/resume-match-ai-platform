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

  const runTabs = Array.from(document.querySelectorAll(".run-tab-btn"));
  const runPanels = Array.from(document.querySelectorAll(".run-tab-panel"));
  function setRunTab(tab) {
    runTabs.forEach((btn) => {
      const active = btn.dataset.runTab === tab;
      btn.classList.toggle("bg-indigo-600", active);
      btn.classList.toggle("text-white", active);
      btn.classList.toggle("bg-white/5", !active);
      btn.classList.toggle("text-slate-300", !active);
    });
    runPanels.forEach((panel) => {
      panel.classList.toggle("hidden", panel.id !== (tab === "premium" ? "runTabPremium" : "runTabFree"));
    });
  }
  if (runTabs.length) {
    const defaultTab = (window.defaultRunTab || "free").toLowerCase() === "premium" ? "premium" : "free";
    setRunTab(defaultTab);
    runTabs.forEach((btn) => btn.addEventListener("click", () => setRunTab(btn.dataset.runTab || "free")));
  }

  const locationMode = document.getElementById("locationMode");
  const selectedCountriesWrap = document.getElementById("selectedCountriesWrap");
  const countrySearchInput = document.getElementById("countrySearchInput");
  const countryPicker = document.getElementById("countryPicker");
  const addCountryBtn = document.getElementById("addCountryBtn");
  const selectedCountryChips = document.getElementById("selectedCountryChips");

  if (locationMode && selectedCountriesWrap) {
    const syncCountryMode = () => {
      selectedCountriesWrap.classList.toggle("hidden", locationMode.value !== "selected");
    };
    locationMode.addEventListener("change", syncCountryMode);
    syncCountryMode();
  }

  if (selectedCountryChips && countryPicker) {
    let selected = [];
    try {
      const raw = JSON.parse(selectedCountryChips.dataset.selected || "[]");
      if (Array.isArray(raw)) selected = raw.filter(Boolean);
    } catch (e) {}

    const allOptions = Array.from(countryPicker.querySelectorAll("option")).map((opt) => opt.value).filter(Boolean);

    const syncPickerOptions = () => {
      const query = ((countrySearchInput && countrySearchInput.value) || "").trim().toLowerCase();
      const currentValue = countryPicker.value;
      countryPicker.innerHTML = '<option value="">Choose a country</option>';
      allOptions.forEach((country) => {
        if (selected.includes(country)) return;
        if (query && !country.toLowerCase().includes(query)) return;
        const opt = document.createElement("option");
        opt.value = country;
        opt.textContent = country;
        if (country === currentValue) opt.selected = true;
        countryPicker.appendChild(opt);
      });
    };

    const renderSelectedCountries = () => {
      selectedCountryChips.innerHTML = "";
      if (selected.length === 0) {
        const empty = document.createElement("div");
        empty.className = "text-sm text-slate-500";
        empty.textContent = "No countries chosen yet.";
        selectedCountryChips.appendChild(empty);
      } else {
        selected.forEach((country) => {
          const chip = document.createElement("div");
          chip.className = "inline-flex items-center gap-2 rounded-full bg-white/10 px-3 py-1.5 text-sm ring-soft";
          const label = document.createElement("span");
          label.textContent = country;
          chip.appendChild(label);
          const removeBtn = document.createElement("button");
          removeBtn.type = "button";
          removeBtn.className = "text-slate-300 hover:text-white";
          removeBtn.textContent = "×";
          removeBtn.setAttribute("aria-label", `Remove ${country}`);
          removeBtn.addEventListener("click", () => {
            selected = selected.filter((item) => item !== country);
            renderSelectedCountries();
          });
          chip.appendChild(removeBtn);
          selectedCountryChips.appendChild(chip);
          const input = document.createElement("input");
          input.type = "hidden";
          input.name = "selected_countries";
          input.value = country;
          selectedCountryChips.appendChild(input);
        });
      }
      syncPickerOptions();
    };

    const addSelectedCountry = () => {
      const value = (countryPicker.value || "").trim();
      if (!value || selected.includes(value)) return;
      selected.push(value);
      selected.sort((a, b) => a.localeCompare(b));
      if (countrySearchInput) countrySearchInput.value = "";
      renderSelectedCountries();
    };

    if (countrySearchInput) {
      countrySearchInput.addEventListener("input", syncPickerOptions);
      countrySearchInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          if (countryPicker.options.length > 1) {
            countryPicker.selectedIndex = 1;
            addSelectedCountry();
          }
        }
      });
    }
    if (addCountryBtn) addCountryBtn.addEventListener("click", addSelectedCountry);
    countryPicker.addEventListener("dblclick", addSelectedCountry);
    renderSelectedCountries();
  }

  function fmtElapsed(s) {
    s = Number(s || 0);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    const r = s % 60;
    return `${m}m ${r}s`;
  }

  function setBarAlive(elBar, isRunning) {
    if (!elBar) return;
    elBar.classList.toggle("animate-pulse", !!isRunning);
  }

  function makeStatusController(opts) {
    const msgEl = document.getElementById(opts.msgId);
    const stepEl = document.getElementById(opts.stepId);
    const elapsedEl = document.getElementById(opts.elapsedId);
    const pctEl = document.getElementById(opts.pctId);
    const barEl = document.getElementById(opts.barId);
    const hintEl = document.getElementById(opts.hintId);
    const finalTextEl = document.getElementById(opts.finalTextId);
    const errBox = document.getElementById(opts.errBoxId);
    const errText = document.getElementById(opts.errTextId);
    const form = document.getElementById(opts.formId);
    const btn = document.getElementById(opts.btnId);
    const btnText = document.getElementById(opts.btnTextId);

    function setPending(isPending) {
      if (btn) btn.disabled = !!isPending;
      if (btnText) btnText.textContent = isPending ? (opts.startingLabel || "Starting…") : (opts.buttonLabel || "Start");
    }

    function showStartingState() {
      if (msgEl) msgEl.textContent = "Starting";
      if (stepEl) stepEl.textContent = "Getting ready";
      if (elapsedEl) elapsedEl.textContent = "0s";
      if (pctEl) pctEl.textContent = "1%";
      if (barEl) barEl.style.width = "1%";
      if (hintEl) hintEl.textContent = opts.startHint || "Getting things ready...";
      if (errBox) errBox.classList.add("hidden");
      if (errText) errText.textContent = "";
      setBarAlive(barEl, true);
    }

    async function poll() {
      if (!msgEl || !barEl) return;
      try {
        const s = await fetch(opts.statusUrl, { cache: "no-store" }).then((r) => r.json());
        const running = !!s.running;
        const msg = s.message || (running ? "Running" : "Idle");
        const step = s.current_step || "—";
        const elapsed = s.elapsed_s || 0;
        let pct = typeof s.progress_pct_smooth === "number" ? s.progress_pct_smooth : 0;
        if (!Number.isFinite(pct)) pct = 0;
        pct = Math.max(0, Math.min(100, pct));
        if (running && pct >= 100) pct = 99;
        if (msgEl) msgEl.textContent = msg;
        if (stepEl) stepEl.textContent = step;
        if (elapsedEl) elapsedEl.textContent = fmtElapsed(elapsed);
        if (pctEl) pctEl.textContent = `${Math.floor(pct)}%`;
        if (barEl) barEl.style.width = `${pct}%`;
        setBarAlive(barEl, running);
        if (hintEl) {
          if (running) hintEl.textContent = opts.runningHint || "Working on it now...";
          else if (String(msg).toLowerCase().startsWith("finished")) hintEl.textContent = opts.finishedHint || "Finished.";
          else hintEl.textContent = "";
        }
        if (finalTextEl) {
          if (!running && typeof msg === "string" && (msg.toLowerCase().startsWith("finished") || msg.toLowerCase().startsWith("failed"))) finalTextEl.textContent = `${fmtElapsed(elapsed)} total`;
          else if (!running && String(msg).toLowerCase() === "idle") finalTextEl.textContent = "No completed run yet.";
        }
        if (!running && typeof msg === "string" && msg.toLowerCase().startsWith("failed")) {
          if (errBox) errBox.classList.remove("hidden");
          if (errText) errText.textContent = (s.error_detail || s.error || "Unknown error").trim();
        } else {
          if (errBox) errBox.classList.add("hidden");
          if (errText) errText.textContent = "";
        }
      } catch (e) {}
    }

    if (form) {
      form.addEventListener("submit", async (e) => {
        e.preventDefault();
        setPending(true);
        showStartingState();
        try {
          const resp = await fetch(form.action, {
            method: "POST",
            body: new FormData(form),
            headers: { "X-Requested-With": "XMLHttpRequest", "Accept": "application/json" }
          });
          const data = await resp.json().catch(() => ({ ok: false, message: `Failed to start ${opts.kind || 'run'}.` }));
          if (!resp.ok || !data.ok) {
            if (msgEl) msgEl.textContent = "Failed";
            if (stepEl) stepEl.textContent = "—";
            if (pctEl) pctEl.textContent = "0%";
            if (barEl) barEl.style.width = "0%";
            setBarAlive(barEl, false);
            if (hintEl) hintEl.textContent = "";
            if (errBox) errBox.classList.remove("hidden");
            if (errText) errText.textContent = (data && data.message) ? data.message : `Failed to start ${opts.kind || 'run'}.`;
          } else {
            poll();
          }
        } catch (err) {
          if (msgEl) msgEl.textContent = "Failed";
          if (stepEl) stepEl.textContent = "—";
          if (pctEl) pctEl.textContent = "0%";
          if (barEl) barEl.style.width = "0%";
          setBarAlive(barEl, false);
          if (hintEl) hintEl.textContent = "";
          if (errBox) errBox.classList.remove("hidden");
          if (errText) errText.textContent = `Could not start ${opts.kind || 'run'}.`;
        } finally {
          setPending(false);
        }
      });
    }

    if (msgEl && barEl) {
      poll();
      setInterval(poll, 1000);
    }
  }

  makeStatusController({
    kind: "free run",
    formId: "pipelineStartForm",
    btnId: "pipelineStartBtn",
    btnTextId: "pipelineStartBtnText",
    buttonLabel: "Start free run",
    startingLabel: "Starting…",
    statusUrl: "/api/status",
    msgId: "statusMessage",
    stepId: "statusStep",
    elapsedId: "statusElapsed",
    pctId: "statusPct",
    barId: "statusBar",
    hintId: "statusHint",
    finalTextId: "statusFinalText",
    errBoxId: "statusErrorBox",
    errTextId: "statusErrorText",
    startHint: "Getting your free run ready...",
    runningHint: "Free pipeline is running now.",
    finishedHint: "Finished. Open Results when you're ready."
  });

  makeStatusController({
    kind: "premium run",
    formId: "premiumRunForm",
    btnId: "premiumRunBtn",
    btnTextId: "premiumRunBtnText",
    buttonLabel: "Start premium run",
    startingLabel: "Starting…",
    statusUrl: "/api/premium-status",
    msgId: "premiumStatusMessage",
    stepId: "premiumStatusStep",
    elapsedId: "premiumStatusElapsed",
    pctId: "premiumStatusPct",
    barId: "premiumStatusBar",
    hintId: "premiumStatusHint",
    finalTextId: "premiumStatusFinalText",
    errBoxId: "premiumStatusErrorBox",
    errTextId: "premiumStatusErrorText",
    startHint: "Getting your premium search ready...",
    runningHint: "Premium is searching live job posts now.",
    finishedHint: "Finished. Open Premium when you're ready."
  });
})();
