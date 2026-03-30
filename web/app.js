const state = {
  csrfToken: "",
  remoteAccessToken: "",
  preview: null,
  previewDirty: false,
  attachments: [],
  sendingJobId: "",
  isBusy: false,
  activeBlade: "connection",
  overlayOpen: false,
  libraries: {
    templates: [],
    campaigns: [],
  },
};

const elements = {};
const MAX_LOG_ENTRIES = 250;
const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;
const ACTION_BUTTON_IDS = [
  "testConnectionBtn",
  "testConnectionBtnSticky",
  "testConnectionBtnOverlay",
  "sendTestEmailBtn",
  "sendTestEmailBtnSticky",
  "sendTestEmailBtnOverlay",
  "previewBtn",
  "previewBtnSticky",
  "previewBtnOverlay",
  "sendBtn",
  "sendBtnSticky",
  "sendBtnOverlay",
];
const BASIC_SMTP_FIELDS = ["smtpHost", "smtpUsername", "fromEmail"];
const CONNECTION_FIELDS = [...BASIC_SMTP_FIELDS, "smtpPort", "smtpPassword", "replyTo", "unsubscribeEmail", "useStarttls", "useSsl", "verifyTls"];
const SENT_COPY_FIELDS = ["saveSentCopy", "imapHost", "imapPort", "imapUsername", "imapPassword", "imapSentFolder", "imapUseSsl", "imapVerifyTls"];
const CAMPAIGN_FIELDS = ["recipientList", "subjectLine", "htmlContent", "textContent", "batchSize", "pauseSeconds", "maxAttempts", "bodyMode"];

class ValidationError extends Error {
  constructor(issues) {
    super("Fix the highlighted fields before continuing.");
    this.name = "ValidationError";
    this.issues = issues;
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  cacheElements();
  bindEvents();
  try {
    captureRemoteAccessToken();
    await loadBootstrap();
  } catch (error) {
    const message = error.message || String(error);
    elements.securityBanner.textContent = message;
    logActivity("App boot failed", message, "error", { persist: false });
  }
});

function cacheElements() {
  const ids = [
    "securityBanner",
    "workflowStatus",
    "overlayWorkflowStatus",
    "activeBladeLabel",
    "connectionBladeMeta",
    "composeBladeMeta",
    "reviewBladeMeta",
    "activityBladeMeta",
    "bladeStage",
    "bladeConnection",
    "bladeActivity",
    "overlayToggleBtn",
    "overlayCloseBtn",
    "overlayBackdrop",
    "commandOverlay",
    "smtpHost",
    "smtpPort",
    "smtpUsername",
    "smtpPassword",
    "fromEmail",
    "fromName",
    "replyTo",
    "unsubscribeEmail",
    "useStarttls",
    "useSsl",
    "verifyTls",
    "savePasswordToKeychain",
    "saveSentCopy",
    "imapHost",
    "imapPort",
    "imapUsername",
    "imapPassword",
    "imapSentFolder",
    "imapUseSsl",
    "imapVerifyTls",
    "templateName",
    "savedTemplateSelect",
    "saveTemplateBtn",
    "loadTemplateBtn",
    "campaignName",
    "savedCampaignSelect",
    "saveCampaignBtn",
    "loadCampaignBtn",
    "batchSize",
    "pauseSeconds",
    "maxAttempts",
    "retryExhausted",
    "bodyMode",
    "subjectLine",
    "recipientList",
    "htmlContent",
    "textContent",
    "htmlFileInput",
    "textFileInput",
    "htmlComposerSection",
    "textComposerSection",
    "attachmentInput",
    "attachmentSummary",
    "attachmentList",
    "testConnectionBtn",
    "testConnectionBtnSticky",
    "testConnectionBtnOverlay",
    "sendTestEmailBtn",
    "sendTestEmailBtnSticky",
    "sendTestEmailBtnOverlay",
    "saveSettingsBtn",
    "clearSavedPasswordBtn",
    "clearLogBtn",
    "exportLogBtn",
    "stepCampaign",
    "stepViewer",
    "previewBtn",
    "previewBtnSticky",
    "previewBtnOverlay",
    "sendBtn",
    "sendBtnSticky",
    "sendBtnOverlay",
    "resetStateBtn",
    "summaryReady",
    "summaryEligible",
    "summarySent",
    "summaryInvalid",
    "summaryDuplicates",
    "summaryMissing",
    "progressHeadline",
    "progressPercent",
    "progressFill",
    "progressCaption",
    "progressBreakdown",
    "campaignKeyText",
    "stateFileText",
    "bodyModeText",
    "attachmentCountText",
    "nextRecipientList",
    "missingList",
    "sampleSubject",
    "sampleText",
    "htmlPreviewFrame",
    "activityLog",
  ];

  for (const id of ids) {
    elements[id] = document.getElementById(id);
  }

  elements.formFields = Array.from(document.querySelectorAll("input, select, textarea"));
  elements.bladeTabs = Array.from(document.querySelectorAll("[data-blade-target]"));
  elements.bladePanels = Array.from(document.querySelectorAll("[data-blade]"));
}

function bindEvents() {
  elements.htmlFileInput.addEventListener("change", event => loadSelectedFile(event.target.files[0], elements.htmlContent, "HTML editor"));
  elements.textFileInput.addEventListener("change", event => loadSelectedFile(event.target.files[0], elements.textContent, "plain-text editor"));
  elements.attachmentInput.addEventListener("change", event => loadAttachmentFiles(event.target.files));
  elements.bodyMode.addEventListener("change", () => {
    syncBodyModeUI();
    refreshActionAvailability();
  });
  elements.useStarttls.addEventListener("change", () => syncTransportMode("starttls"));
  elements.useSsl.addEventListener("change", () => syncTransportMode("ssl"));
  elements.imapUseSsl.addEventListener("change", syncImapMode);

  bindMirroredAction(["testConnectionBtn", "testConnectionBtnSticky", "testConnectionBtnOverlay"], testConnection, "SMTP check failed");
  bindMirroredAction(["sendTestEmailBtn", "sendTestEmailBtnSticky", "sendTestEmailBtnOverlay"], sendTestEmail, "Test email failed");
  bindMirroredAction(["previewBtn", "previewBtnSticky", "previewBtnOverlay"], previewCampaign, "Preview failed");
  bindMirroredAction(["sendBtn", "sendBtnSticky", "sendBtnOverlay"], sendCampaign, "Send failed");
  bindMirroredAction(["saveSettingsBtn"], saveSettings, "Saving settings failed");
  bindMirroredAction(["clearSavedPasswordBtn"], clearSavedPassword, "Keychain update failed");
  bindMirroredAction(["saveTemplateBtn"], saveTemplate, "Saving template failed");
  bindMirroredAction(["loadTemplateBtn"], loadTemplate, "Loading template failed");
  bindMirroredAction(["saveCampaignBtn"], saveCampaign, "Saving campaign failed");
  bindMirroredAction(["loadCampaignBtn"], loadCampaign, "Loading campaign failed");
  elements.clearLogBtn.addEventListener("click", clearActivityLog);
  bindMirroredAction(["exportLogBtn"], exportActivityLogCsv, "Export failed");
  bindMirroredAction(["resetStateBtn"], resetCampaignState, "Reset failed");
  elements.overlayToggleBtn.addEventListener("click", openOverlay);
  elements.overlayCloseBtn.addEventListener("click", closeOverlay);
  elements.overlayBackdrop.addEventListener("click", closeOverlay);

  for (const tab of elements.bladeTabs) {
    tab.addEventListener("click", () => activateBlade(tab.dataset.bladeTarget, { focusTab: true }));
  }

  for (const field of elements.formFields) {
    field.addEventListener("input", () => handleFieldInteraction(field));
    field.addEventListener("change", () => handleFieldInteraction(field));
  }

  document.addEventListener("keydown", handleGlobalKeydown);
}

async function loadBootstrap() {
  const response = await apiFetch("/api/bootstrap", { cache: "no-store" });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Could not start the app session.");
  }
  state.csrfToken = payload.csrfToken;

  fillProfile(payload.profile || {});
  renderLibraries(payload.libraries || {});
  syncBodyModeUI();
  renderAttachmentList();
  resetPreviewDisplay();

  const passwordLine = payload.hasSavedPassword
    ? "A password is already saved in macOS Keychain for this account."
    : "No password is saved.";
  const keychainLine = payload.keychainAvailable
    ? ""
    : "Keychain support is unavailable on this machine.";

  const hostLine = payload.remoteAccessEnabled
    ? "Remote access is enabled for this server session. API calls require the current access token."
    : "Local-only server on 127.0.0.1.";
  elements.securityBanner.textContent = [hostLine, passwordLine, keychainLine].filter(Boolean).join(" ");
  setWorkflowStatus("Run TEST LOGIN to confirm mailbox access, then SEND TEST EMAIL before sending a batch.");
  activateBlade(state.activeBlade);
  refreshBladeTelemetry();
  refreshActionAvailability();
  logActivity(
    "App ready",
    `Settings file: ${payload.settingsPath}\nLog store: ${payload.logStorePath}\nStored log entries: ${payload.logEntryCount || 0}`,
    "success"
  );
}

function captureRemoteAccessToken() {
  const url = new URL(window.location.href);
  const token = url.searchParams.get("access_token");
  if (!token) {
    return;
  }
  state.remoteAccessToken = token;
  url.searchParams.delete("access_token");
  const nextUrl = `${url.pathname}${url.search}${url.hash}`;
  window.history.replaceState({}, document.title, nextUrl || "/");
}

function apiHeaders(extraHeaders = {}) {
  const headers = new Headers(extraHeaders);
  if (state.remoteAccessToken) {
    headers.set("X-Remote-Access-Token", state.remoteAccessToken);
  }
  return headers;
}

function apiFetch(url, options = {}) {
  return fetch(url, {
    ...options,
    headers: apiHeaders(options.headers || {}),
  });
}

function fillProfile(profile) {
  elements.smtpHost.value = profile.host || "";
  elements.smtpPort.value = profile.port || 587;
  elements.smtpUsername.value = profile.username || "";
  elements.fromEmail.value = profile.from_email || "";
  elements.fromName.value = profile.from_name || "";
  elements.replyTo.value = profile.reply_to || "";
  elements.unsubscribeEmail.value = profile.unsubscribe_email || "";
  elements.useStarttls.checked = Boolean(profile.use_starttls);
  elements.useSsl.checked = Boolean(profile.use_ssl);
  elements.verifyTls.checked = profile.verify_tls !== false;
  elements.saveSentCopy.checked = Boolean(profile.save_sent_copy);
  elements.imapHost.value = profile.imap_host || "";
  elements.imapPort.value = profile.imap_port || 993;
  elements.imapUsername.value = profile.imap_username || "";
  elements.imapSentFolder.value = profile.imap_sent_folder || "Sent";
  elements.imapUseSsl.checked = profile.imap_use_ssl !== false;
  elements.imapVerifyTls.checked = profile.imap_verify_tls !== false;
  elements.batchSize.value = profile.batch_size || 100;
  elements.pauseSeconds.value = profile.pause_seconds ?? 1;
  elements.campaignName.value = profile.campaign_name || "";
  elements.maxAttempts.value = profile.max_attempts_per_row || 3;
  elements.retryExhausted.checked = Boolean(profile.retry_exhausted);
  elements.bodyMode.value = profile.body_mode || "html";
}

function bindMirroredAction(ids, action, errorTitle) {
  const buttons = ids.map(id => elements[id]).filter(Boolean);
  for (const button of buttons) {
    button.addEventListener("click", () => runActionGroup(buttons, action, errorTitle));
  }
}

function handleFieldInteraction(field) {
  clearFieldError(field);
  if (CAMPAIGN_FIELDS.includes(field.id)) {
    state.previewDirty = state.preview !== null;
    setWorkflowStatus("Campaign inputs changed. Run PREVIEW BATCH again before sending.", "warning");
  } else if (CONNECTION_FIELDS.includes(field.id) || SENT_COPY_FIELDS.includes(field.id)) {
    setWorkflowStatus("Connection settings changed. Run TEST LOGIN again before sending.", "warning");
  }
  refreshBladeTelemetry();
  refreshActionAvailability();
}

function actionButtons(ids = ACTION_BUTTON_IDS) {
  return ids.map(id => elements[id]).filter(Boolean);
}

function activateBlade(blade, options = {}) {
  if (!blade) {
    return;
  }

  const tabs = elements.bladeTabs || [];
  const panels = elements.bladePanels || [];
  const nextIndex = tabs.findIndex(tab => tab.dataset.bladeTarget === blade);
  if (nextIndex === -1) {
    return;
  }

  const previousIndex = tabs.findIndex(tab => tab.dataset.bladeTarget === state.activeBlade);
  state.activeBlade = blade;

  if (elements.bladeStage) {
    elements.bladeStage.dataset.direction = nextIndex >= previousIndex ? "forward" : "backward";
  }

  for (const tab of tabs) {
    const isActive = tab.dataset.bladeTarget === blade;
    tab.classList.toggle("is-active", isActive);
    tab.setAttribute("aria-selected", String(isActive));
    tab.setAttribute("tabindex", isActive ? "0" : "-1");
  }

  for (const panel of panels) {
    const isActive = panel.dataset.blade === blade;
    panel.hidden = !isActive;
    panel.classList.toggle("is-active", isActive);
    panel.setAttribute("aria-hidden", String(!isActive));
  }

  if (options.focusTab) {
    tabs[nextIndex]?.focus();
  }

  refreshBladeTelemetry();
}

function cycleBlade(delta) {
  const tabs = elements.bladeTabs || [];
  if (!tabs.length) {
    return;
  }
  const currentIndex = Math.max(0, tabs.findIndex(tab => tab.dataset.bladeTarget === state.activeBlade));
  const nextIndex = (currentIndex + delta + tabs.length) % tabs.length;
  activateBlade(tabs[nextIndex].dataset.bladeTarget, { focusTab: true });
}

function openOverlay() {
  if (!elements.commandOverlay) {
    return;
  }
  state.overlayOpen = true;
  elements.commandOverlay.hidden = false;
  document.body.classList.add("overlay-open");
  elements.overlayCloseBtn?.focus();
}

function closeOverlay() {
  if (!elements.commandOverlay || elements.commandOverlay.hidden) {
    return;
  }
  state.overlayOpen = false;
  elements.commandOverlay.hidden = true;
  document.body.classList.remove("overlay-open");
  elements.overlayToggleBtn?.focus();
}

function handleGlobalKeydown(event) {
  if (state.overlayOpen && event.key === "Escape") {
    event.preventDefault();
    closeOverlay();
    return;
  }

  const wantsOverlay = event.key === "?" || (event.key === "/" && event.shiftKey);
  if (wantsOverlay && !isEditableElement(document.activeElement)) {
    event.preventDefault();
    if (state.overlayOpen) {
      closeOverlay();
    } else {
      openOverlay();
    }
    return;
  }

  if (state.overlayOpen || isEditableElement(document.activeElement)) {
    return;
  }

  if (event.key === "ArrowLeft") {
    event.preventDefault();
    cycleBlade(-1);
  } else if (event.key === "ArrowRight") {
    event.preventDefault();
    cycleBlade(1);
  }
}

function isEditableElement(node) {
  if (!node) {
    return false;
  }
  const tag = node.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || node.isContentEditable;
}

function renderLibraries(libraries) {
  state.libraries.templates = Array.isArray(libraries.templates) ? libraries.templates : [];
  state.libraries.campaigns = Array.isArray(libraries.campaigns) ? libraries.campaigns : [];
  fillLibrarySelect(elements.savedTemplateSelect, state.libraries.templates, "No saved templates yet");
  fillLibrarySelect(elements.savedCampaignSelect, state.libraries.campaigns, "No saved campaigns yet");
  refreshBladeTelemetry();
  refreshActionAvailability();
}

function fillLibrarySelect(select, items, emptyLabel, selectedValue = "") {
  select.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = emptyLabel;
  select.appendChild(placeholder);

  for (const item of items) {
    const option = document.createElement("option");
    option.value = item.id;
    option.textContent = libraryOptionLabel(item);
    if (selectedValue && selectedValue === item.id) {
      option.selected = true;
    }
    select.appendChild(option);
  }
}

function libraryOptionLabel(item) {
  const parts = [item.name];
  if (item.recipientCount) {
    parts.push(`${item.recipientCount} recipient(s)`);
  }
  if (item.attachmentCount) {
    parts.push(`${item.attachmentCount} attachment(s)`);
  }
  return parts.join(" | ");
}

function syncTransportMode(mode) {
  if (mode === "starttls" && elements.useStarttls.checked) {
    elements.useSsl.checked = false;
    if (!elements.smtpPort.value || Number(elements.smtpPort.value) === 465) {
      elements.smtpPort.value = 587;
    }
  }

  if (mode === "ssl" && elements.useSsl.checked) {
    elements.useStarttls.checked = false;
    if (!elements.smtpPort.value || Number(elements.smtpPort.value) === 587) {
      elements.smtpPort.value = 465;
    }
  }

  refreshActionAvailability();
}

function syncImapMode() {
  if (elements.imapUseSsl.checked) {
    if (!elements.imapPort.value || Number(elements.imapPort.value) === 143) {
      elements.imapPort.value = 993;
    }
    refreshActionAvailability();
    return;
  }

  if (!elements.imapPort.value || Number(elements.imapPort.value) === 993) {
    elements.imapPort.value = 143;
  }
  refreshActionAvailability();
}

function syncBodyModeUI() {
  const mode = bodyMode();
  const showHtml = mode === "html" || mode === "both";
  const showText = mode === "text" || mode === "both";
  elements.htmlComposerSection.hidden = !showHtml;
  elements.textComposerSection.hidden = !showText;
  if (!state.preview) {
    elements.bodyModeText.textContent = `Format: ${formatLabel(mode)}`;
  }
}

async function loadSelectedFile(file, target, label) {
  if (!file) return;
  const text = await file.text();
  target.value = text;
  state.previewDirty = state.preview !== null;
  clearFieldError(target);
  refreshBladeTelemetry();
  refreshActionAvailability();
  logActivity("Loaded file", `Inserted ${file.name} into the ${label}.`, "info");
}

async function loadAttachmentFiles(files) {
  if (!files || !files.length) {
    return;
  }

  try {
    const loaded = [];
    for (const file of files) {
      loaded.push(await fileToAttachment(file));
    }
    state.attachments.push(...loaded);
    state.previewDirty = state.preview !== null;
    elements.attachmentInput.value = "";
    enforceAttachmentLimit();
    renderAttachmentList();
    clearFieldError(elements.attachmentInput);
    setWorkflowStatus("Attachments updated. Preview the batch again if the message changed.", "info");
    refreshBladeTelemetry();
    logActivity("Attachments loaded", `Added ${loaded.length} attachment(s). Total loaded: ${state.attachments.length}.`, "info");
  } catch (error) {
    presentValidationIssues([{ field: elements.attachmentInput, message: error.message || String(error) }]);
    setWorkflowStatus(error.message || String(error), "warning");
  }
}

async function fileToAttachment(file) {
  const dataUrl = await readFileAsDataUrl(file);
  const parts = String(dataUrl).split(",", 2);
  return {
    filename: file.name,
    content_type: file.type || "application/octet-stream",
    data_base64: parts[1] || "",
    size: file.size || 0,
  };
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(new Error(`Could not read ${file.name}.`));
    reader.readAsDataURL(file);
  });
}

function renderAttachmentList() {
  elements.attachmentList.innerHTML = "";

  if (!state.attachments.length) {
    elements.attachmentSummary.textContent = "No attachments loaded.";
    if (!state.preview) {
      elements.attachmentCountText.textContent = "Attachments: 0";
    }
    refreshActionAvailability();
    return;
  }

  const totalBytes = attachmentBytes();
  elements.attachmentSummary.textContent = `${state.attachments.length} attachment(s) loaded, ${humanFileSize(totalBytes)} total.`;
  if (!state.preview) {
    elements.attachmentCountText.textContent = `Attachments: ${state.attachments.length}`;
  }

  state.attachments.forEach((attachment, index) => {
    const item = document.createElement("article");
    item.className = "attachment-item";

    const meta = document.createElement("div");
    meta.className = "attachment-meta";

    const name = document.createElement("strong");
    name.textContent = attachment.filename;

    const detail = document.createElement("span");
    detail.textContent = `${attachment.content_type || "application/octet-stream"} | ${humanFileSize(attachment.size || 0)}`;

    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "ghost small-button";
    remove.textContent = "Remove";
    remove.addEventListener("click", () => removeAttachment(index));

    meta.append(name, detail);
    item.append(meta, remove);
    elements.attachmentList.appendChild(item);
  });

  refreshBladeTelemetry();
  refreshActionAvailability();
}

function removeAttachment(index) {
  const [removed] = state.attachments.splice(index, 1);
  state.previewDirty = state.preview !== null;
  renderAttachmentList();
  if (removed) {
    setWorkflowStatus("Attachment removed. Preview again if this changes the outgoing message.", "warning");
    refreshBladeTelemetry();
    logActivity("Attachment removed", `${removed.filename} removed from the current message.`, "warning");
  }
}

function attachmentBytes() {
  return state.attachments.reduce((sum, item) => sum + Number(item.size || 0), 0);
}

function enforceAttachmentLimit() {
  const total = attachmentBytes();
  if (total > MAX_ATTACHMENT_BYTES) {
    state.attachments.pop();
    renderAttachmentList();
    throw new Error("Keep total attachments under 20 MB.");
  }
}

function smtpPayload() {
  return {
    host: elements.smtpHost.value.trim(),
    port: Number(elements.smtpPort.value || 587),
    username: elements.smtpUsername.value.trim(),
    password: elements.smtpPassword.value,
    from_email: elements.fromEmail.value.trim(),
    from_name: elements.fromName.value.trim(),
    reply_to: elements.replyTo.value.trim(),
    use_starttls: elements.useStarttls.checked,
    use_ssl: elements.useSsl.checked,
    verify_tls: elements.verifyTls.checked,
    unsubscribe_email: elements.unsubscribeEmail.value.trim(),
    unsubscribe_url: "",
  };
}

function sentCopyPayload() {
  return {
    enabled: elements.saveSentCopy.checked,
    host: elements.imapHost.value.trim(),
    port: Number(elements.imapPort.value || 993),
    username: elements.imapUsername.value.trim(),
    password: elements.imapPassword.value,
    sent_folder: elements.imapSentFolder.value.trim(),
    use_ssl: elements.imapUseSsl.checked,
    verify_tls: elements.imapVerifyTls.checked,
  };
}

function currentMessageFields() {
  return {
    subject: elements.subjectLine.value,
    body_mode: bodyMode(),
    html_content: elements.htmlContent.value,
    text_content: elements.textContent.value,
    attachments: cloneAttachments(state.attachments),
  };
}

function templatePayload() {
  return {
    template_name: elements.templateName.value.trim(),
    ...currentMessageFields(),
  };
}

function campaignPayload() {
  return {
    campaign_name: elements.campaignName.value.trim(),
    batch_size: Number(elements.batchSize.value || 100),
    pause_seconds: Number(elements.pauseSeconds.value || 1),
    max_attempts_per_row: Number(elements.maxAttempts.value || 3),
    retry_exhausted: elements.retryExhausted.checked,
    recipient_list: elements.recipientList.value,
    ...currentMessageFields(),
  };
}

function cloneAttachments(items) {
  return items.map(item => ({
    filename: item.filename,
    content_type: item.content_type,
    data_base64: item.data_base64,
    size: Number(item.size || 0),
  }));
}

function bodyMode() {
  return elements.bodyMode.value || "html";
}

async function runActionGroup(buttons, action, errorTitle = "Error") {
  const labels = new Map(buttons.map(button => [button, button.textContent]));
  clearValidationErrors();
  for (const button of buttons) {
    button.disabled = true;
  }

  try {
    await action();
  } catch (error) {
    if (error instanceof ValidationError) {
      presentValidationIssues(error.issues);
      setWorkflowStatus(error.message, "warning");
      return;
    }
    setWorkflowStatus(error.message || String(error), "error");
    logActivity(errorTitle, error.message || String(error), "error");
  } finally {
    for (const button of buttons) {
      button.disabled = false;
      button.textContent = labels.get(button);
    }
    refreshActionAvailability();
  }
}

async function postJson(url, payload) {
  const response = await apiFetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-CSRF-Token": state.csrfToken,
    },
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Request failed.");
  }
  return data;
}

async function persistActivityLog(entry) {
  if (!state.csrfToken) {
    return;
  }
  try {
    await postJson("/api/log-event", entry);
  } catch (error) {
    console.error("Activity log persistence failed.", error);
  }
}

async function testConnection() {
  validateConnectionForm();
  const result = await postJson("/api/test-connection", { smtp: smtpPayload(), sent_copy: sentCopyPayload() });
  closeOverlay();
  setWorkflowStatus("Mailbox verified. Send the fixed test email before previewing the live batch.", "success");
  refreshBladeTelemetry();
  logActivity("SMTP check", result.message, "success");
}

async function sendTestEmail() {
  validateTestEmailForm();
  const result = await postJson("/api/send-test-email", {
    smtp: smtpPayload(),
    sent_copy: sentCopyPayload(),
    campaign: campaignPayload(),
  });
  closeOverlay();
  setWorkflowStatus("Test email sent. Review the inbox result, then run PREVIEW BATCH for the next recipients.", "success");
  refreshBladeTelemetry();
  logActivity("Test email sent", result.message, "success");
}

async function saveSettings() {
  const result = await postJson("/api/save-settings", {
    smtp: smtpPayload(),
    sent_copy: sentCopyPayload(),
    savePasswordToKeychain: elements.savePasswordToKeychain.checked,
  });
  logActivity("Settings saved", result.message, "success");
  const passwordLine = result.hasSavedPassword
    ? "A password is saved in macOS Keychain."
    : "No password is stored in macOS Keychain.";
  const hostLine = state.remoteAccessToken
    ? "Remote access is enabled for this server session."
    : "Local-only server on 127.0.0.1.";
  elements.securityBanner.textContent = `${hostLine} ${passwordLine}`;
  setWorkflowStatus("Local defaults saved. Re-run TEST LOGIN if you changed connection details.", "success");
  refreshBladeTelemetry();
}

async function clearSavedPassword() {
  const result = await postJson("/api/clear-saved-password", { smtp: smtpPayload() });
  logActivity("Keychain", result.message, "warning");
  const hostLine = state.remoteAccessToken
    ? "Remote access is enabled for this server session."
    : "Local-only server on 127.0.0.1.";
  elements.securityBanner.textContent = `${hostLine} No password is stored in macOS Keychain.`;
  setWorkflowStatus("Saved password removed. The current session password remains in memory until the page is reloaded.", "warning");
  refreshBladeTelemetry();
}

async function saveTemplate() {
  validateTemplateForm();
  const result = await postJson("/api/save-template", { template: templatePayload() });
  renderLibraries(result.libraries || {});
  elements.savedTemplateSelect.value = result.template.id;
  closeOverlay();
  setWorkflowStatus("Template saved. You can keep composing or load it later into a fresh campaign.", "success");
  logActivity(
    "Template saved",
    `${result.message}\nFormat: ${formatLabel(result.template.body_mode)}\nAttachments: ${(result.template.attachments || []).length}`,
    "success"
  );
}

async function loadTemplate() {
  const templateId = elements.savedTemplateSelect.value;
  if (!templateId) {
    throw new Error("Choose a saved template first.");
  }
  const result = await postJson("/api/load-template", { template_id: templateId });
  applyTemplate(result.template);
  renderLibraries(result.libraries || {});
  elements.savedTemplateSelect.value = result.template.id;
  openStep(elements.stepCampaign);
  resetPreviewDisplay();
  closeOverlay();
  setWorkflowStatus("Template loaded. Paste recipients, preview the next batch, and then send.", "info");
  logActivity(
    "Template loaded",
    `${result.template.name}\nFormat: ${formatLabel(result.template.body_mode)}\nAttachments: ${(result.template.attachments || []).length}`,
    "success"
  );
}

async function saveCampaign() {
  validateCampaignForm();
  const result = await postJson("/api/save-campaign", { campaign: campaignPayload() });
  renderLibraries(result.libraries || {});
  elements.savedCampaignSelect.value = result.campaign.id;
  closeOverlay();
  setWorkflowStatus("Campaign saved. Preview again before sending if you continue editing.", "success");
  logActivity(
    "Campaign saved",
    `${result.message}\nRecipients: ${result.campaign.recipient_count}\nAttachments: ${(result.campaign.attachments || []).length}`,
    "success"
  );
}

async function loadCampaign() {
  const campaignId = elements.savedCampaignSelect.value;
  if (!campaignId) {
    throw new Error("Choose a saved campaign first.");
  }
  const result = await postJson("/api/load-campaign", { campaign_id: campaignId });
  applyCampaign(result.campaign);
  renderLibraries(result.libraries || {});
  elements.savedCampaignSelect.value = result.campaign.id;
  openStep(elements.stepCampaign);
  resetPreviewDisplay();
  closeOverlay();
  setWorkflowStatus("Campaign loaded. Review the content and run PREVIEW BATCH before sending.", "info");
  logActivity(
    "Campaign loaded",
    `${result.campaign.name}\nRecipients: ${result.campaign.recipient_count}\nResume by previewing or sending the next batch.`,
    "success"
  );
}

async function previewCampaign() {
  validateCampaignForm();
  const result = await postJson("/api/preview", {
    smtp: smtpPayload(),
    sent_copy: sentCopyPayload(),
    campaign: campaignPayload(),
  });
  state.preview = result.preview;
  state.previewDirty = false;
  renderPreview(result.preview);
  const summary = result.preview.summary;
  closeOverlay();
  setWorkflowStatus(`Preview ready. ${summary.readyNow} recipient(s) are ready in the next batch.`, "success");
  refreshBladeTelemetry();
  logActivity(
    "Preview ready",
    `Campaign ${result.preview.campaignKey}\nFormat: ${formatLabel(result.preview.bodyMode)}\nReady now: ${summary.readyNow}\nEligible remaining: ${summary.eligibleRemaining}\nAttachments: ${(result.preview.attachments || []).length}`,
    "success"
  );
}

async function sendCampaign() {
  validateSendForm();
  if (!confirm("Send the next batch now?")) {
    return;
  }
  closeOverlay();
  setSendingUi(true);
  setWorkflowStatus("Sending the current batch now. Watch Step 3 for live progress.", "info");
  try {
    const result = await postJson("/api/send", {
      smtp: smtpPayload(),
      sent_copy: sentCopyPayload(),
      campaign: campaignPayload(),
    });

    if (!result.job) {
      state.preview = result.preview;
      state.previewDirty = false;
      renderPreview(result.preview);
      setWorkflowStatus("Batch send finished.", "success");
      logActivity("Send finished", result.message, "success");
      return;
    }

    state.sendingJobId = result.job.id;
    if (result.preview) {
      state.preview = result.preview;
      state.previewDirty = false;
      renderPreview(result.preview);
    }
    renderLiveSendProgress(result.job);
    logActivity("Send started", `${result.message}\nBatch size: ${result.job.batchTotal}`, "info", { persist: false });

    const job = await pollSendJob(result.job.id);
    state.sendingJobId = "";

    if (job.status === "failed") {
      setWorkflowStatus(job.error || job.message || "Send failed.", "error");
      logActivity("Send failed", job.error || job.message || "Send failed.", "error", { persist: false });
      return;
    }

    state.preview = job.preview;
    state.previewDirty = false;
    renderPreview(job.preview);
    setWorkflowStatus(
      job.failed.length
        ? `Batch finished with ${job.failed.length} failed recipient(s). Review warnings before retrying.`
        : "Batch finished successfully. Preview again to inspect the next recipients.",
      job.failed.length ? "warning" : "success"
    );
    logActivity(
      "Send finished",
      `${job.message}\nDelivered: ${job.sent.length}\nFailed: ${job.failed.length}\nAttachments: ${(job.preview?.attachments || []).length}`,
      job.failed.length ? "warning" : "success",
      { persist: false }
    );

    for (const item of job.sent) {
      logActivity("Sent", `${item.email}\nRow ${item.row}`, "success", { persist: false });
      if (item.warning) {
        logActivity("Sent copy warning", `${item.email}\n${item.warning}`, "warning", { persist: false });
      }
    }
    for (const item of job.failed) {
      logActivity("Failed", `${item.email}\nRow ${item.row}\n${item.error}`, "error", { persist: false });
    }
  } finally {
    state.sendingJobId = "";
    setSendingUi(false);
  }
}

async function resetCampaignState() {
  if (!confirm("Reset saved progress for this campaign?")) {
    return;
  }
  const result = await postJson("/api/reset-state", {
    campaign: campaignPayload(),
  });
  logActivity("State reset", `${result.message}\nCampaign key: ${result.campaignKey}`, "warning");
  resetPreviewDisplay();
  elements.campaignKeyText.textContent = `Campaign key: ${result.campaignKey}`;
  elements.stateFileText.textContent = "State file: reset";
  setWorkflowStatus("Campaign progress reset. Run PREVIEW BATCH again before the next send.", "warning");
  refreshBladeTelemetry();
}

function applyTemplate(record) {
  elements.templateName.value = record.name || "";
  elements.subjectLine.value = record.subject || "";
  elements.bodyMode.value = record.body_mode || "html";
  elements.htmlContent.value = record.html_content || "";
  elements.textContent.value = record.text_content || "";
  state.attachments = cloneAttachments(record.attachments || []);
  syncBodyModeUI();
  renderAttachmentList();
  state.previewDirty = state.preview !== null;
  refreshBladeTelemetry();
  refreshActionAvailability();
}

function applyCampaign(record) {
  elements.campaignName.value = record.campaign_name || record.name || "";
  elements.batchSize.value = record.batch_size || 100;
  elements.pauseSeconds.value = record.pause_seconds ?? 1;
  elements.maxAttempts.value = record.max_attempts_per_row || 3;
  elements.retryExhausted.checked = Boolean(record.retry_exhausted);
  elements.subjectLine.value = record.subject || "";
  elements.recipientList.value = record.recipient_list || "";
  elements.bodyMode.value = record.body_mode || "html";
  elements.htmlContent.value = record.html_content || "";
  elements.textContent.value = record.text_content || "";
  state.attachments = cloneAttachments(record.attachments || []);
  syncBodyModeUI();
  renderAttachmentList();
  state.previewDirty = state.preview !== null;
  refreshBladeTelemetry();
  refreshActionAvailability();
}

function renderPreview(preview) {
  openStep(elements.stepViewer);
  state.previewDirty = false;
  const summary = preview.summary;
  elements.summaryReady.textContent = summary.readyNow;
  elements.summaryEligible.textContent = summary.eligibleRemaining;
  elements.summarySent.textContent = summary.alreadySent;
  elements.summaryInvalid.textContent = summary.invalidEmails;
  elements.summaryDuplicates.textContent = summary.duplicateEmails;
  elements.summaryMissing.textContent = summary.missingFields;
  renderProgress(summary);

  elements.campaignKeyText.textContent = `Campaign key: ${preview.campaignKey}`;
  elements.stateFileText.textContent = `State file: ${preview.stateFile}`;
  elements.bodyModeText.textContent = `Format: ${formatLabel(preview.bodyMode)}`;
  elements.attachmentCountText.textContent = `Attachments: ${(preview.attachments || []).length}`;

  renderList(elements.nextRecipientList, preview.nextRecipients, item => `row ${item.row}: ${item.email}`);
  const warningItems = [
    ...(preview.missingFieldRows || []).map(item => `row ${item.row}: ${item.email || "<empty>"} missing [${item.missing}]`),
    ...(preview.warnings || []).map(item => `row ${item.row}: ${item.message}`),
  ];
  renderList(elements.missingList, warningItems, item => item);

  elements.sampleSubject.textContent = preview.sample ? preview.sample.subject : "No pending contacts remain for this campaign.";
  elements.sampleText.textContent = preview.sample ? (preview.sample.text || "No plain-text part for this message.") : "No pending contacts remain for this campaign.";
  elements.htmlPreviewFrame.srcdoc = preview.sample
    ? preview.sample.html
    : "<html><body style='font-family:sans-serif;padding:24px;'>No pending contacts remain for this campaign.</body></html>";
  refreshBladeTelemetry();
  refreshActionAvailability();
}

function resetPreviewDisplay() {
  state.preview = null;
  state.previewDirty = false;
  renderProgress(null);
  elements.summaryReady.textContent = "0";
  elements.summaryEligible.textContent = "0";
  elements.summarySent.textContent = "0";
  elements.summaryInvalid.textContent = "0";
  elements.summaryDuplicates.textContent = "0";
  elements.summaryMissing.textContent = "0";
  elements.campaignKeyText.textContent = "Campaign key: not generated yet";
  elements.stateFileText.textContent = "State file: not generated yet";
  elements.bodyModeText.textContent = `Format: ${formatLabel(bodyMode())}`;
  elements.attachmentCountText.textContent = `Attachments: ${state.attachments.length}`;
  elements.sampleSubject.textContent = "Preview a batch to see the rendered subject.";
  elements.sampleText.textContent = "Preview a batch to see the rendered plain-text email.";
  elements.htmlPreviewFrame.srcdoc = "<html><body style='font-family:sans-serif;padding:24px;'>Preview a batch to render the message.</body></html>";
  renderList(elements.nextRecipientList, [], item => item);
  renderList(elements.missingList, [], item => item);
  refreshBladeTelemetry();
  refreshActionAvailability();
}

function renderProgress(summary) {
  if (!summary) {
    elements.progressHeadline.textContent = "0 of 0 sendable emails sent";
    elements.progressPercent.textContent = "0%";
    elements.progressFill.style.width = "0%";
    elements.progressCaption.textContent = "Preview a batch to initialize the counter.";
    elements.progressBreakdown.textContent = "Ready now 0";
    return;
  }

  const sendableTotal = summary.alreadySent + summary.eligibleRemaining;
  const percent = sendableTotal ? Math.round((summary.alreadySent / sendableTotal) * 100) : 0;

  elements.progressHeadline.textContent = `${summary.alreadySent} of ${sendableTotal} sendable emails sent`;
  elements.progressPercent.textContent = `${percent}%`;
  elements.progressFill.style.width = `${percent}%`;
  elements.progressCaption.textContent = `${summary.eligibleRemaining} sendable email(s) still remaining.`;
  elements.progressBreakdown.textContent =
    `Ready now ${summary.readyNow} | Invalid ${summary.invalidEmails} | Duplicates ${summary.duplicateEmails} | Missing ${summary.missingFields}`;
}

function renderLiveSendProgress(job) {
  openStep(elements.stepViewer);
  const total = Number(job.batchTotal || 0);
  const processed = Number(job.processed || 0);
  const sent = Number(job.sentCount || 0);
  const failed = Number(job.failedCount || 0);
  const percent = total ? Math.round((processed / total) * 100) : 0;

  elements.progressHeadline.textContent = total
    ? `Sending ${processed} of ${total} emails`
    : "Preparing batch send";
  elements.progressPercent.textContent = `${percent}%`;
  elements.progressFill.style.width = `${percent}%`;
  elements.progressCaption.textContent = job.currentRecipient
    ? `Current recipient: ${job.currentRecipient}`
    : job.message || "Preparing delivery pipeline.";
  elements.progressBreakdown.textContent = `Sent ${sent} | Failed ${failed}`;
  refreshBladeTelemetry();
  refreshActionAvailability();
}

function renderList(target, items, formatter) {
  target.innerHTML = "";
  if (!items.length) {
    const li = document.createElement("li");
    li.textContent = "Nothing to show.";
    target.appendChild(li);
    return;
  }
  for (const item of items) {
    const li = document.createElement("li");
    li.textContent = formatter(item);
    target.appendChild(li);
  }
}

function validateConnectionForm() {
  const issues = [];
  collectSmtpIssues(issues);
  collectSentCopyIssues(issues);
  throwIfValidationIssues(issues);
}

function validateTestEmailForm() {
  const issues = [];
  collectSmtpIssues(issues);
  collectSentCopyIssues(issues);
  collectMessageIssues(issues, { requireTemplateName: false });
  throwIfValidationIssues(issues);
}

function validateTemplateForm() {
  const issues = [];
  collectMessageIssues(issues, { requireTemplateName: true });
  throwIfValidationIssues(issues);
}

function validateCampaignForm() {
  const issues = [];
  collectCampaignIssues(issues);
  throwIfValidationIssues(issues);
}

function validateSendForm() {
  const issues = [];
  collectCampaignIssues(issues);
  collectSmtpIssues(issues);
  collectSentCopyIssues(issues);
  throwIfValidationIssues(issues);
}

function collectSmtpIssues(issues) {
  requireValue(issues, elements.smtpHost, "Enter the SMTP host.");
  requireValue(issues, elements.smtpUsername, "Enter the SMTP username.");
  requireValue(issues, elements.fromEmail, "Enter the From email.");
}

function collectMessageIssues(issues, options = {}) {
  if (options.requireTemplateName) {
    requireValue(issues, elements.templateName, "Enter a template name.");
  }
  requireValue(issues, elements.subjectLine, "Enter an email subject.");

  const mode = bodyMode();
  if ((mode === "html" || mode === "both") && !elements.htmlContent.value.trim()) {
    issues.push({ field: elements.htmlContent, message: "Paste or load the HTML email first." });
  }
  if ((mode === "text" || mode === "both") && !elements.textContent.value.trim()) {
    issues.push({ field: elements.textContent, message: "Paste the plain-text email first." });
  }
  if (attachmentBytes() > MAX_ATTACHMENT_BYTES) {
    issues.push({ field: elements.attachmentInput, message: "Keep total attachments under 20 MB." });
  }
}

function collectCampaignIssues(issues) {
  collectMessageIssues(issues, { requireTemplateName: false });
  requireValue(issues, elements.recipientList, "Paste at least one recipient email.");
}

function collectSentCopyIssues(issues) {
  if (!elements.saveSentCopy.checked) {
    return;
  }
  requireValue(issues, elements.imapHost, "Enter the IMAP host or turn off server-side sent copies.");
  requireValue(issues, elements.imapSentFolder, "Enter the IMAP sent folder name.");
}

function requireValue(issues, field, message) {
  if (!field || field.value.trim()) {
    return;
  }
  issues.push({ field, message });
}

function throwIfValidationIssues(issues) {
  if (issues.length) {
    throw new ValidationError(issues);
  }
}

function presentValidationIssues(issues) {
  if (!issues.length) {
    return;
  }

  const seen = new Set();
  for (const issue of issues) {
    const field = issue.field;
    if (!field) {
      continue;
    }
    if (seen.has(field)) {
      continue;
    }
    seen.add(field);
    setFieldError(field, issue.message);
  }

  const firstField = issues.find(issue => issue.field)?.field;
  if (firstField && typeof firstField.focus === "function") {
    firstField.focus();
  }
}

function clearValidationErrors() {
  const nodes = document.querySelectorAll(".field-error");
  for (const node of nodes) {
    node.textContent = "";
    node.hidden = true;
  }

  for (const field of elements.formFields || []) {
    clearFieldError(field);
  }
}

function clearFieldError(field) {
  if (!field) {
    return;
  }
  const container = errorContainerForField(field);
  const node = container?.querySelector(".field-error");
  if (container) {
    container.classList.remove("has-error");
  }
  field.removeAttribute("aria-invalid");
  if (node) {
    node.hidden = true;
    node.textContent = "";
  }
}

function setFieldError(field, message) {
  const container = errorContainerForField(field);
  if (!container) {
    return;
  }
  let node = container.querySelector(".field-error");
  if (!node) {
    node = document.createElement("p");
    node.className = "field-error";
    container.appendChild(node);
  }
  container.classList.add("has-error");
  field.setAttribute("aria-invalid", "true");
  node.hidden = false;
  node.textContent = message;
}

function errorContainerForField(field) {
  return field.closest("label") || field.closest(".module-panel") || field.parentElement;
}

function setWorkflowStatus(message, tone = "info") {
  if (!elements.workflowStatus) {
    return;
  }
  elements.workflowStatus.textContent = message;
  elements.workflowStatus.dataset.tone = tone;
  if (elements.overlayWorkflowStatus) {
    elements.overlayWorkflowStatus.textContent = message;
    elements.overlayWorkflowStatus.dataset.tone = tone;
  }
}

function refreshBladeTelemetry() {
  if (elements.activeBladeLabel) {
    elements.activeBladeLabel.textContent = bladeLabel(state.activeBlade);
  }
  if (elements.connectionBladeMeta) {
    elements.connectionBladeMeta.textContent = connectionTelemetry();
  }
  if (elements.composeBladeMeta) {
    elements.composeBladeMeta.textContent = composeTelemetry();
  }
  if (elements.reviewBladeMeta) {
    elements.reviewBladeMeta.textContent = reviewTelemetry();
  }
  if (elements.activityBladeMeta) {
    const count = elements.activityLog ? elements.activityLog.children.length : 0;
    elements.activityBladeMeta.textContent = `${count} local entr${count === 1 ? "y" : "ies"}`;
  }
}

function refreshActionAvailability() {
  const busy = Boolean(state.isBusy || state.sendingJobId);

  setButtonGroupDisabled(["testConnectionBtn", "testConnectionBtnSticky", "testConnectionBtnOverlay"], busy || !canTestConnection());
  setButtonGroupDisabled(["sendTestEmailBtn", "sendTestEmailBtnSticky", "sendTestEmailBtnOverlay"], busy || !canSendTestEmail());
  setButtonGroupDisabled(["previewBtn", "previewBtnSticky", "previewBtnOverlay"], busy || !canPreviewBatch());
  setButtonGroupDisabled(["sendBtn", "sendBtnSticky", "sendBtnOverlay"], busy || !canSendBatch());
  setButtonGroupDisabled(["saveTemplateBtn"], busy);
  setButtonGroupDisabled(["loadTemplateBtn"], busy);
  setButtonGroupDisabled(["saveCampaignBtn"], busy);
  setButtonGroupDisabled(["loadCampaignBtn"], busy);
  setButtonGroupDisabled(["resetStateBtn"], busy);
  setButtonGroupDisabled(["saveSettingsBtn"], busy);
  setButtonGroupDisabled(["clearSavedPasswordBtn"], busy);
  refreshBladeTelemetry();
}

function setButtonGroupDisabled(ids, disabled) {
  for (const button of actionButtons(ids)) {
    button.disabled = disabled;
  }
}

function canTestConnection() {
  return hasBasicSmtpInputs() && hasValidSentCopyInputs();
}

function canSendTestEmail() {
  return hasBasicSmtpInputs() && hasValidSentCopyInputs() && hasMessageInputs();
}

function canPreviewBatch() {
  return hasCampaignInputs();
}

function canSendBatch() {
  return hasBasicSmtpInputs() && hasValidSentCopyInputs() && hasCampaignInputs();
}

function hasBasicSmtpInputs() {
  return BASIC_SMTP_FIELDS.every(id => Boolean(elements[id]?.value.trim()));
}

function hasMessageInputs() {
  if (!elements.subjectLine.value.trim()) {
    return false;
  }
  const mode = bodyMode();
  if ((mode === "html" || mode === "both") && !elements.htmlContent.value.trim()) {
    return false;
  }
  if ((mode === "text" || mode === "both") && !elements.textContent.value.trim()) {
    return false;
  }
  return attachmentBytes() <= MAX_ATTACHMENT_BYTES;
}

function hasCampaignInputs() {
  return hasMessageInputs() && Boolean(elements.recipientList.value.trim());
}

function hasValidSentCopyInputs() {
  if (!elements.saveSentCopy.checked) {
    return true;
  }
  return Boolean(elements.imapHost.value.trim() && elements.imapSentFolder.value.trim());
}

function bladeLabel(blade) {
  if (blade === "compose") return "COMPOSE // 構築";
  if (blade === "review") return "REVIEW // 検証";
  if (blade === "activity") return "TRACE // 記録";
  return "LINK // 接続";
}

function connectionTelemetry() {
  const host = elements.smtpHost.value.trim();
  const user = elements.smtpUsername.value.trim();
  if (!host || !user) {
    return "Awaiting transport";
  }
  const mode = elements.useSsl.checked ? "SSL" : elements.useStarttls.checked ? "STARTTLS" : "PLAIN";
  return `${mode} ${host}:${elements.smtpPort.value || 587}`;
}

function composeTelemetry() {
  const recipientCount = estimatedRecipientCount();
  if (!recipientCount) {
    return "0 rows loaded";
  }
  const attachmentCount = state.attachments.length;
  return `${recipientCount} row${recipientCount === 1 ? "" : "s"} / ${attachmentCount} attach`;
}

function reviewTelemetry() {
  if (state.sendingJobId) {
    return "Live send active";
  }
  if (!state.preview) {
    return "No preview";
  }
  if (state.previewDirty) {
    return "Preview stale";
  }
  const summary = state.preview.summary || {};
  return `${summary.readyNow || 0} ready / ${summary.eligibleRemaining || 0} left`;
}

function estimatedRecipientCount() {
  const raw = elements.recipientList.value.trim();
  if (!raw) {
    return 0;
  }
  return raw
    .split(/[\n,]+/)
    .map(item => item.trim())
    .filter(Boolean)
    .length;
}

function clearActivityLog() {
  elements.activityLog.innerHTML = "";
  setWorkflowStatus("Visible activity history cleared. Persisted logs are still available through Export CSV.", "info");
  refreshBladeTelemetry();
  logActivity(
    "Activity panel cleared",
    "Visible browser entries were cleared. Persisted local logs remain available through Export CSV.",
    "info"
  );
}

async function exportActivityLogCsv() {
  const response = await apiFetch("/api/log-export.csv", { cache: "no-store" });
  if (!response.ok) {
    let message = "Could not export activity logs.";
    try {
      const payload = await response.json();
      if (payload && payload.error) {
        message = payload.error;
      }
    } catch (error) {
      // Ignore JSON parsing errors for download responses.
    }
    throw new Error(message);
  }
  const blob = await response.blob();
  const downloadUrl = URL.createObjectURL(blob);
  const link = document.createElement("a");
  const disposition = response.headers.get("Content-Disposition") || "";
  const filenameMatch = disposition.match(/filename=\"?([^"]+)\"?/i);
  link.href = downloadUrl;
  link.download = filenameMatch ? filenameMatch[1] : "mail-in-the-shell-activity.csv";
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(downloadUrl);
  setWorkflowStatus(`CSV exported as ${link.download}.`, "success");
  logActivity("CSV exported", `Downloaded ${link.download}.`, "success");
}

function openStep(element) {
  const blade = element?.dataset?.blade || element?.closest?.("[data-blade]")?.dataset?.blade;
  if (blade) {
    activateBlade(blade);
  }
}

function setSendingUi(isBusy) {
  state.isBusy = isBusy;
  refreshActionAvailability();
}

async function pollSendJob(jobId) {
  while (true) {
    const result = await postJson("/api/send-status", { job_id: jobId });
    renderLiveSendProgress(result.job);
    if (result.job.status === "completed" || result.job.status === "failed") {
      return result.job;
    }
    await sleep(350);
  }
}

function sleep(ms) {
  return new Promise(resolve => window.setTimeout(resolve, ms));
}

function logActivity(title, message, tone = "info", options = {}) {
  const entry = document.createElement("article");
  entry.className = "log-entry";
  entry.dataset.tone = tone;

  const top = document.createElement("div");
  top.className = "log-entry-top";

  const titleWrap = document.createElement("div");
  titleWrap.className = "log-title-wrap";

  const titleNode = document.createElement("strong");
  titleNode.className = "log-title";
  titleNode.textContent = title;

  const badge = document.createElement("span");
  badge.className = "log-badge";
  badge.textContent = toneLabel(tone);

  const time = document.createElement("span");
  time.className = "log-time";
  time.textContent = timestampLabel();

  const body = document.createElement("p");
  body.className = "log-message";
  body.textContent = message;

  titleWrap.append(titleNode, badge);
  top.append(titleWrap, time);
  entry.append(top, body);
  elements.activityLog.prepend(entry);

  while (elements.activityLog.children.length > MAX_LOG_ENTRIES) {
    elements.activityLog.removeChild(elements.activityLog.lastElementChild);
  }

  refreshBladeTelemetry();

  if (options.persist !== false) {
    void persistActivityLog({ title, message, tone });
  }
}

function toneLabel(tone) {
  if (tone === "success") return "Success";
  if (tone === "warning") return "Warning";
  if (tone === "error") return "Error";
  return "Info";
}

function timestampLabel() {
  return new Date().toLocaleString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    month: "short",
    day: "2-digit",
  });
}

function formatLabel(mode) {
  if (mode === "text") return "Plain text only";
  if (mode === "both") return "HTML + plain text";
  return "HTML only";
}

function humanFileSize(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  const rounded = value >= 10 || unitIndex === 0 ? Math.round(value) : value.toFixed(1);
  return `${rounded} ${units[unitIndex]}`;
}
