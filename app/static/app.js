const PAGE_IDS = ["dashboard", "diagnostics", "config", "sources"];
const ACTIVE_PAGE_STORAGE_KEY = "opcua-dashboard.active-page";

function normalizedPageId(value) {
  return PAGE_IDS.includes(value) ? value : "dashboard";
}

function getInitialPage() {
  const hashPage = String(window.location.hash || "").replace(/^#/, "");
  if (PAGE_IDS.includes(hashPage)) return hashPage;
  try {
    return normalizedPageId(window.sessionStorage.getItem(ACTIVE_PAGE_STORAGE_KEY));
  } catch (_) {
    return "dashboard";
  }
}

function persistActivePage(page) {
  try {
    window.sessionStorage.setItem(ACTIVE_PAGE_STORAGE_KEY, page);
  } catch (_) {
    // The URL hash remains the fallback when storage is unavailable.
  }
  if (window.location.hash !== `#${page}`) {
    window.history.replaceState(null, "", `${window.location.pathname}${window.location.search}#${page}`);
  }
}

const state = {
  snapshot: null,
  operations: [],
  filter: "",
  dictFilter: "",
  configTreeFilter: "",
  configBrowseItems: [],
  configBrowseExpanded: new Set(),
  configBrowseLoadedNodes: new Set(),
  dictionary: [],
  dictVisibleLimit: 100,
  configNodes: [],
  draftNodes: [],
  selectedBrowseNode: null,
  selectedDictParamId: null,
  pendingAssignNodeId: null,
  pendingAssignGroupId: null,
  pendingAssignGroupPath: null,
  workspaceCollapsed: new Set(),
  workspaceVisibleLimits: new Map(),
  workspaceUngroupedLimit: 100,
  pendingConfigChanges: false,
  activePage: getInitialPage(),
  snapshotLoading: false,
  overloadCounterLoading: false,
  publishAuditPage: 1,
  nodesPage: 1,
  selectedMonitoringNodeIds: new Set(),
  overloadCounterEnabled: false,
  overloadCounterStartedAtMs: null,
  modalReturnFocus: null,
};

const GROUP_SUBSCRIBE_MAX_DEPTH = "2";
const OPC_NODE_DRAG_TYPE = "application/x-opc-node";
const PUBLISH_AUDIT_PAGE_SIZE = 200;
const MONITORING_NODES_PAGE_SIZE = 20;
const DICTIONARY_PAGE_SIZE = 100;
const WORKSPACE_PAGE_SIZE = 100;
let dictionaryFilterTimer = null;

const formatDate = (value) => {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("ru-RU");
};

const formatValue = (value) => {
  if (value === null || value === undefined) return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};

const formatDurationUntil = (value) => {
  if (!value) return "-";
  const target = new Date(value);
  if (Number.isNaN(target.getTime())) return "-";
  const totalSeconds = Math.max(0, Math.floor((target.getTime() - Date.now()) / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}ч ${String(minutes).padStart(2, "0")}м`;
  if (minutes > 0) return `${minutes}м ${String(seconds).padStart(2, "0")}с`;
  return `${seconds}с`;
};

const formatElapsedSeconds = (value) => {
  const totalSeconds = Math.max(0, Number(value) || 0);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = Math.floor(totalSeconds % 60);
  if (hours > 0) return `${hours}ч ${String(minutes).padStart(2, "0")}м`;
  if (minutes > 0) return `${minutes}м ${String(seconds).padStart(2, "0")}с`;
  return `${seconds}с`;
};

const statusBadge = (stateValue, connected = false) => {
  if (stateValue === "connected" || connected) return "badge-ok";
  if (stateValue === "degraded" || stateValue === "reconnecting") return "badge-warn";
  if (stateValue === "failed" || stateValue === "disconnected") return "badge-bad";
  return "badge-muted";
};

const setText = (id, value) => {
  const element = document.getElementById(id);
  if (element) element.textContent = value;
};

const pretty = (value) => {
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
};

function errorMessageFromPayload(payload) {
  if (typeof payload === "string") {
    const trimmed = payload.trim();
    if (!trimmed) return "Пустой ответ сервиса.";
    try {
      return errorMessageFromPayload(JSON.parse(trimmed));
    } catch (_) {
      return trimmed;
    }
  }
  if (payload && typeof payload === "object") {
    if (typeof payload.detail === "string") return payload.detail;
    if (Array.isArray(payload.detail)) {
      return payload.detail
        .map((item) => item?.msg || item?.message || pretty(item))
        .filter(Boolean)
        .join("; ");
    }
    if (typeof payload.message === "string") return payload.message;
  }
  return pretty(payload ?? "Неизвестная ошибка.");
}

const clone = (value) => {
  if (typeof structuredClone === "function") return structuredClone(value);
  return JSON.parse(JSON.stringify(value));
};

function ensureUniqueNodeConfigIds(nodes) {
  const seen = new Set();
  return (Array.isArray(nodes) ? nodes : []).map((node, index) => {
    let id = String(node.id || "");
    if (!id || seen.has(id)) {
      id = makeNodeConfigId(node.endpoint_id, node.node_id || `${index}`, "node");
    }
    if (seen.has(id)) {
      id = makeNodeConfigId(node.endpoint_id, `${node.node_id || "node"}\u0000${index}`, "node");
    }
    seen.add(id);
    return id === node.id ? node : { ...node, id };
  });
}

const nodeKey = (endpointId, nodeId) => `${endpointId || ""}\u0000${nodeId || ""}`;

const hasDragType = (dataTransfer, type) => Array.from(dataTransfer?.types || []).includes(type);

const buildByParent = (items) => {
  const byParent = new Map();
  for (const item of items) {
    const key = item.parent_node_id ?? "__root__";
    const bucket = byParent.get(key) || [];
    bucket.push(item);
    byParent.set(key, bucket);
  }
  return byParent;
};

function browsePathForNode(browseNode, items = state.configBrowseItems) {
  if (!browseNode?.node_id) return [];
  const byId = new Map(items.map((item) => [item.node_id, item]));
  let current = byId.get(browseNode.node_id) || browseNode;
  const path = [];
  const visited = new Set();
  while (current?.node_id && !visited.has(current.node_id)) {
    visited.add(current.node_id);
    path.unshift(current.display_name || current.browse_name || current.node_id);
    current = current.parent_node_id ? byId.get(current.parent_node_id) : null;
  }
  return path;
}

const buildDictByName = () => {
  const result = new Map();
  for (const param of state.dictionary) {
    if (param.name) result.set(param.name, param);
    if (param.description) result.set(param.description, param);
  }
  return result;
};

function setConfigStatus(message, tone = "info", action = null) {
  const element = document.getElementById("configStatus");
  if (!element) return;
  element.className = `config-status tone-${tone}`;
  element.replaceChildren(document.createTextNode(message));
  if (action?.label && typeof action.onClick === "function") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "config-status-action";
    button.textContent = action.label;
    button.addEventListener("click", action.onClick, { once: true });
    element.append(button);
  }
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, { cache: "no-store", ...options });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) throw new Error(errorMessageFromPayload(payload));
  return payload;
}

async function loadOperations() {
  state.operations = await fetchJson("/api/operations");
  renderOperations();
}

async function fetchSnapshot() {
  if (state.snapshotLoading) return state.snapshot;
  state.snapshotLoading = true;
  try {
    state.snapshot = await fetchJson("/api/snapshot");
    render();
    return state.snapshot;
  } finally {
    state.snapshotLoading = false;
  }
}

async function fetchStatusOverloadCounter() {
  if (state.overloadCounterLoading) return null;
  state.overloadCounterLoading = true;
  try {
    const counter = await fetchJson("/api/status-overload-counter");
    if (state.snapshot?.diagnostics) {
      state.snapshot.diagnostics.status_overload_counter = counter;
    }
    renderStatusOverloadCounter(counter);
    return counter;
  } catch {
    return null;
  } finally {
    state.overloadCounterLoading = false;
  }
}

function setBusyState(button, busy) {
  if (!button) return;
  button.classList.toggle("is-loading", busy);
  button.disabled = busy;
  button.setAttribute("aria-busy", busy ? "true" : "false");
}

async function withBusy(button, work) {
  setBusyState(button, true);
  try {
    return await work();
  } finally {
    setBusyState(button, false);
  }
}

function render() {
  const snapshot = state.snapshot;
  if (!snapshot) return;

  const connected = snapshot.connections.filter((item) => item.connected).length;
  const activeNodes = snapshot.nodes.filter((item) => item.status?.active).length;
  const readyBadge = document.getElementById("readyBadge");
  const healthy = Boolean(snapshot.healthy);

  readyBadge.className = `badge ${healthy ? "badge-ok" : "badge-bad"}`;
  readyBadge.textContent = healthy ? "client up" : "client down";

  setText("clientUrl", `${snapshot.client.base_url} · timeout ${snapshot.client.timeout_seconds ?? "-"}s`);
  setText("readyHint", `ready: ${snapshot.ready ? "yes" : "no"}`);
  setText("updatedAt", formatDate(snapshot.updated_at));
  setText("endpointCount", snapshot.connections.length);
  setText("connectedCount", `${connected} connected`);
  setText("nodeCount", snapshot.nodes.length);
  setText("activeNodeCount", `${activeNodes} active`);
  setText("bufferCount", snapshot.buffer.buffered_events ?? "?");
  setText("deadLetterCount", `${snapshot.buffer.dead_letter_events ?? "?"} dead-letter`);
  setText("rabbitMessages", snapshot.rabbitmq?.messages ?? "?");
  setText(
    "rabbitDetails",
    snapshot.rabbitmq?.available
      ? `${snapshot.rabbitmq.queue ?? "queue"} · ${snapshot.rabbitmq.messages_ready ?? 0} ready / ${snapshot.rabbitmq.messages_unacknowledged ?? 0} unacked`
      : `${snapshot.rabbitmq?.queue ?? "RabbitMQ"} · ${snapshot.rabbitmq?.error ?? "недоступен"}`,
  );
  setText("eventCount", snapshot.events?.length ?? 0);
  setText("alarmCount", `${snapshot.alarms?.length ?? 0} alarms`);

  renderEndpointOptions(snapshot.connections);
  renderConnections(snapshot.connections, snapshot.readiness?.endpoints || []);
  renderConnectionEvents(snapshot.diagnostics?.connection_events || []);
  renderEvents([...(snapshot.alarms || []), ...(snapshot.events || [])]);
  renderDiagnostics(snapshot.diagnostics || {});
  renderNodes(snapshot.nodes);
  if (state.activePage === "sources" && state.sourcesLoaded) renderSourcesList();
}

function renderEndpointOptions(connections) {
  for (const id of ["apiEndpoint", "configEndpoint"]) {
    const select = document.getElementById(id);
    const current = select.value;
    select.innerHTML = "";
    for (const connection of connections) {
      const option = document.createElement("option");
      option.value = connection.endpoint_id;
      option.textContent = connection.endpoint_id;
      select.append(option);
    }
    if (current) select.value = current;
  }
}

function renderOperations() {
  const technical = document.getElementById("technicalApiButtons");
  const functional = document.getElementById("functionalApiButtons");
  technical.innerHTML = "";
  functional.innerHTML = "";

  for (const operation of state.operations) {
    const button = document.createElement("button");
    button.className = "api-button";
    button.type = "button";
    const required = operation.needs?.length ? `needs: ${operation.needs.join(", ")}` : "no body";
    button.innerHTML = `
      ${operation.title}
      <small>${operation.method} ${operation.path}</small>
      <small class="api-needs">${required}</small>
    `;
    button.addEventListener("click", (event) => runOperation(operation, event.currentTarget));
    if (operation.group === "technical") {
      technical.append(button);
    } else {
      functional.append(button);
    }
  }
}

async function runOperation(operation, button) {
  const result = document.getElementById("apiResult");
  clearFieldErrors();
  const { payload, missing } = buildOperationPayload(operation);

  if (missing.length) {
    markFieldErrors(missing);
    result.textContent = pretty({
      ok: false,
      operation: operation.id,
      request_sent: false,
      reason: "Не заполнены обязательные поля для этого API-запроса.",
      required: operation.needs || [],
      missing,
      request_preview: payload,
    });
    return;
  }

  result.classList.add("loading");
  result.textContent = pretty({
    status: `running ${operation.id}...`,
    request_preview: payload,
  });

  try {
    const response = await withBusy(button, () =>
      fetchJson("/api/client/request", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
    result.classList.remove("loading");
    result.textContent = pretty(response);
    await fetchSnapshot();
  } catch (error) {
    result.classList.remove("loading");
    result.textContent = error.message;
  }
}


function buildOperationPayload(operation) {
  const endpointId = document.getElementById("apiEndpoint").value || null;
  const nodeId = document.getElementById("apiNodeId").value.trim() || null;
  const rawValue = document.getElementById("apiValue").value.trim();
  const payload = {
    operation: operation.id,
    endpoint_id: endpointId,
    node_id: nodeId,
    value: rawValue ? parseApiValue(rawValue) : null,
    max_depth: 2,
    include_variables: true,
    include_objects: true,
  };
  const missing = [];

  for (const field of operation.needs || []) {
    if (field === "endpoint_id" && !endpointId) missing.push(field);
    if (field === "node_id" && !nodeId) missing.push(field);
    if (field === "value" && !rawValue) missing.push(field);
  }

  return { payload, missing };
}

function clearFieldErrors() {
  for (const element of document.querySelectorAll(".field-error")) {
    element.classList.remove("field-error");
  }
}

function markFieldErrors(fields) {
  const fieldIds = {
    endpoint_id: "apiEndpoint",
    node_id: "apiNodeId",
    value: "apiValue",
  };
  for (const field of fields) {
    const element = document.getElementById(fieldIds[field]);
    if (element) element.classList.add("field-error");
  }
}

function parseApiValue(raw) {
  const value = raw.trim();
  if (!value) return null;
  if (value === "true") return true;
  if (value === "false") return false;
  if (!Number.isNaN(Number(value))) return Number(value);
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function getConnectionError(item, readinessEndpoints) {
  if (item.last_error) return item.last_error;
  const readinessMatch = readinessEndpoints.find((entry) => entry.endpoint_id === item.endpoint_id);
  if (readinessMatch?.last_error) return readinessMatch.last_error;
  if (item.cooldown) return `Cooldown до ${formatDate(item.cooldown_until)}. Следующая попытка через ${formatDurationUntil(item.cooldown_until)}.`;
  if (item.connection_phase === "discovery") return "Discovery: клиент ожидает endpoints от OPC UA сервера.";
  if (item.connection_phase === "session") return "Session: клиент открывает OPC UA сессию.";
  if (item.connection_phase === "subscriptions") return "Subscriptions: клиент создает подписки.";
  if (item.state === "failed") return "Подключение завершилось ошибкой, но OPC UA client не передал текст причины.";
  if (item.state === "reconnecting") return "Идет переподключение к OPC UA серверу.";
  if (item.state === "disconnected") return "Соединение разорвано.";
  return "-";
}

function renderConnections(connections, readinessEndpoints = []) {
  const table = document.getElementById("connectionsTable");
  table.innerHTML = "";
  if (!connections.length) {
    table.innerHTML = `<tr><td colspan="8" class="muted">Endpoint не зарегистрированы.</td></tr>`;
    return;
  }

  for (const item of connections) {
    const row = document.createElement("tr");
    const nextRetry = item.next_retry_at || item.cooldown_until;
    const phase = item.cooldown ? "cooldown" : item.connection_phase || item.state || "-";
    const retryHint = nextRetry
      ? `${formatDate(nextRetry)}${item.cooldown ? ` · ${formatDurationUntil(nextRetry)}` : ""}`
      : "-";
    const error = getConnectionError(item, readinessEndpoints);
    row.innerHTML = `
      <td class="mono">${item.endpoint_id}</td>
      <td><span class="badge ${statusBadge(item.state, item.connected)}">${item.state}</span></td>
      <td><span class="badge ${phaseBadge(phase)}">${escapeHtml(phase)}</span></td>
      <td>${formatDate(item.last_data_at)}</td>
      <td class="node-id">${escapeHtml(retryHint)}</td>
      <td>${item.reconnect_attempts}</td>
      <td class="node-id">${escapeHtml(error)}</td>
      <td><button class="btn compact reconnect-now" type="button" data-endpoint-id="${escapeHtml(item.endpoint_id)}">Reconnect</button></td>
    `;
    table.append(row);
  }
}

function phaseBadge(phase) {
  if (phase === "connected" || phase === "monitoring") return "badge-ok";
  if (phase === "cooldown") return "badge-warn";
  if (["connecting", "discovery", "session", "session_check", "subscriptions", "retry_wait", "reconnecting"].includes(phase)) {
    return "badge-warn";
  }
  if (phase === "failed" || phase === "disconnected") return "badge-bad";
  return "badge-muted";
}

function renderConnectionEvents(events) {
  const table = document.getElementById("connectionEventsTable");
  if (!table) return;
  table.innerHTML = "";
  setText("connectionEventsMeta", `${events.length} последних событий`);
  if (!events.length) {
    table.innerHTML = `<tr><td colspan="5" class="muted">Истории подключения пока нет.</td></tr>`;
    return;
  }
  for (const item of events.slice(0, 30)) {
    const row = document.createElement("tr");
    const details = connectionEventDetails(item);
    row.innerHTML = `
      <td>${escapeHtml(formatDate(item.recorded_at))}</td>
      <td class="mono">${escapeHtml(item.endpoint_id || "-")}</td>
      <td class="mono">${escapeHtml(item.event || "-")}</td>
      <td><span class="badge ${phaseBadge(item.stage || item.phase)}">${escapeHtml(item.stage || item.phase || "-")}</span></td>
      <td class="node-id">${escapeHtml(details)}</td>
    `;
    table.append(row);
  }
}

function connectionEventDetails(item) {
  const parts = [];
  if (item.error) parts.push(item.error);
  if (item.url) parts.push(item.url);
  if (item.error_type) parts.push(`type: ${item.error_type}`);
  if (item.attempts !== undefined) parts.push(`attempts: ${item.attempts}`);
  if (item.cooldown_seconds !== undefined) parts.push(`cooldown: ${item.cooldown_seconds}s`);
  if (item.next_retry_at) parts.push(`next: ${formatDate(item.next_retry_at)}`);
  if (item.endpoint_count !== undefined) parts.push(`endpoints: ${item.endpoint_count}`);
  return parts.join(" · ") || "-";
}

function renderEvents(events) {
  const table = document.getElementById("eventsTable");
  if (!table) return;
  table.innerHTML = "";
  const visible = events.slice(-20).reverse();
  if (!visible.length) {
    table.innerHTML = `<tr><td colspan="4" class="muted">События не получены или подписка выключена.</td></tr>`;
    return;
  }
  for (const event of visible) {
    const type = event.event_type || event.EventType || event.ConditionName || "-";
    const time = event.Time || event.ReceiveTime || event.received_at;
    const message = event.Message || event.message || event.SourceName || event.SourceNode || "-";
    const row = document.createElement("tr");
    row.innerHTML = `
      <td class="mono">${escapeHtml(event.endpoint_id || "-")}</td>
      <td>${escapeHtml(formatValue(type))}</td>
      <td>${escapeHtml(formatDate(time))}</td>
      <td class="node-id">${escapeHtml(formatValue(message))}</td>
    `;
    table.append(row);
  }
}

function decisionBadge(decision) {
  if (decision === "published") return "badge-ok";
  if (decision === "suppressed") return "badge-muted";
  if (decision === "buffered") return "badge-warn";
  if (decision === "failed") return "badge-bad";
  return "badge-muted";
}

function validationBadge(validationState) {
  if (validationState === "valid") return "badge-ok";
  if (validationState === "invalid") return "badge-bad";
  if (validationState === "duplicate") return "badge-muted";
  return "badge-muted";
}

function severityBadge(severity) {
  if (severity === "critical" || severity === "major") return "badge-bad";
  if (severity === "warning") return "badge-warn";
  if (severity === "info") return "badge-muted";
  return "badge-muted";
}

function renderDiagnostics(diagnostics) {
  const publishStats = diagnostics.publish_stats || {};
  const decisions = publishStats.decisions || {};
  const overloadCounter = diagnostics.status_overload_counter || {};
  const audit = Array.isArray(diagnostics.publish_audit) ? diagnostics.publish_audit : [];

  setText("diagPublishedCount", decisions.published ?? 0);
  setText("diagSuppressedCount", decisions.suppressed ?? 0);
  setText("diagAuditWindowCount", publishStats.window_records ?? audit.length);
  setText("diagAuditWindowHint", `${publishStats.max_records ?? "-"} max · TTL ${formatElapsedSeconds(publishStats.ttl_seconds ?? 0)}`);
  setText("publishAuditMeta", `${audit.length} записей · окно ${publishStats.window_records ?? 0}/${publishStats.max_records ?? "-"}`);

  renderStatusOverloadCounter(overloadCounter);
  renderPublishAudit(audit);
}

function renderStatusOverloadCounter(counter) {
  const enabled = counter.enabled === true;
  state.overloadCounterEnabled = enabled;
  state.overloadCounterStartedAtMs = resolveOverloadCounterStartMs(counter, state.overloadCounterStartedAtMs);
  setText("overloadCounterStatus", enabled ? "ON" : "OFF");
  setText("overloadCounterValue", enabled ? counter.count ?? 0 : 0);
  setText("overloadCounterNodes", counter.active_nodes ?? 0);
  updateStatusOverloadCounterTimer();
  setText("overloadCounterStarted", enabled ? `с ${formatDate(counter.started_at)}` : "выключен");
  setText("overloadCounterLastSeen", enabled ? formatDate(counter.last_seen_at) : "-");
  setText("overloadCounterLastNode", enabled ? counter.last_parameter_code || counter.last_node_id || "-" : "-");
  const button = document.getElementById("overloadCounterToggle");
  if (button) {
    button.textContent = enabled ? "Выключить" : "Включить";
    button.dataset.enabled = enabled ? "true" : "false";
    button.classList.toggle("secondary", enabled);
  }
}

function resolveOverloadCounterStartMs(counter, currentStartMs) {
  if (counter.enabled === false) return null;
  if (counter.started_at) {
    const started = new Date(counter.started_at);
    if (!Number.isNaN(started.getTime())) return started.getTime();
  }
  if (currentStartMs) return currentStartMs;
  if (counter.elapsed_seconds !== undefined && counter.elapsed_seconds !== null) {
    return Date.now() - Math.max(0, Number(counter.elapsed_seconds) || 0) * 1000;
  }
  return null;
}

function updateStatusOverloadCounterTimer() {
  if (!state.overloadCounterEnabled) {
    setText("overloadCounterElapsed", "0с");
    return;
  }
  if (!state.overloadCounterStartedAtMs) {
    setText("overloadCounterElapsed", "-");
    return;
  }
  const elapsedSeconds = Math.max(0, Math.floor((Date.now() - state.overloadCounterStartedAtMs) / 1000));
  setText("overloadCounterElapsed", formatElapsedSeconds(elapsedSeconds));
}

function renderPublishAudit(records) {
  const table = document.getElementById("publishAuditTable");
  if (!table) return;
  table.innerHTML = "";
  if (!records.length) {
    table.innerHTML = `<tr><td colspan="8" class="muted">Последних решений публикации нет.</td></tr>`;
    setPublishAuditPagination(0, 1);
    return;
  }
  const totalPages = Math.max(1, Math.ceil(records.length / PUBLISH_AUDIT_PAGE_SIZE));
  state.publishAuditPage = Math.min(Math.max(state.publishAuditPage, 1), totalPages);
  const start = (state.publishAuditPage - 1) * PUBLISH_AUDIT_PAGE_SIZE;
  const pageRecords = records.slice(start, start + PUBLISH_AUDIT_PAGE_SIZE);
  setPublishAuditPagination(records.length, totalPages);

  for (const record of pageRecords) {
    const row = document.createElement("tr");
    const statusLabel = `${record.status_name || record.quality_code || "-"} (${record.published_status ?? "-"})`;
    const validationState = record.validation_state || "-";
    const validationErrors = Array.isArray(record.validation_errors) ? record.validation_errors.filter(Boolean) : [];
    const reason = [record.reason, record.error, ...validationErrors].filter(Boolean).join(" · ") || "-";
    row.innerHTML = `
      <td><span class="badge ${decisionBadge(record.decision)}">${escapeHtml(record.decision || "-")}</span></td>
      <td><span class="badge ${validationBadge(validationState)}">${escapeHtml(validationState)}</span></td>
      <td class="mono">${escapeHtml(statusLabel)}</td>
      <td class="node-id">${escapeHtml(record.value_preview || "-")}</td>
      <td>${escapeHtml(formatDate(record.source_timestamp))}</td>
      <td>${escapeHtml(formatDate(record.recorded_at))}</td>
      <td class="node-id">${escapeHtml(record.parameter_code || record.node_id || "-")}</td>
      <td class="node-id">${escapeHtml(reason)}</td>
    `;
    table.append(row);
  }
}

function setPublishAuditPagination(totalRecords, totalPages) {
  setText("publishAuditPage", `${state.publishAuditPage} / ${totalPages}`);
  const prev = document.getElementById("publishAuditPrev");
  const next = document.getElementById("publishAuditNext");
  if (prev) prev.disabled = state.publishAuditPage <= 1 || totalRecords <= PUBLISH_AUDIT_PAGE_SIZE;
  if (next) next.disabled = state.publishAuditPage >= totalPages || totalRecords <= PUBLISH_AUDIT_PAGE_SIZE;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

// Selected Bootstrap Icons are embedded as SVG paths so the dashboard does not
// depend on a CDN at runtime. Source: https://icons.getbootstrap.com/ (MIT).
function bootstrapIcon(name, className = "") {
  const paths = {
    "folder-open": '<path d="M1 3.5A1.5 1.5 0 0 1 2.5 2h2.764c.958 0 1.76.56 2.311 1.184C7.985 3.648 8.48 4 9 4h4.5A1.5 1.5 0 0 1 15 5.5v.64c.57.265.94.876.856 1.546l-.64 5.124A2.5 2.5 0 0 1 12.733 15H3.266a2.5 2.5 0 0 1-2.481-2.19l-.64-5.124A1.5 1.5 0 0 1 1 6.14zM2 6h12v-.5a.5.5 0 0 0-.5-.5H9c-.964 0-1.71-.629-2.174-1.154C6.374 3.334 5.82 3 5.264 3H2.5a.5.5 0 0 0-.5.5zm-.367 1a.5.5 0 0 0-.496.562l.64 5.124A1.5 1.5 0 0 0 3.266 14h9.468a1.5 1.5 0 0 0 1.489-1.314l.64-5.124A.5.5 0 0 0 14.367 7z"/>',
    object: '<path d="M8 1.5 14 4.75v6.5L8 14.5l-6-3.25v-6.5zM2.4 4.95 8 8l5.6-3.05M8 8v6.2" fill="none" stroke="currentColor" stroke-width="1.25" stroke-linejoin="round"/>',
    variable: '<path d="m3 4.5 4 7m0-7-4 7M10 5h3m-3 3h3m-3 3h3" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/>',
    method: '<rect x="1.5" y="2" width="13" height="12" rx="2" fill="none" stroke="currentColor" stroke-width="1.2"/><path d="M6 5 3.8 8 6 11m4-6 2.2 3-2.2 3" fill="none" stroke="currentColor" stroke-width="1.35" stroke-linecap="round" stroke-linejoin="round"/>',
    node: '<circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-width="1.25"/><circle cx="8" cy="8" r="1.5"/>',
    group: '<rect x="1.5" y="2.5" width="8.5" height="7" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.2"/><rect x="6" y="6.5" width="8.5" height="7" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.2"/>',
    "chevron-right": '<path fill-rule="evenodd" d="M6.646 4.646a.5.5 0 0 1 .708 0l3 3a.5.5 0 0 1 0 .708l-3 3a.5.5 0 0 1-.708-.708L9.293 8 6.646 5.354a.5.5 0 0 1 0-.708"/>',
    trash: '<path d="M6.5 1h3a.5.5 0 0 1 .5.5v1H6v-1a.5.5 0 0 1 .5-.5M11 2.5v-1A1.5 1.5 0 0 0 9.5 0h-3A1.5 1.5 0 0 0 5 1.5v1H1.5a.5.5 0 0 0 0 1h.538l.853 10.66A2 2 0 0 0 4.885 16h6.23a2 2 0 0 0 1.994-1.84l.853-10.66h.538a.5.5 0 0 0 0-1zm1.958 1-.846 10.58a1 1 0 0 1-.997.92h-6.23a1 1 0 0 1-.997-.92L3.042 3.5zm-7.487 1a.5.5 0 0 1 .528.47l.5 8.5a.5.5 0 0 1-.998.06L5 5.03a.5.5 0 0 1 .47-.53Zm5.058 0a.5.5 0 0 1 .47.53l-.5 8.5a.5.5 0 1 1-.998-.06l.5-8.5a.5.5 0 0 1 .528-.47M8 4.5a.5.5 0 0 1 .5.5v8.5a.5.5 0 0 1-1 0V5a.5.5 0 0 1 .5-.5"/>',
    diagram: '<path fill-rule="evenodd" d="M6 3.5A1.5 1.5 0 0 1 7.5 2h1A1.5 1.5 0 0 1 10 3.5v1A1.5 1.5 0 0 1 8.5 6v1H14a.5.5 0 0 1 .5.5v1a.5.5 0 0 1-1 0V8h-5v.5a.5.5 0 0 1-1 0V8h-5v.5a.5.5 0 0 1-1 0v-1A.5.5 0 0 1 2 7h5.5V6A1.5 1.5 0 0 1 6 4.5zM8.5 5a.5.5 0 0 0 .5-.5v-1a.5.5 0 0 0-.5-.5h-1a.5.5 0 0 0-.5.5v1a.5.5 0 0 0 .5.5zM0 11.5A1.5 1.5 0 0 1 1.5 10h1A1.5 1.5 0 0 1 4 11.5v1A1.5 1.5 0 0 1 2.5 14h-1A1.5 1.5 0 0 1 0 12.5zm1.5-.5a.5.5 0 0 0-.5.5v1a.5.5 0 0 0 .5.5h1a.5.5 0 0 0 .5-.5v-1a.5.5 0 0 0-.5-.5zm4.5.5A1.5 1.5 0 0 1 7.5 10h1a1.5 1.5 0 0 1 1.5 1.5v1A1.5 1.5 0 0 1 8.5 14h-1A1.5 1.5 0 0 1 6 12.5zm1.5-.5a.5.5 0 0 0-.5.5v1a.5.5 0 0 0 .5.5h1a.5.5 0 0 0 .5-.5v-1a.5.5 0 0 0-.5-.5zm4.5.5a1.5 1.5 0 0 1 1.5-1.5h1a1.5 1.5 0 0 1 1.5 1.5v1a1.5 1.5 0 0 1-1.5 1.5h-1a1.5 1.5 0 0 1-1.5-1.5zm1.5-.5a.5.5 0 0 0-.5.5v1a.5.5 0 0 0 .5.5h1a.5.5 0 0 0 .5-.5v-1a.5.5 0 0 0-.5-.5z"/>',
  };
  const path = paths[name];
  if (!path) return "";
  return `<svg class="bi-icon ${escapeHtml(className)}" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true" focusable="false">${path}</svg>`;
}

async function copyTextToClipboard(value) {
  const text = String(value || "");
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (_) {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.append(textarea);
    textarea.select();
    const copied = document.execCommand("copy");
    textarea.remove();
    return copied;
  }
}

function hasPersistedMappingForParam(paramId) {
  return state.configNodes.some((node) => node.dict_param_id === paramId);
}

function hasPersistedMappingForBrowseNode(endpointId, nodeId) {
  return state.configNodes.some((node) => node.endpoint_id === endpointId && node.node_id === nodeId);
}

function isSameBrowseNode(left, right) {
  if (!left || !right) return false;
  return left.endpoint_id === right.endpoint_id && left.node_id === right.node_id;
}

function updatePendingConfigChanges() {
  state.pendingConfigChanges = JSON.stringify(state.draftNodes) !== JSON.stringify(state.configNodes);
  const saveButton = document.getElementById("saveConfigButton");
  if (saveButton) {
    saveButton.classList.toggle("has-pending", state.pendingConfigChanges);
    saveButton.textContent = state.pendingConfigChanges ? "Сохранить в клиент •" : "Сохранить в клиент";
    saveButton.title = state.pendingConfigChanges ? "Есть несохранённые изменения" : "Несохранённых изменений нет";
  }
}

function clearConfigSelection() {
  state.selectedBrowseNode = null;
  state.selectedDictParamId = null;
  state.pendingAssignNodeId = null;
  state.pendingAssignGroupId = null;
  state.pendingAssignGroupPath = null;
}

function buildDraftStatusMessage() {
  return `Привязка добавлена в черновик. Нажмите «Сохранить в клиент», чтобы применить изменения. Всего нод в черновике: ${state.draftNodes.length}.`;
}

function buildConfigSavedMessage(savedNodes) {
  return `Сохранение прошло успешно. В клиенте настроено ${savedNodes} нод.`;
}

function buildSelectionStatusMessage() {
  const node = state.selectedBrowseNode;
  const param = state.dictionary.find((item) => item.id === state.selectedDictParamId);
  if (node && param) return `Пара выбрана: ${node.node_id} -> ${param.name}. Нажмите «Привязать», чтобы добавить в черновик.`;
  if (node) return `Выбрана нода ${node.node_id}. Теперь выберите параметр справа.`;
  if (param) return `Выбран параметр ${param.name}. Теперь выберите ноду слева.`;
  return "Выберите ноду и параметр для новой привязки.";
}

function renderNodes(nodes) {
  const table = document.getElementById("nodesTable");
  const visibleNodes = filteredMonitoringNodes(nodes);
  const totalPages = Math.max(1, Math.ceil(visibleNodes.length / MONITORING_NODES_PAGE_SIZE));
  state.nodesPage = Math.min(Math.max(1, state.nodesPage), totalPages);
  const start = (state.nodesPage - 1) * MONITORING_NODES_PAGE_SIZE;
  const pageNodes = visibleNodes.slice(start, start + MONITORING_NODES_PAGE_SIZE);
  const selectedCount = state.selectedMonitoringNodeIds.size;

  setText(
    "nodesPageMeta",
    `${visibleNodes.length} нод · ${selectedCount} выбрано · страница ${pageNodes.length}/${MONITORING_NODES_PAGE_SIZE}`,
  );
  setText("nodesPageIndicator", `${state.nodesPage} / ${totalPages}`);

  const prevButton = document.getElementById("nodesPrev");
  const nextButton = document.getElementById("nodesNext");
  if (prevButton) prevButton.disabled = state.nodesPage <= 1;
  if (nextButton) nextButton.disabled = state.nodesPage >= totalPages;

  const selectPage = document.getElementById("nodesSelectPage");
  if (selectPage) {
    const selectableIds = pageNodes.map((node) => node.config_id).filter(Boolean);
    const selectedOnPage = selectableIds.filter((id) => state.selectedMonitoringNodeIds.has(id)).length;
    selectPage.checked = selectableIds.length > 0 && selectedOnPage === selectableIds.length;
    selectPage.indeterminate = selectedOnPage > 0 && selectedOnPage < selectableIds.length;
  }

  table.innerHTML = "";
  if (!pageNodes.length) {
    table.innerHTML = `<tr><td colspan="11" class="muted">Ноды не найдены.</td></tr>`;
    return;
  }

  for (const node of pageNodes) {
    const s = node.status;
    const read = node.read;
    const active = Boolean(s?.active);
    const enabled = node.enabled !== false;
    const quality = read?.status_code || "-";
    const qualityOk = quality === "Good" || quality === "-";
    const row = document.createElement("tr");
    row.dataset.configId = node.config_id || "";
    row.innerHTML = `
      <td class="nt-select"><input class="node-select" type="checkbox" ${state.selectedMonitoringNodeIds.has(node.config_id) ? "checked" : ""} ${node.config_id ? "" : "disabled"} /></td>
      <td class="nt-status"><span class="badge ${active ? "badge-ok" : "badge-muted"}">${active ? "active" : "inactive"}</span></td>
      <td class="nt-enabled"><span class="badge ${enabled ? "badge-ok" : "badge-muted"}">${enabled ? "enabled" : "disabled"}</span></td>
      <td class="nt-param">${escapeHtml(node.parameter_code || "-")}</td>
      <td class="nt-value"><span class="nt-val-text">${escapeHtml(formatValue(read?.value))}</span>${node.read_error ? `<div class="nt-err">${escapeHtml(node.read_error)}</div>` : ""}</td>
      <td class="nt-quality ${qualityOk ? "" : "nt-quality-bad"}">${escapeHtml(quality)}</td>
      <td class="nt-time">${escapeHtml(formatDate(read?.source_timestamp || s?.last_value_at))}</td>
      <td class="nt-nodeid" title="${escapeHtml(node.node_id || "")}"><code>${escapeHtml(node.node_id || "-")}</code></td>
      <td class="nt-endpoint" title="${escapeHtml(node.endpoint_id || "")}">${escapeHtml(node.endpoint_id || "-")}</td>
      <td class="nt-mode">${escapeHtml(node.acquisition_mode || "-")}</td>
      <td class="nt-action"><button class="btn compact secondary toggle-node-enabled" type="button" ${node.config_id ? "" : "disabled"}>${enabled ? "Деактивировать" : "Активировать"}</button></td>
    `;
    row.addEventListener("click", (event) => {
      if (event.target.closest("input, button")) return;
      document.getElementById("apiEndpoint").value = node.endpoint_id || "";
      document.getElementById("apiNodeId").value = node.node_id || "";
    });
    row.querySelector(".node-select")?.addEventListener("change", (event) => {
      if (!node.config_id) return;
      if (event.target.checked) {
        state.selectedMonitoringNodeIds.add(node.config_id);
      } else {
        state.selectedMonitoringNodeIds.delete(node.config_id);
      }
      renderNodes(state.snapshot?.nodes || []);
    });
    row.querySelector(".toggle-node-enabled")?.addEventListener("click", (event) => {
      updateNodesEnabled([node.config_id], !enabled, event.currentTarget).catch((error) => showApiError(error));
    });
    row.title = "Нажмите, чтобы подставить endpoint и node_id в форму API.";
    table.append(row);
  }
}

function filteredMonitoringNodes(nodes) {
  const query = state.filter.trim().toLowerCase();
  if (!query) return nodes;
  return nodes.filter((node) => {
    const haystack = [
      node.parameter_code,
      node.node_id,
      node.endpoint_id,
      node.acquisition_mode,
      node.enabled === false ? "disabled" : "enabled",
      node.read?.data_type,
      String(node.read?.value ?? ""),
    ].join(" ").toLowerCase();
    return haystack.includes(query);
  });
}

function currentMonitoringPageNodes() {
  const nodes = state.snapshot?.nodes || [];
  const visibleNodes = filteredMonitoringNodes(nodes);
  const totalPages = Math.max(1, Math.ceil(visibleNodes.length / MONITORING_NODES_PAGE_SIZE));
  state.nodesPage = Math.min(Math.max(1, state.nodesPage), totalPages);
  const start = (state.nodesPage - 1) * MONITORING_NODES_PAGE_SIZE;
  return visibleNodes.slice(start, start + MONITORING_NODES_PAGE_SIZE);
}

async function updateSelectedNodesEnabled(enabled, button) {
  const ids = [...state.selectedMonitoringNodeIds];
  if (!ids.length) {
    showApiError(new Error("Выберите хотя бы одну ноду."));
    return;
  }
  await updateNodesEnabled(ids, enabled, button);
}

async function updateNodesEnabled(nodeIds, enabled, button) {
  const ids = nodeIds.filter(Boolean);
  if (!ids.length) return;
  await withBusy(button, async () => {
    await fetchJson("/api/client/request", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        operation: "set_nodes_enabled",
        node_ids: ids,
        enabled,
      }),
    });
    for (const id of ids) {
      state.selectedMonitoringNodeIds.delete(id);
    }
    await fetchSnapshot();
  });
}

async function updateStatusOverloadCounterEnabled(enabled, button) {
  await withBusy(button, async () => {
    const currentCounter = state.snapshot?.diagnostics?.status_overload_counter || {};
    renderStatusOverloadCounter({
      ...currentCounter,
      enabled,
      count: enabled ? 0 : currentCounter.count,
      started_at: enabled ? new Date().toISOString() : null,
      elapsed_seconds: 0,
      last_seen_at: null,
      last_node_id: null,
      last_parameter_code: null,
    });

    const result = await fetchJson("/api/client/request", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        operation: "set_status_overload_counter_enabled",
        enabled,
      }),
    });

    if (!result.ok) {
      throw new Error(pretty(result.response || "Не удалось переключить счетчик."));
    }

    renderStatusOverloadCounter(result.response || {});
    if (state.snapshot?.diagnostics) {
      state.snapshot.diagnostics.status_overload_counter = result.response || {};
    }
  });
}

function showApiError(error) {
  const result = document.getElementById("apiResult");
  if (result) result.textContent = error.message;
}

function switchPage(page, options = {}) {
  const activePage = normalizedPageId(page);
  state.activePage = activePage;
  if (options.persist !== false) persistActivePage(activePage);
  for (const section of document.querySelectorAll("[data-page]")) {
    section.classList.toggle("hidden", section.dataset.page !== activePage);
  }
  for (const button of document.querySelectorAll("[data-page-target]")) {
    const active = button.dataset.pageTarget === activePage;
    button.classList.toggle("active", active);
    if (active) {
      button.setAttribute("aria-current", "page");
    } else {
      button.removeAttribute("aria-current");
    }
  }
}

async function loadConfigurationPage(force = false) {
  if (state.pendingConfigChanges && !force) {
    setConfigStatus("В рабочей области есть несохранённые изменения. Обновление заменит черновик данными из клиента.", "warn", {
      label: "Всё равно обновить",
      onClick: () => loadConfigurationPage(true).catch((error) => setConfigStatus(error.message, "error")),
    });
    return;
  }
  setConfigStatus("Загружаю текущую конфигурацию клиента и справочник параметров...", "info");
  const [configResult, dictionaryResult] = await Promise.allSettled([
    fetchJson("/api/config/nodes"),
    fetchJson("/api/dictionary"),
  ]);

  const messages = [];
  if (configResult.status === "fulfilled") {
    state.configNodes = ensureUniqueNodeConfigIds(configResult.value.nodes);
    state.draftNodes = clone(state.configNodes);
    updatePendingConfigChanges();
    messages.push(`Конфигурация клиента: ${state.draftNodes.length} нод`);
  } else {
    messages.push(`Конфигурация клиента не загружена: ${configResult.reason.message}`);
  }

  if (dictionaryResult.status === "fulfilled") {
    state.dictionary = Array.isArray(dictionaryResult.value.params) ? dictionaryResult.value.params : [];
    messages.push(`Справочник параметров: ${state.dictionary.length} параметров`);
    const pagination = dictionaryResult.value.params_pagination;
    if (pagination && Array.isArray(pagination.pages)) {
      const pages = pagination.pages.map((page) => `${page.offset}:${page.count}`).join(", ");
      messages.push(`Страницы справочника: ${pages || "-"}`);
    }
    messages.push(`Сервис параметров: ${dictionaryResult.value.base_url || state.snapshot?.client?.params_service_base_url || "-"}`);
  } else {
    messages.push(`Справочник не загружен: ${dictionaryResult.reason.message}`);
  }

  messages.push(`OPC UA клиент: ${state.snapshot?.client?.base_url || "-"}`);
  clearConfigSelection();
  const loadedParts = [configResult, dictionaryResult].filter((result) => result.status === "fulfilled").length;
  setConfigStatus(messages.join("\n"), loadedParts === 2 ? "success" : loadedParts === 1 ? "warn" : "error");
  renderDictionary();
  renderMappings();
  renderConfigBrowseTree();
  renderSelectionBridge();
}

async function browseForConfig() {
  const endpointId = document.getElementById("configEndpoint").value;
  if (!endpointId) {
    setConfigStatus("Выберите endpoint для browse.", "warn");
    return;
  }
  setConfigStatus("Загружаю дерево OPC UA...", "info");
  const params = new URLSearchParams({
    endpoint_id: endpointId,
    max_depth: "1",
    include_variables: "true",
    include_objects: "true",
  });
  const response = await fetchJson(`/api/browse?${params.toString()}`);
  state.configBrowseItems = Array.isArray(response.items) ? response.items : [];
  state.configBrowseExpanded = new Set();
  state.configBrowseLoadedNodes = new Set();

  // Mark nodes whose children are already present in the response
  for (const item of state.configBrowseItems) {
    if (item.parent_node_id) state.configBrowseLoadedNodes.add(item.parent_node_id);
  }
  // Auto-expand only top-level nodes so user sees the first real layer
  state.configBrowseExpanded = new Set(
    state.configBrowseItems
      .filter((item) => item.has_children && Number(item.depth ?? 0) < 1)
      .map((item) => item.node_id),
  );

  updateConfigBrowseMeta();
  setConfigStatus(`Верхний уровень загружен: ${state.configBrowseItems.length} узлов. Раскрывайте ветки по мере необходимости.`, "success");
  renderConfigBrowseTree();
}

async function loadConfigNodeChildren(nodeId) {
  const endpointId = document.getElementById("configEndpoint").value;
  if (!endpointId) return;
  const params = new URLSearchParams({
    endpoint_id: endpointId,
    node_id: nodeId,
    max_depth: "1",
    include_variables: "true",
    include_objects: "true",
  });
  const response = await fetchJson(`/api/browse?${params.toString()}`);
  const newItems = Array.isArray(response.items) ? response.items : [];
  const children = newItems.filter((i) => i.parent_node_id === nodeId);
  const existingIds = new Set(state.configBrowseItems.map((i) => i.node_id));
  for (const child of children) {
    if (!existingIds.has(child.node_id)) {
      state.configBrowseItems.push(child);
      existingIds.add(child.node_id);
    }
  }
  state.configBrowseLoadedNodes.add(nodeId);
  updateConfigBrowseMeta();
}

function updateConfigBrowseMeta() {
  const total = state.configBrowseItems.length;
  const variables = state.configBrowseItems.filter((item) => item.node_class === "Variable").length;
  setText("configBrowseMeta", `Загружено: ${total} · Variable: ${variables}`);
}

function getTreeVisibleIds(items, query) {
  const q = query.toLowerCase();
  const matchingIds = new Set();
  for (const item of items) {
    const name = (item.display_name || item.browse_name || item.node_id).toLowerCase();
    if (name.includes(q)) matchingIds.add(item.node_id);
  }
  if (!matchingIds.size) return { visible: new Set(), matching: matchingIds };
  const byId = new Map(items.map((i) => [i.node_id, i]));
  const visible = new Set(matchingIds);
  for (const id of matchingIds) {
    let cur = byId.get(id);
    while (cur?.parent_node_id && !visible.has(cur.parent_node_id)) {
      visible.add(cur.parent_node_id);
      cur = byId.get(cur.parent_node_id);
    }
  }
  return { visible, matching: matchingIds };
}

function renderConfigBrowseTree() {
  const root = document.getElementById("configBrowseTree");
  const items = state.configBrowseItems;
  if (!items.length) {
    root.innerHTML = `<div class="tree-empty">Нажмите Browse, чтобы загрузить дерево.</div>`;
    return;
  }

  const byParent = buildByParent(items);

  const endpointId = document.getElementById("configEndpoint").value || "";
  const treeFilter = state.configTreeFilter.trim();
  const { visible: visibleIds, matching: matchingIds } = treeFilter
    ? getTreeVisibleIds(items, treeFilter)
    : { visible: null, matching: null };

  const wsNodeIds = new Set(state.draftNodes.filter((n) => n.endpoint_id === endpointId).map((n) => n.node_id));
  const persistedNodeKeys = new Set(state.configNodes.map((n) => nodeKey(n.endpoint_id, n.node_id)));
  // Direct-parent marking: only the immediate parent of a workspace node gets the green indicator,
  // not all transitive ancestors (prevents Objects/Server from being incorrectly highlighted)
  const wsParentIds = new Set();
  for (const item of items) {
    if (wsNodeIds.has(item.node_id) && item.parent_node_id) {
      wsParentIds.add(item.parent_node_id);
    }
  }

  const renderBranch = (parentId, level) => {
    let nodes = byParent.get(parentId) || [];
    if (visibleIds) nodes = nodes.filter((n) => visibleIds.has(n.node_id));
    return nodes
      .map((item) => {
        const children = byParent.get(item.node_id) || [];
        const isLoaded = state.configBrowseLoadedNodes.has(item.node_id) || byParent.has(item.node_id);
        const expandable = isLoaded ? children.length > 0 : Boolean(item.has_children);
        const expanded = visibleIds ? (children.length > 0) : state.configBrowseExpanded.has(item.node_id);
        const isVariable = item.node_class === "Variable";
        const isObject = item.node_class === "Object";
        const isMethod = item.node_class === "Method";
        const nodeKind = isObject ? "object" : isVariable ? "variable" : isMethod ? "method" : "node";
        const inWorkspace = wsNodeIds.has(item.node_id);
        const parentInWs = wsParentIds.has(item.node_id);
        const mapped = persistedNodeKeys.has(nodeKey(endpointId, item.node_id));
        const rawLabel = item.display_name || item.browse_name || item.node_id;
        const isMatch = matchingIds?.has(item.node_id);
        const label = isMatch
          ? `<span class="tree-name-match">${escapeHtml(rawLabel)}</span>`
          : escapeHtml(rawLabel);
        const nodeJson = escapeHtml(JSON.stringify({ ...item, endpoint_id: endpointId }));
        const showAllBtn = isObject || (isVariable && expandable);
        const highlight = inWorkspace || parentInWs;
        return `
          <div class="tree-row"
            data-tree-level="${level}" style="--tree-indent: ${level * 20}px; padding-left: ${8 + level * 20}px">
            ${
              expandable
                ? `<button class="tree-toggle" type="button" data-config-toggle="${escapeHtml(item.node_id)}"
                    aria-expanded="${expanded ? "true" : "false"}"
                    aria-label="${expanded ? "Свернуть" : "Раскрыть"} ${escapeHtml(rawLabel)}"><span class="tree-chevron" aria-hidden="true"></span></button>`
                : `<span class="tree-spacer"></span>`
            }
            <div class="tree-content config-tree-content ${isVariable ? "tree-node-variable" : "tree-content-static"} ${highlight ? "in-workspace" : ""}"
              data-config-node='${nodeJson}'
              ${isVariable ? `tabindex="0" ${showAllBtn ? "" : `role="button"`} aria-label="Добавить ноду ${escapeHtml(rawLabel)} в рабочую область"` : ""}
              ${isVariable ? `draggable="true" data-drag-node='${nodeJson}'` : ""}>
              <div class="tree-title">
                <span class="config-tree-node-icon is-${nodeKind}">
                  ${bootstrapIcon(nodeKind)}
                </span>
                <span class="tree-name">${label}</span>
                <span class="tree-class">${escapeHtml(item.node_class || "")}</span>
                ${highlight ? `<span class="ws-dot" title="В рабочей области"></span>` : ""}
                ${mapped ? `<span class="inline-badge">в работе</span>` : ""}
                ${showAllBtn ? `<button class="group-subscribe-btn" type="button" data-group-subscribe='${nodeJson}' title="Добавить все дочерние Variable-ноды">
                  ${bootstrapIcon("chevron-right", "group-subscribe-icon")}
                  <span>Добавить ветку</span>
                </button>` : ""}
              </div>
              <div class="tree-node-id">${escapeHtml(item.node_id)}</div>
            </div>
          </div>
          ${expandable && expanded ? renderBranch(item.node_id, level + 1) : ""}
        `;
      })
      .join("");
  };

  const emptyMsg = treeFilter && visibleIds?.size === 0
    ? `<div class="tree-empty">Ничего не найдено по «${escapeHtml(treeFilter)}».</div>`
    : "";
  root.innerHTML = emptyMsg || `<div class="tree-list">${renderBranch("__root__", 0)}</div>`;

  for (const button of root.querySelectorAll("[data-config-toggle]")) {
    button.addEventListener("click", async (event) => {
      const nodeId = event.currentTarget.dataset.configToggle;
      if (state.configBrowseExpanded.has(nodeId)) {
        state.configBrowseExpanded.delete(nodeId);
        renderConfigBrowseTree();
        return;
      }
      const alreadyLoaded =
        state.configBrowseLoadedNodes.has(nodeId) ||
        state.configBrowseItems.some((i) => i.parent_node_id === nodeId);
      if (!alreadyLoaded) {
        const btn = event.currentTarget;
        btn.textContent = "…";
        btn.disabled = true;
        try {
          await loadConfigNodeChildren(nodeId);
        } catch (e) {
          btn.textContent = "+";
          btn.disabled = false;
          setConfigStatus(`Не удалось загрузить узлы: ${e.message}`, "error");
          return;
        }
      }
      state.configBrowseExpanded.add(nodeId);
      renderConfigBrowseTree();
    });
  }

  for (const element of root.querySelectorAll("[data-config-node]")) {
    const activateNode = (event) => {
      if (event.target.closest("[data-group-subscribe]")) return;
      const clickedNode = JSON.parse(element.dataset.configNode);
      if (clickedNode.node_class !== "Variable") return;
      const endpointId = clickedNode.endpoint_id;
      const alreadyAdded = state.draftNodes.some((n) => n.endpoint_id === endpointId && n.node_id === clickedNode.node_id);
      if (!alreadyAdded) {
        addBrowseNodeToDraft(clickedNode);
        renderMappings();
        renderConfigBrowseTree();
        setConfigStatus("Нода добавлена в рабочую область. Назначьте параметр и сохраните конфигурацию.", "warn");
      } else {
        setConfigStatus("Нода уже в рабочей области.", "info");
      }
    };
    element.addEventListener("click", activateNode);
    element.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      if (event.target.closest("[data-group-subscribe]")) return;
      event.preventDefault();
      activateNode(event);
    });
  }

  for (const btn of root.querySelectorAll("[data-group-subscribe]")) {
    btn.addEventListener("click", async (event) => {
      event.stopPropagation();
      const nodeData = JSON.parse(btn.dataset.groupSubscribe);
      await groupSubscribeObject(nodeData, btn);
    });
  }

  for (const el of root.querySelectorAll("[data-drag-node]")) {
    el.addEventListener("dragstart", (event) => {
      event.dataTransfer.effectAllowed = "copy";
      event.dataTransfer.setData(OPC_NODE_DRAG_TYPE, el.dataset.dragNode);
    });
  }
}


function addBrowseNodeToDraft(browseNode, param, groupData, options = {}) {
  const endpointId = browseNode.endpoint_id;
  const nodeId = makeNodeConfigId(endpointId, browseNode.node_id, "node");
  state.draftNodes.push({
    id: nodeId,
    endpoint_id: endpointId,
    node_id: browseNode.node_id,
    browse_name: browseNode.browse_name || null,
    display_name: browseNode.display_name || null,
    dict_param_id: param?.id || null,
    parameter_code: param?.name || null,
    parameter_name: param?.description || param?.name || null,
    enabled: true,
    acquisition_mode: "subscription",
    read_enabled: true,
    write_enabled: false,
    sampling_interval_ms: 1000,
    polling_interval_seconds: 5,
    expected_type: param ? mapDatatypeToExpectedType(param.datatype_name) : null,
    value_shape: "scalar",
    unit: param?.unit_symbol || param?.unit_name || null,
    group_id: groupData?.group_id || null,
    group_path: groupData?.group_path || [],
    group_display_name: groupData?.group_display_name || null,
    value_transform: { scale_factor: 1, offset: 0, target_unit: param?.unit_symbol || param?.unit_name || null },
    input_control: { stale_after_seconds: 30, suppress_duplicates: false },
    metadata: {
      opcua_browse_name: browseNode.browse_name || null,
      opcua_display_name: browseNode.display_name || null,
      opcua_data_type: browseNode.data_type || null,
      opcua_browse_path: browsePathForNode(browseNode),
      opcua_group_nodes: Array.isArray(groupData?.group_nodes)
        ? groupData.group_nodes.map((groupNode) => ({ ...groupNode }))
        : [],
    },
    tags: [],
  });
  if (!options.deferUpdate) updatePendingConfigChanges();
}

function addGroupFromItems(parentNodeId, groupPath, groupData, allItems, endpointId, context = null) {
  const ctx = context || {
    byParent: buildByParent(allItems),
    dictByName: buildDictByName(),
    existingNodeKeys: new Set(state.draftNodes.map((n) => nodeKey(n.endpoint_id, n.node_id))),
  };
  const children = ctx.byParent.get(parentNodeId) || [];
  for (const child of children) {
    if (child.node_class === "Variable") {
      // Add the variable itself to the current group
      const key = nodeKey(endpointId, child.node_id);
      if (!ctx.existingNodeKeys.has(key)) {
        const param = ctx.dictByName.get(child.browse_name) || ctx.dictByName.get(child.display_name) || null;
        addBrowseNodeToDraft({ ...child, endpoint_id: endpointId }, param, groupData, { deferUpdate: true });
        ctx.existingNodeKeys.add(key);
      }
      // If this variable has sub-variables (e.g. BuildInfo → ProductUri, SoftwareVersion…),
      // recurse into them as a child sub-group, preserving hierarchy
      const subChildren = ctx.byParent.get(child.node_id) || [];
      if (subChildren.length > 0) {
        const subName = child.browse_name || child.display_name || child.node_id;
        const subPath = [...groupPath, subName];
        const subGroupNodes = [
          ...(groupData?.group_nodes || []),
          {
            name: subName,
            node_id: child.node_id,
            node_class: child.node_class,
            browse_name: child.browse_name || null,
            display_name: child.display_name || null,
          },
        ];
        addGroupFromItems(child.node_id, subPath, {
          group_id: makeNodeConfigId(endpointId, subPath.join("/")),
          group_path: subPath,
          group_display_name: child.display_name || child.browse_name || child.node_id,
          group_nodes: subGroupNodes,
        }, allItems, endpointId, ctx);
      }
    } else if (child.node_class === "Object") {
      const subName = child.browse_name || child.display_name || child.node_id;
      const subPath = [...groupPath, subName];
      const subGroupNodes = [
        ...(groupData?.group_nodes || []),
        {
          name: subName,
          node_id: child.node_id,
          node_class: child.node_class,
          browse_name: child.browse_name || null,
          display_name: child.display_name || null,
        },
      ];
      addGroupFromItems(child.node_id, subPath, {
        group_id: makeNodeConfigId(endpointId, subPath.join("/")),
        group_path: subPath,
        group_display_name: child.display_name || child.browse_name || child.node_id,
        group_nodes: subGroupNodes,
      }, allItems, endpointId, ctx);
    }
  }
}

async function groupSubscribeObject(parentNode, button) {
  const endpointId = parentNode.endpoint_id || document.getElementById("configEndpoint").value;
  setBusyState(button, true);
  try {
    const params = new URLSearchParams({
      endpoint_id: endpointId,
      node_id: parentNode.node_id,
      max_depth: GROUP_SUBSCRIBE_MAX_DEPTH,
      include_variables: "true",
      include_objects: "true",
    });
    const response = await fetchJson(`/api/browse?${params.toString()}`);
    const allItems = Array.isArray(response.items) ? response.items : [];
    if (!allItems.some((item) => item.node_class === "Variable")) {
      setConfigStatus(`Нет Variable-нод в «${parentNode.display_name || parentNode.node_id}».`, "warn");
      return;
    }
    const groupPath = [parentNode.browse_name || parentNode.display_name || parentNode.node_id];
    const groupId = makeNodeConfigId(endpointId, groupPath.join("/"));
    const groupData = {
      group_id: groupId,
      group_path: groupPath,
      group_display_name: parentNode.display_name || parentNode.browse_name || parentNode.node_id,
      group_nodes: [{
        name: groupPath[0],
        node_id: parentNode.node_id,
        node_class: parentNode.node_class,
        browse_name: parentNode.browse_name || null,
        display_name: parentNode.display_name || null,
      }],
    };
    const beforeCount = state.draftNodes.length;
    addGroupFromItems(parentNode.node_id, groupPath, groupData, allItems, endpointId);
    updatePendingConfigChanges();
    const addedCount = state.draftNodes.length - beforeCount;
    const totalVars = allItems.filter((item) => item.node_class === "Variable").length;
    const skipped = totalVars - addedCount;
    const unbound = state.draftNodes.slice(beforeCount).filter((n) => !n.parameter_code).length;
    const parts = [];
    if (addedCount - unbound > 0) parts.push(`${addedCount - unbound} привязано к параметру`);
    if (unbound > 0) parts.push(`${unbound} без параметра — назначьте вручную`);
    if (skipped > 0) parts.push(`${skipped} уже в рабочей области`);
    setConfigStatus(
      `«${groupData.group_display_name}»: ${parts.length ? parts.join(", ") : "нет новых нод"} (глубина ${GROUP_SUBSCRIBE_MAX_DEPTH}).`,
      unbound > 0 ? "warn" : "success",
    );
    renderMappings();
    renderConfigBrowseTree();
    renderDictionary();
  } catch (error) {
    setConfigStatus(`Ошибка: ${error.message}`, "error");
  } finally {
    setBusyState(button, false);
  }
}

function renderDictionary() {
  const root = document.getElementById("dictionaryList");
  const queryTokens = state.dictFilter.trim().toLocaleLowerCase("ru-RU").split(/\s+/).filter(Boolean);
  const visible = queryTokens.length
    ? state.dictionary.filter((param) => {
        const haystack = [
          param.name,
          param.description,
          param.datatype_name,
          param.unit_name,
          param.unit_symbol,
        ].join(" ").toLocaleLowerCase("ru-RU");
        return queryTokens.every((token) => haystack.includes(token));
      })
    : state.dictionary;

  const shown = visible.slice(0, state.dictVisibleLimit);
  setText("dictMeta", `${shown.length} показано · ${visible.length} найдено · ${state.dictionary.length} всего`);
  if (!visible.length) {
    root.innerHTML = `<div class="tree-empty">Параметры не найдены.</div>`;
    return;
  }

  root.innerHTML = shown
    .map((param) => {
      const mapped = state.draftNodes.some((node) => node.dict_param_id === param.id);
      const selected = state.selectedDictParamId === param.id;
      return `
        <button type="button" class="dict-card ${selected ? "selected" : ""}" data-dict-id="${escapeHtml(param.id)}"
          aria-pressed="${selected ? "true" : "false"}">
          <div class="dict-name">${escapeHtml(param.name)}</div>
          <div class="dict-description">${escapeHtml(param.description || "-")}</div>
          <div class="dict-meta">
            <span>${escapeHtml(param.datatype_name || "-")}</span>
            <span>${escapeHtml(param.unit_symbol || param.unit_name || "без единиц")}</span>
            ${mapped ? `<span class="badge badge-ok">назначен</span>` : ""}
          </div>
        </button>
      `;
    })
    .join("") + (shown.length < visible.length
      ? `<button type="button" class="dict-load-more" data-dict-load-more>
           Показать ещё ${Math.min(DICTIONARY_PAGE_SIZE, visible.length - shown.length)}
         </button>`
      : "");

  for (const card of root.querySelectorAll("[data-dict-id]")) {
    card.addEventListener("click", () => {
      if (state.pendingAssignGroupId) {
        assignParamToGroup(state.pendingAssignGroupId, card.dataset.dictId, state.pendingAssignGroupPath);
        return;
      }
      if (state.pendingAssignNodeId) {
        assignParamToDraftNode(state.pendingAssignNodeId, card.dataset.dictId);
        return;
      }
      state.selectedDictParamId = state.selectedDictParamId === card.dataset.dictId ? null : card.dataset.dictId;
      renderDictionary();
    });
  }

  root.querySelector("[data-dict-load-more]")?.addEventListener("click", () => {
    const scrollTop = root.scrollTop;
    state.dictVisibleLimit += DICTIONARY_PAGE_SIZE;
    renderDictionary();
    root.scrollTop = scrollTop;
  });
}

function renderSelectionBridge() {
  // selection-bridge UI is currently commented out in HTML
}

function openDictModal(title = "Справочник параметров") {
  state.modalReturnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const titleEl = document.querySelector(".dict-modal-title");
  if (titleEl) titleEl.textContent = title;
  const overlay = document.getElementById("dictModalOverlay");
  if (overlay) overlay.classList.remove("hidden");
  document.body.classList.add("modal-open");
  state.dictFilter = "";
  state.dictVisibleLimit = DICTIONARY_PAGE_SIZE;
  const filter = document.getElementById("dictFilter");
  if (filter) filter.value = "";
  renderDictionary();
  filter?.focus();
}

function closeDictModal() {
  const overlay = document.getElementById("dictModalOverlay");
  if (overlay) overlay.classList.add("hidden");
  document.body.classList.remove("modal-open");
  const returnFocus = state.modalReturnFocus;
  state.modalReturnFocus = null;
  if (returnFocus?.isConnected) returnFocus.focus();
}

function cancelDictModal() {
  state.pendingAssignNodeId = null;
  state.pendingAssignGroupId = null;
  state.pendingAssignGroupPath = null;
  closeDictModal();
  renderMappings();
}

function assignParamToDraftNode(nodeId, dictParamId) {
  const param = state.dictionary.find((item) => item.id === dictParamId);
  const idx = state.draftNodes.findIndex((n) => n.id === nodeId);
  if (!param || idx < 0) return;
  const node = state.draftNodes[idx];
  const previousParam = node.parameter_code || "";
  state.draftNodes[idx] = {
    ...node,
    dict_param_id: param.id,
    parameter_code: param.name,
    parameter_name: param.description || param.name,
    expected_type: mapDatatypeToExpectedType(param.datatype_name),
    unit: param.unit_symbol || param.unit_name || null,
    value_transform: {
      ...(node.value_transform || {}),
      target_unit: param.unit_symbol || param.unit_name || null,
    },
    metadata: {
      ...(node.metadata || {}),
      dict_param_name: param.name,
      dict_param_description: param.description || null,
    },
  };
  state.pendingAssignNodeId = null;
  closeDictModal();
  updatePendingConfigChanges();
  renderMappings();
  renderConfigBrowseTree();
  const statusText = previousParam && previousParam !== param.name
    ? `Параметр переназначен: «${previousParam}» → «${param.name}». Не забудьте сохранить.`
    : `Параметр «${param.name}» назначен. Не забудьте сохранить.`;
  setConfigStatus(statusText, "warn");
}

function normalizedGroupPath(value) {
  return Array.isArray(value) ? value.map((segment) => String(segment)) : [];
}

function normalizedBrowseSegment(value) {
  return String(value || "").replace(/^\d+:/, "");
}

function browseItemMatchesGroupSegment(item, segment) {
  const target = String(segment || "");
  const normalizedTarget = normalizedBrowseSegment(target);
  return [item?.browse_name, item?.display_name]
    .filter(Boolean)
    .some((value) => String(value) === target || normalizedBrowseSegment(value) === normalizedTarget);
}

function resolveBrowseGroupNode(groupPath, endpointId) {
  const path = normalizedGroupPath(groupPath);
  const selectedEndpoint = document.getElementById("configEndpoint")?.value || "";
  if (!path.length || !endpointId || selectedEndpoint !== endpointId) return null;

  let candidates = state.configBrowseItems.filter((item) => browseItemMatchesGroupSegment(item, path[0]));
  for (let index = 1; index < path.length && candidates.length > 0; index += 1) {
    const parentIds = new Set(candidates.map((item) => item.node_id));
    candidates = state.configBrowseItems.filter(
      (item) => parentIds.has(item.parent_node_id) && browseItemMatchesGroupSegment(item, path[index]),
    );
  }

  // A duplicated browse path is not safe to expose as a copyable NodeId.
  if (candidates.length !== 1) return null;
  const item = candidates[0];
  return {
    name: path.at(-1),
    node_id: item.node_id,
    node_class: item.node_class || null,
    browse_name: item.browse_name || null,
    display_name: item.display_name || null,
  };
}

function storedGroupNodeIdentity(node, groupIndex) {
  const identity = node?.metadata?.opcua_group_nodes?.[groupIndex];
  if (!identity?.node_id) return null;
  return {
    name: identity.name || null,
    node_id: identity.node_id,
    node_class: identity.node_class || null,
    browse_name: identity.browse_name || null,
    display_name: identity.display_name || null,
  };
}

function withResolvedGroupNodeMetadata(node) {
  const groupPath = normalizedGroupPath(node?.group_path);
  if (!groupPath.length) return node;
  const identities = groupPath.map((_, index) => (
    storedGroupNodeIdentity(node, index)
      || resolveBrowseGroupNode(groupPath.slice(0, index + 1), node.endpoint_id)
  ));
  if (!identities.some(Boolean)) return node;
  return {
    ...node,
    metadata: {
      ...(node.metadata || {}),
      opcua_group_nodes: identities,
    },
  };
}

function withResolvedBrowsePathMetadata(node) {
  if (node?.group_id || !node?.node_id) return node;
  const storedPath = Array.isArray(node.metadata?.opcua_browse_path)
    ? node.metadata.opcua_browse_path.filter(Boolean).map(String)
    : [];
  if (storedPath.length > 1) return node;
  const resolvedPath = browsePathForNode(node);
  if (resolvedPath.length <= 1) return node;
  return {
    ...node,
    metadata: {
      ...(node.metadata || {}),
      opcua_browse_path: resolvedPath,
    },
  };
}

function groupPathStartsWith(path, parentPath) {
  const normalizedPath = normalizedGroupPath(path);
  const normalizedParent = normalizedGroupPath(parentPath);
  return normalizedParent.length > 0 && normalizedPath.length >= normalizedParent.length
    && normalizedParent.every((segment, index) => segment === normalizedPath[index]);
}

function draftGroupMembers(groupId, groupPath, endpointId) {
  const normalizedPath = normalizedGroupPath(groupPath);
  return state.draftNodes.filter((node) => {
    if (endpointId && node.endpoint_id !== endpointId) return false;
    if (normalizedPath.length > 0) return groupPathStartsWith(node.group_path, normalizedPath);
    return node.group_id === groupId;
  });
}

function assignParamToGroup(groupId, dictParamId, requestedGroupPath = null) {
  const param = state.dictionary.find((item) => item.id === dictParamId);
  if (!param) return;
  const currentEndpoint = document.getElementById("configEndpoint")?.value || "";
  const baseNode = state.draftNodes.find((n) => n.group_id === groupId);
  const basePath = normalizedGroupPath(requestedGroupPath).length > 0
    ? normalizedGroupPath(requestedGroupPath)
    : normalizedGroupPath(baseNode?.group_path);
  const groupNodes = draftGroupMembers(groupId, basePath, currentEndpoint);
  if (!groupNodes.length) return;
  const reassignedCount = groupNodes.filter((n) => n.parameter_code && n.parameter_code !== param.name).length;
  for (const node of groupNodes) {
    const idx = state.draftNodes.indexOf(node);
    if (idx < 0) continue;
    state.draftNodes[idx] = {
      ...node,
      dict_param_id: param.id,
      parameter_code: param.name,
      parameter_name: param.description || param.name,
      expected_type: mapDatatypeToExpectedType(param.datatype_name),
      unit: param.unit_symbol || param.unit_name || null,
      value_transform: { ...(node.value_transform || {}), target_unit: param.unit_symbol || param.unit_name || null },
      metadata: { ...(node.metadata || {}), dict_param_name: param.name, dict_param_description: param.description || null },
    };
  }
  state.pendingAssignGroupId = null;
  state.pendingAssignGroupPath = null;
  closeDictModal();
  updatePendingConfigChanges();
  renderMappings();
  renderConfigBrowseTree();
  const groupName = basePath.at(-1) || baseNode?.group_display_name || groupId;
  const actionText = reassignedCount > 0 ? "переназначен" : "назначен";
  setConfigStatus(`Параметр «${param.name}» ${actionText} ${groupNodes.length} нодам группы «${groupName}». Не забудьте сохранить.`, "warn");
}

function removeDraftGroup(groupId, groupPath, endpointId, groupName) {
  const members = draftGroupMembers(groupId, groupPath, endpointId);
  if (!members.length) return;
  const memberKeys = new Set(members.map((node) => nodeKey(node.endpoint_id, node.node_id)));
  const memberIds = new Set(members.map((node) => node.id));
  const removedEntries = state.draftNodes
    .map((node, index) => ({ node, index }))
    .filter((entry) => memberKeys.has(nodeKey(entry.node.endpoint_id, entry.node.node_id)));

  state.draftNodes = state.draftNodes.filter((node) => !memberKeys.has(nodeKey(node.endpoint_id, node.node_id)));
  if (state.pendingAssignNodeId && memberIds.has(state.pendingAssignNodeId)) state.pendingAssignNodeId = null;
  if (state.pendingAssignGroupId === groupId) {
    state.pendingAssignGroupId = null;
    state.pendingAssignGroupPath = null;
  }
  state.workspaceCollapsed.delete(groupId);
  state.workspaceVisibleLimits.delete(groupId);
  updatePendingConfigChanges();
  renderConfigBrowseTree();
  renderMappings();

  setConfigStatus(`Группа «${groupName}» и ${removedEntries.length} нод удалены из черновика.`, "warn", {
    label: "Отменить",
    onClick: () => {
      const existingKeys = new Set(state.draftNodes.map((node) => nodeKey(node.endpoint_id, node.node_id)));
      for (const { node, index } of removedEntries) {
        const key = nodeKey(node.endpoint_id, node.node_id);
        if (existingKeys.has(key)) continue;
        state.draftNodes.splice(Math.min(index, state.draftNodes.length), 0, node);
        existingKeys.add(key);
      }
      updatePendingConfigChanges();
      renderConfigBrowseTree();
      renderMappings();
      setConfigStatus(`Группа «${groupName}» восстановлена в черновике.`, "success");
    },
  });
}

function assignSelectedPair() {
  if (!state.selectedBrowseNode || !state.selectedDictParamId) return;
  assignNodeToParam(state.selectedBrowseNode, state.selectedDictParamId);
}

function assignNodeToParam(browseNode, dictParamId) {
  const param = state.dictionary.find((item) => item.id === dictParamId);
  if (!param || !browseNode?.node_id) return;
  const endpointId = browseNode.endpoint_id || document.getElementById("configEndpoint").value;
  const existingIndex = state.draftNodes.findIndex(
    (node) => node.dict_param_id === param.id || (node.endpoint_id === endpointId && node.node_id === browseNode.node_id),
  );
  const previous = existingIndex >= 0 ? state.draftNodes[existingIndex] : null;
  const nodeConfig = {
    ...(previous || {}),
    id: previous?.id || makeNodeConfigId(endpointId, browseNode.node_id, "node"),
    endpoint_id: endpointId,
    node_id: browseNode.node_id,
    namespace_uri: previous?.namespace_uri || null,
    browse_name: browseNode.browse_name || previous?.browse_name || null,
    display_name: browseNode.display_name || previous?.display_name || null,
    acquisition_mode: previous?.acquisition_mode || "subscription",
    read_enabled: previous?.read_enabled ?? true,
    write_enabled: previous?.write_enabled ?? false,
    sampling_interval_ms: previous?.sampling_interval_ms || 1000,
    polling_interval_seconds: previous?.polling_interval_seconds || 5,
    parameter_code: param.name,
    parameter_name: param.description || param.name,
    dict_param_id: param.id,
    type_by_dict: param.datatype_name || null,
    unit_by_dict: param.unit_symbol || param.unit_name || null,
    expected_type: mapDatatypeToExpectedType(param.datatype_name),
    value_shape: "scalar",
    unit: param.unit_symbol || param.unit_name || null,
    value_transform: {
      scale_factor: previous?.value_transform?.scale_factor ?? 1,
      offset: previous?.value_transform?.offset ?? 0,
      target_unit: param.unit_symbol || param.unit_name || null,
    },
    input_control: previous?.input_control || {
      stale_after_seconds: 30,
      suppress_duplicates: false,
    },
    metadata: {
      ...(previous?.metadata || {}),
      dict_param_name: param.name,
      dict_param_description: param.description || null,
      opcua_browse_name: browseNode.browse_name || null,
      opcua_display_name: browseNode.display_name || null,
      opcua_data_type: browseNode.data_type || null,
      opcua_browse_path: browsePathForNode(browseNode),
    },
    tags: previous?.tags || [],
  };
  if (existingIndex >= 0) {
    state.draftNodes[existingIndex] = nodeConfig;
  } else {
    state.draftNodes.push(nodeConfig);
  }
  updatePendingConfigChanges();
  state.selectedBrowseNode = { ...browseNode, endpoint_id: endpointId };
  state.selectedDictParamId = param.id;
  setConfigStatus(buildDraftStatusMessage(), "warn");
  renderSelectionBridge();
  renderConfigBrowseTree();
  renderDictionary();
  renderMappings();
}

function renderMappings() {
  const root = document.getElementById("mappingList");
  const tableWrap = root.closest(".mapping-table-wrap");
  const previousScrollTop = tableWrap?.scrollTop || 0;
  const previousScrollLeft = tableWrap?.scrollLeft || 0;
  const filterEl = document.getElementById("mappingFilter");
  const filterText = filterEl ? filterEl.value.toLowerCase() : "";
  const dictCodes = new Set(state.dictionary.map((p) => p.name));
  const savedById = new Map(state.configNodes.map((item) => [item.id, item]));
  const currentEndpoint = document.getElementById("configEndpoint")?.value || "";
  const endpointNodes = currentEndpoint
    ? state.draftNodes.filter((n) => n.endpoint_id === currentEndpoint)
    : state.draftNodes;

  setText("mappingMeta", `${endpointNodes.length} нод`);

  if (!endpointNodes.length) {
    root.innerHTML = `<tr><td colspan="6" class="ua-table-empty">Нет нод. Кликните или перетащите Variable-ноду из дерева${currentEndpoint ? ` (${currentEndpoint})` : ""}.</td></tr>`;
    return;
  }

  const filtered = filterText
    ? endpointNodes.filter(
        (n) =>
          (n.parameter_code || "").toLowerCase().includes(filterText) ||
          (n.node_id || "").toLowerCase().includes(filterText) ||
          (n.browse_name || "").toLowerCase().includes(filterText) ||
          (n.group_display_name || "").toLowerCase().includes(filterText),
      )
    : endpointNodes;

  const makeNodeRow = (node, rowIdx) => {
    const savedNode = savedById.get(node.id);
    const isNew = !savedNode;
    const isChanged = savedNode && JSON.stringify(node) !== JSON.stringify(savedNode);
    const hasParam = node.parameter_code && dictCodes.has(node.parameter_code);
    const hasMissingParam = node.parameter_code && !hasParam;
    const isPending = state.pendingAssignNodeId === node.id;
    const rowClass = isPending ? "pending-assign" : isNew ? "status-new" : isChanged ? "status-changed" : "status-saved";
    const paramTitle = node.parameter_code
      ? escapeHtml(node.parameter_code + (node.parameter_name && node.parameter_name !== node.parameter_code ? `\n${node.parameter_name}` : ""))
      : "";
    const paramCell = hasParam || hasMissingParam
      ? `<div class="ua-param-wrap ${hasMissingParam ? "param-missing" : ""}">
           <div class="ua-param-text">
             <span class="ua-param-code">${escapeHtml(node.parameter_code)}</span>
             ${hasMissingParam
               ? `<span class="ua-param-name">нет в справочнике</span>`
               : node.parameter_name && node.parameter_name !== node.parameter_code
                 ? `<span class="ua-param-name">${escapeHtml(node.parameter_name)}</span>`
                 : ""}
           </div>
           <button class="ua-reassign-btn" type="button" data-assign-node="${escapeHtml(node.id)}" data-assign-mode="reassign">
             ${hasMissingParam ? "Заменить" : "Сменить"}
           </button>
         </div>`
      : isPending
        ? `<span class="assign-hint">выберите в справочнике</span>`
        : `<button class="ua-assign-btn" type="button" data-assign-node="${escapeHtml(node.id)}">Назначить</button>`;
    const storedBrowsePath = Array.isArray(node.metadata?.opcua_browse_path)
      ? node.metadata.opcua_browse_path.filter(Boolean).map(String)
      : [];
    const standaloneBrowsePath = node.group_id
      ? []
      : (storedBrowsePath.length > 1 ? storedBrowsePath : browsePathForNode(node));
    const nodeContext = standaloneBrowsePath.length > 1
      ? `<span class="ua-node-breadcrumb" title="${escapeHtml(standaloneBrowsePath.join(" / "))}">${standaloneBrowsePath.map(escapeHtml).join('<span class="ua-node-path-separator">/</span>')}</span>`
      : node.browse_name
        ? `<span class="ua-browse-name">${escapeHtml(node.browse_name)}</span>`
        : "";
    return `
      <tr class="ua-row ${rowClass}" data-node-id="${escapeHtml(node.id)}">
        <td class="col-num">${rowIdx}</td>
        <td class="col-nodeid" title="${escapeHtml(node.node_id + (node.browse_name ? '\n' + node.browse_name : ''))}">
          <div class="ua-node-cell">
            <div class="ua-node-id-line">
              <code>${escapeHtml(node.node_id)}</code>
              <button class="ua-copy-btn" type="button" data-copy-text="${escapeHtml(node.node_id)}"
                title="Скопировать Node ID" aria-label="Скопировать Node ID"></button>
            </div>
            ${nodeContext}
          </div>
        </td>
        <td class="col-param-cell" ${paramTitle ? `title="${paramTitle}"` : ""}>${paramCell}</td>
        <td class="col-mode">${escapeHtml(node.acquisition_mode || "-")}</td>
        <td class="col-type">${escapeHtml(node.expected_type || "-")}</td>
        <td class="col-del"><button class="ua-del-btn" type="button" data-remove-node="${escapeHtml(node.id)}"
          title="Удалить ноду из черновика" aria-label="Удалить ${escapeHtml(node.display_name || node.browse_name || node.node_id)}">×</button></td>
      </tr>`;
  };

  // Materialize every group_path prefix. A node stored in ["DB", "Array"]
  // therefore renders as DB -> Array instead of one ambiguous "DB / Array" row.
  const groupsByPath = new Map();
  const ungrouped = [];
  for (const node of filtered) {
    const groupPath = normalizedGroupPath(node.group_path);
    if (node.group_id && groupPath.length > 0) {
      for (let length = 1; length <= groupPath.length; length += 1) {
        const path = groupPath.slice(0, length);
        const pathKey = JSON.stringify(path);
        const isLeafGroup = length === groupPath.length;
        const nodeIdentity = storedGroupNodeIdentity(node, length - 1)
          || resolveBrowseGroupNode(path, node.endpoint_id);
        if (!groupsByPath.has(pathKey)) {
          groupsByPath.set(pathKey, {
            group_id: isLeafGroup ? node.group_id : makeNodeConfigId(currentEndpoint, path.join("/"), "group"),
            group_path: path,
            display_name: path.at(-1),
            node_identity: nodeIdentity,
            nodes: [],
            children: [],
            is_virtual: !isLeafGroup,
          });
        } else if (isLeafGroup) {
          const existingGroup = groupsByPath.get(pathKey);
          existingGroup.group_id = node.group_id;
          existingGroup.is_virtual = false;
          if (!existingGroup.node_identity && nodeIdentity) existingGroup.node_identity = nodeIdentity;
        } else if (nodeIdentity && !groupsByPath.get(pathKey).node_identity) {
          groupsByPath.get(pathKey).node_identity = nodeIdentity;
        }
      }
      groupsByPath.get(JSON.stringify(groupPath)).nodes.push(node);
    } else if (node.group_id) {
      const path = [node.group_display_name || node.group_id];
      const pathKey = JSON.stringify(path);
      if (!groupsByPath.has(pathKey)) {
        groupsByPath.set(pathKey, {
          group_id: node.group_id,
          group_path: [],
          display_name: node.group_display_name || node.group_id,
          node_identity: null,
          nodes: [],
          children: [],
          is_virtual: false,
        });
      }
      groupsByPath.get(pathKey).nodes.push(node);
    } else {
      ungrouped.push(node);
    }
  }

  // O(G) hierarchy construction by path key; the previous implementation did
  // repeated find/some scans and became quadratic for deeply grouped configs.
  const allGroups = [...groupsByPath.values()].sort((a, b) => a.group_path.length - b.group_path.length);
  const topGroups = [];
  for (const group of allGroups) {
    const parent = group.group_path.length > 1
      ? groupsByPath.get(JSON.stringify(group.group_path.slice(0, -1)))
      : null;
    if (parent) parent.children.push(group);
    else topGroups.push(group);
  }
  const groupsById = new Map(allGroups.map((group) => [group.group_id, group]));

  function countNodes(group) {
    return group.nodes.length + group.children.reduce((s, c) => s + countNodes(c), 0);
  }
  function hasUnbound(group) {
    return (
      group.nodes.some((n) => !n.parameter_code || !dictCodes.has(n.parameter_code)) ||
      group.children.some((c) => hasUnbound(c))
    );
  }

  let html = "";

  function renderGroup(group, depth) {
    const isCollapsed = state.workspaceCollapsed.has(group.group_id);
    const totalCount = countNodes(group);
    const nestedCount = totalCount - group.nodes.length;
    const unbound = hasUnbound(group);
    const assignGroupLabel = unbound ? "Назначить всем" : "Переназначить";
    const groupPath = normalizedGroupPath(group.group_path);
    const groupName = groupPath.at(-1) || group.display_name || group.group_id;
    const parentPath = groupPath.slice(0, -1);
    const pathLabel = groupPath.length > 0 ? groupPath.join(" / ") : groupName;
    const breadcrumb = parentPath.length > 0
      ? parentPath.map((segment) => `<span>${escapeHtml(segment)}</span>`).join('<span class="ua-group-path-separator">/</span>')
      : '<span class="ua-group-root-label">Корневая группа</span>';
    const groupPathJson = escapeHtml(JSON.stringify(groupPath));
    const groupNodeId = group.node_identity?.node_id || null;
    const groupNodeClass = group.node_identity?.node_class || null;
    html += `
      <tr class="ua-group-header${depth > 0 ? " ua-group-sub" : ""}"
        style="--group-indent: ${Math.min(depth, 6) * 12}px">
        <td class="ua-group-toggle-cell">
          <button class="ua-group-toggle" type="button" data-toggle-group="${escapeHtml(group.group_id)}"
            aria-expanded="${isCollapsed ? "false" : "true"}" aria-label="${isCollapsed ? "Раскрыть" : "Свернуть"} группу ${escapeHtml(pathLabel)}">
            <span class="ua-chevron" aria-hidden="true"></span>
          </button>
        </td>
        <td colspan="5" class="ua-group-cell">
          <div class="ua-group-inner">
            <span class="ua-group-icon">${bootstrapIcon("group")}</span>
            <div class="ua-group-identity">
              <div class="ua-group-breadcrumb" aria-label="Родительский путь: ${escapeHtml(parentPath.join(" / ") || "корень")}">${breadcrumb}</div>
              <div class="ua-group-title-row">
                <span class="ua-group-name-text" title="${escapeHtml(pathLabel)}">${escapeHtml(groupName)}</span>
                ${group.is_virtual ? '<span class="ua-group-virtual-badge">раздел</span>' : ""}
              </div>
              ${groupNodeId ? `
                <div class="ua-group-node-meta" title="${escapeHtml(groupNodeId)}">
                  <code>${escapeHtml(groupNodeId)}</code>
                  ${groupNodeClass ? `<span class="ua-group-node-class">${escapeHtml(groupNodeClass)}</span>` : ""}
                  <button class="ua-copy-btn" type="button" data-copy-text="${escapeHtml(groupNodeId)}"
                    title="Скопировать Node ID родительской ноды" aria-label="Скопировать Node ID группы ${escapeHtml(groupName)}"></button>
                </div>` : ""}
            </div>
            <div class="ua-group-stats" aria-label="${totalCount} нод в группе">
              <span class="ua-group-count">${totalCount} нод</span>
              ${nestedCount > 0 ? `<span class="ua-group-nested-count">${nestedCount} во вложенных</span>` : ""}
            </div>
            <div class="ua-group-actions">
              <button class="ua-assign-group-btn ${unbound ? "has-unbound" : ""}" type="button"
                data-assign-group="${escapeHtml(group.group_id)}"
                data-group-path='${groupPathJson}'
                data-group-name="${escapeHtml(pathLabel)}"
                data-assign-mode="${unbound ? "assign" : "reassign"}">
                ${bootstrapIcon("diagram")}<span>${assignGroupLabel}</span>
              </button>
              <button class="ua-remove-group-btn" type="button"
                data-remove-group="${escapeHtml(group.group_id)}"
                data-group-path='${groupPathJson}'
                data-group-name="${escapeHtml(pathLabel)}"
                title="Удалить группу и все вложенные ноды">
                ${bootstrapIcon("trash")}<span>Удалить</span>
              </button>
            </div>
          </div>
        </td>
      </tr>`;
    if (!isCollapsed) {
      const visibleLimit = state.workspaceVisibleLimits.get(group.group_id) || WORKSPACE_PAGE_SIZE;
      const visibleNodes = group.nodes.slice(0, visibleLimit);
      const hiddenDirectNodes = group.nodes.length - visibleNodes.length;
      let localIdx = 0;
      for (const node of visibleNodes) {
        localIdx++;
        html += makeNodeRow(node, localIdx);
      }
      if (hiddenDirectNodes > 0) {
        html += `
          <tr class="ua-load-more-row">
            <td colspan="6">
              <button class="ua-load-more-btn" type="button" data-load-more-group="${escapeHtml(group.group_id)}">
                Показать ещё ${Math.min(WORKSPACE_PAGE_SIZE, hiddenDirectNodes)} из ${hiddenDirectNodes}
              </button>
            </td>
          </tr>`;
      }
      for (const child of group.children) {
        renderGroup(child, depth + 1);
      }
    }
  }

  for (const group of topGroups) {
    renderGroup(group, 0);
  }
  if (topGroups.length > 0 && ungrouped.length > 0) {
    html += `
      <tr class="ua-standalone-divider">
        <td colspan="6">
          <div class="ua-standalone-divider-inner">
            <span class="ua-standalone-divider-label">Отдельные ноды</span>
            <span class="ua-standalone-divider-count">${ungrouped.length}</span>
          </div>
        </td>
      </tr>`;
  }
  let ungroupedIdx = 0;
  const visibleUngrouped = ungrouped.slice(0, state.workspaceUngroupedLimit);
  for (const node of visibleUngrouped) {
    ungroupedIdx++;
    html += makeNodeRow(node, ungroupedIdx, 0);
  }
  const hiddenUngrouped = ungrouped.length - visibleUngrouped.length;
  if (hiddenUngrouped > 0) {
    html += `
      <tr class="ua-load-more-row">
        <td colspan="6">
          <button class="ua-load-more-btn" type="button" data-load-more-ungrouped>
            Показать ещё ${Math.min(WORKSPACE_PAGE_SIZE, hiddenUngrouped)} из ${hiddenUngrouped}
          </button>
        </td>
      </tr>`;
  }

  root.innerHTML = html;
  if (tableWrap) {
    tableWrap.scrollTop = previousScrollTop;
    tableWrap.scrollLeft = previousScrollLeft;
  }

  for (const button of root.querySelectorAll("[data-toggle-group]")) {
    button.addEventListener("click", () => {
      const gid = button.dataset.toggleGroup;
      if (state.workspaceCollapsed.has(gid)) {
        state.workspaceCollapsed.delete(gid);
      } else {
        state.workspaceCollapsed.add(gid);
      }
      renderMappings();
    });
  }

  for (const button of root.querySelectorAll("[data-remove-group]")) {
    button.addEventListener("click", () => {
      const groupId = button.dataset.removeGroup;
      const group = groupsById.get(groupId);
      const groupPath = group?.group_path || JSON.parse(button.dataset.groupPath || "[]");
      removeDraftGroup(
        groupId,
        groupPath,
        currentEndpoint,
        button.dataset.groupName || groupPath.join(" / ") || groupId,
      );
    });
  }

  for (const button of root.querySelectorAll("[data-load-more-group]")) {
    button.addEventListener("click", () => {
      const groupId = button.dataset.loadMoreGroup;
      const currentLimit = state.workspaceVisibleLimits.get(groupId) || WORKSPACE_PAGE_SIZE;
      state.workspaceVisibleLimits.set(groupId, currentLimit + WORKSPACE_PAGE_SIZE);
      renderMappings();
    });
  }

  for (const button of root.querySelectorAll("[data-load-more-ungrouped]")) {
    button.addEventListener("click", () => {
      state.workspaceUngroupedLimit += WORKSPACE_PAGE_SIZE;
      renderMappings();
    });
  }

  for (const button of root.querySelectorAll("[data-copy-text]")) {
    button.addEventListener("click", async () => {
      const copied = await copyTextToClipboard(button.dataset.copyText);
      setConfigStatus(copied ? "Node ID скопирован." : "Не удалось скопировать Node ID.", copied ? "success" : "error");
    });
  }

  for (const button of root.querySelectorAll("[data-remove-node]")) {
    button.addEventListener("click", () => {
      const removedIndex = state.draftNodes.findIndex((node) => node.id === button.dataset.removeNode);
      if (removedIndex < 0) return;
      if (state.pendingAssignNodeId === button.dataset.removeNode) state.pendingAssignNodeId = null;
      const [removedNode] = state.draftNodes.splice(removedIndex, 1);
      updatePendingConfigChanges();
      setConfigStatus(`Нода «${removedNode.display_name || removedNode.browse_name || removedNode.node_id}» удалена из черновика.`, "warn", {
        label: "Отменить",
        onClick: () => {
          const alreadyExists = state.draftNodes.some(
            (node) => node.endpoint_id === removedNode.endpoint_id && node.node_id === removedNode.node_id,
          );
          if (!alreadyExists) state.draftNodes.splice(Math.min(removedIndex, state.draftNodes.length), 0, removedNode);
          updatePendingConfigChanges();
          renderConfigBrowseTree();
          renderMappings();
          setConfigStatus("Удаление отменено.", "success");
        },
      });
      renderConfigBrowseTree();
      renderMappings();
    });
  }

  for (const button of root.querySelectorAll("[data-assign-node]")) {
    button.addEventListener("click", () => {
      state.pendingAssignNodeId = button.dataset.assignNode;
      state.pendingAssignGroupId = null;
      state.pendingAssignGroupPath = null;
      renderMappings();
      openDictModal(button.dataset.assignMode === "reassign" ? "Переназначить параметр" : "Назначить параметр");
    });
  }

  for (const button of root.querySelectorAll("[data-assign-group]")) {
    button.addEventListener("click", () => {
      state.pendingAssignGroupId = button.dataset.assignGroup;
      state.pendingAssignGroupPath = JSON.parse(button.dataset.groupPath || "[]");
      state.pendingAssignNodeId = null;
      const action = button.dataset.assignMode === "reassign" ? "Переназначить" : "Назначить всем";
      openDictModal(`${action} в «${button.dataset.groupName}»`);
    });
  }
}

async function saveConfiguration() {
  const dictCodes = new Set(state.dictionary.map((p) => p.name));
  // Validate only nodes for the current endpoint — other endpoints' nodes are preserved as-is
  const currentEndpoint = document.getElementById("configEndpoint")?.value || "";
  const scopedNodes = currentEndpoint
    ? state.draftNodes.filter((n) => n.endpoint_id === currentEndpoint)
    : state.draftNodes;
  const unbound = scopedNodes.filter((n) => !n.parameter_code || !dictCodes.has(n.parameter_code));
  if (unbound.length) {
    const ids = unbound.map((n) => n.parameter_code || n.id).join(", ");
    setConfigStatus(
      `Невозможно сохранить: ${unbound.length} нод не привязаны к параметру из справочника (${ids}). Удалите их или замените на привязанные.`,
      "error",
    );
    return;
  }

  const seenNodeKeys = new Set();
  const duplicateNodes = [];
  for (const node of state.draftNodes) {
    const key = nodeKey(node.endpoint_id, node.node_id);
    if (seenNodeKeys.has(key)) duplicateNodes.push(`${node.endpoint_id}: ${node.node_id}`);
    seenNodeKeys.add(key);
  }
  if (duplicateNodes.length) {
    setConfigStatus(`Невозможно сохранить: найдены повторяющиеся NodeId (${duplicateNodes.slice(0, 5).join(", ")}${duplicateNodes.length > 5 ? "…" : ""}).`, "error");
    return;
  }

  const nodesToSave = state.draftNodes.map((node) => withResolvedBrowsePathMetadata(withResolvedGroupNodeMetadata({
    ...node,
    group_path: Array.isArray(node.group_path) ? node.group_path : [],
  })));
  const response = await fetchJson("/api/config/nodes", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ nodes: nodesToSave }),
  });
  state.configNodes = Array.isArray(response.nodes) ? response.nodes : state.draftNodes;
  state.draftNodes = clone(state.configNodes);
  updatePendingConfigChanges();
  clearConfigSelection();
  setConfigStatus(buildConfigSavedMessage(state.configNodes.length), "success");
  renderMappings();
  renderDictionary();
  renderConfigBrowseTree();
  renderSelectionBridge();
  await fetchSnapshot();
}

function stableStringHash(value) {
  let hash = 0x811c9dc5;
  for (const char of String(value || "")) {
    hash ^= char.codePointAt(0);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(36).padStart(7, "0");
}

function makeNodeConfigId(endpointId, identity, kind = "group") {
  const source = `${kind}\u0000${endpointId || ""}\u0000${identity || ""}`;
  const normalized = `${kind}-${endpointId}-${identity}`
    .toLowerCase()
    .replace(/[^a-z0-9а-яё]+/gi, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 72);
  return `${normalized || kind}-${stableStringHash(source)}`;
}

function mapDatatypeToExpectedType(datatype) {
  const normalized = String(datatype || "").toLowerCase();
  if (["integer", "int", "dint", "long"].includes(normalized)) return "int";
  if (["boolean", "bool"].includes(normalized)) return "bool";
  if (["string", "text"].includes(normalized)) return "str";
  if (["char", "byte"].includes(normalized)) return "char";
  if (["datetime", "date_time"].includes(normalized)) return "datetime";
  return "float";
}

function normalizedBindingValue(value) {
  return String(value || "").trim().toLocaleLowerCase("ru-RU");
}

function downloadResponseBlob(response, fallbackName) {
  return response.blob().then((blob) => {
    const disposition = response.headers.get("Content-Disposition") || "";
    const match = disposition.match(/filename="?([^";]+)"?/i);
    const filename = match?.[1] || fallbackName;
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  });
}

async function downloadBindingsTemplate() {
  const response = await fetch("/api/config/bindings/template");
  if (!response.ok) throw new Error(await response.text());
  await downloadResponseBlob(response, "opcua-bindings-template.xlsx");
  setConfigStatus("Шаблон XLSX скачан. Заполните лист «Привязки» и загрузите файл кнопкой «Импорт».", "success");
}

async function exportBindings() {
  if (!state.draftNodes.length) {
    setConfigStatus("В рабочей области нет привязок для экспорта.", "warn");
    return;
  }
  const response = await fetch("/api/config/bindings/export", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ nodes: state.draftNodes }),
  });
  if (!response.ok) throw new Error(await response.text());
  await downloadResponseBlob(response, "opcua-bindings.xlsx");
  setConfigStatus(`Экспортировано ${state.draftNodes.length} привязок из текущей рабочей области.`, "success");
}

function buildImportedNode(row, endpointId, param) {
  const groupPath = Array.isArray(row.group_path) ? row.group_path : [];
  const unit = param.unit_symbol || param.unit_name || row.unit || null;
  return {
    id: makeNodeConfigId(endpointId, row.node_id, "node"),
    endpoint_id: endpointId,
    node_id: row.node_id,
    browse_name: row.browse_name || null,
    display_name: row.display_name || null,
    enabled: row.enabled !== false,
    acquisition_mode: row.acquisition_mode || "subscription",
    read_enabled: row.read_enabled !== false,
    write_enabled: row.write_enabled === true,
    sampling_interval_ms: Number(row.sampling_interval_ms) || 1000,
    polling_interval_seconds: Number(row.polling_interval_seconds) || 5,
    parameter_code: param.name,
    parameter_name: param.description || row.parameter_name || param.name,
    dict_param_id: param.id || null,
    type_by_dict: param.datatype_name || null,
    unit_by_dict: param.unit_symbol || param.unit_name || null,
    expected_type: mapDatatypeToExpectedType(param.datatype_name || row.expected_type),
    value_shape: row.value_shape || "scalar",
    unit,
    group_id: groupPath.length ? makeNodeConfigId(endpointId, groupPath.join("/")) : null,
    group_path: groupPath,
    group_display_name: groupPath.at(-1) || null,
    value_transform: { scale_factor: 1, offset: 0, target_unit: unit },
    input_control: { stale_after_seconds: 30, suppress_duplicates: false },
    metadata: {
      opcua_browse_name: row.browse_name || null,
      opcua_display_name: row.display_name || null,
      imported_from_table: true,
    },
    tags: Array.isArray(row.tags) ? row.tags : [],
  };
}

function mergeImportedBindings(result) {
  if (!state.dictionary.length) {
    setConfigStatus("Справочник параметров не загружен. Сначала нажмите «Обновить данные» и повторите импорт.", "error");
    return;
  }
  const selectedEndpoint = document.getElementById("configEndpoint")?.value || "";
  const knownEndpoints = new Set(
    [...document.querySelectorAll("#configEndpoint option")].map((option) => option.value).filter(Boolean),
  );
  const dictByCode = new Map(state.dictionary.map((param) => [normalizedBindingValue(param.name), param]));
  const existingNodes = new Set(state.draftNodes.map((node) => nodeKey(node.endpoint_id, node.node_id)));
  const existingParamIds = new Set(state.draftNodes.map((node) => normalizedBindingValue(node.dict_param_id)).filter(Boolean));
  const existingParamCodes = new Set(state.draftNodes.map((node) => normalizedBindingValue(node.parameter_code)).filter(Boolean));
  const importedNodes = new Set();
  const importedParamIds = new Set();
  const importedParamCodes = new Set();
  const messages = [];
  let added = 0;
  let skipped = 0;

  for (const row of result.rows || []) {
    const endpointId = String(row.endpoint_id || selectedEndpoint).trim();
    const param = dictByCode.get(normalizedBindingValue(row.parameter_code));
    const rowLabel = row.source_row
      ? `строка ${row.source_row} (${row.node_id || "NodeId не указан"})`
      : `NodeId ${row.node_id || "не указан"}`;

    if (!endpointId) {
      messages.push(`${rowLabel}: не указан endpoint.`);
      skipped++;
      continue;
    }
    if (knownEndpoints.size && !knownEndpoints.has(endpointId)) {
      messages.push(`${rowLabel}: endpoint «${endpointId}» не найден.`);
      skipped++;
      continue;
    }
    if (!param) {
      messages.push(`${rowLabel}: параметр «${row.parameter_code || ""}» не найден в Справочнике.`);
      skipped++;
      continue;
    }

    const opcKey = nodeKey(endpointId, row.node_id);
    const paramId = normalizedBindingValue(param.id);
    const paramCode = normalizedBindingValue(param.name);
    if (existingNodes.has(opcKey) || importedNodes.has(opcKey)) {
      messages.push(`${rowLabel}: NodeId уже есть в рабочей области.`);
      skipped++;
      continue;
    }
    if (
      (paramId && (existingParamIds.has(paramId) || importedParamIds.has(paramId)))
      || (paramCode && (existingParamCodes.has(paramCode) || importedParamCodes.has(paramCode)))
    ) {
      messages.push(`${rowLabel}: параметр «${param.name}» уже привязан.`);
      skipped++;
      continue;
    }

    state.draftNodes.push(buildImportedNode(row, endpointId, param));
    importedNodes.add(opcKey);
    if (paramId) importedParamIds.add(paramId);
    if (paramCode) importedParamCodes.add(paramCode);
    added++;
  }

  for (const issue of result.issues || []) {
    messages.push(`Строка ${issue.row}, ${issue.field}: ${issue.message}`);
  }
  messages.push(...(result.warnings || []));

  updatePendingConfigChanges();
  renderMappings();
  renderConfigBrowseTree();

  const issueCount = (result.issues || []).length;
  const details = messages.slice(0, 12);
  if (messages.length > details.length) details.push(`И ещё ${messages.length - details.length} замечаний.`);
  const summary = [
    `Импорт: добавлено ${added}, пропущено ${skipped}, ошибок таблицы ${issueCount}.`,
    "Изменения только в черновике; для применения нажмите «Сохранить в клиент».",
    ...details,
  ];
  setConfigStatus(summary.join("\n"), added ? (messages.length ? "warn" : "success") : "warn");
}

async function importBindingsFile(file) {
  if (!file) return;
  const formData = new FormData();
  formData.append("file", file);
  const response = await fetch("/api/config/bindings/import", { method: "POST", body: formData });
  if (!response.ok) {
    let message = await response.text();
    try {
      const parsed = JSON.parse(message);
      message = parsed.detail || message;
    } catch { /* use response text */ }
    throw new Error(message);
  }
  mergeImportedBindings(await response.json());
}

document.getElementById("refreshButton").addEventListener("click", (event) => {
  withBusy(event.currentTarget, fetchSnapshot).catch((error) => {
    const readyBadge = document.getElementById("readyBadge");
    readyBadge.className = "badge badge-bad";
    readyBadge.textContent = "error";
    setText("updatedAt", error.message);
  });
});
document.addEventListener("click", (event) => {
  const button = event.target.closest(".reconnect-now");
  if (!button) return;
  const endpointId = button.dataset.endpointId;
  if (!endpointId) return;
  withBusy(button, async () => {
    await fetchJson("/api/client/request", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ operation: "reconnect", endpoint_id: endpointId }),
    });
    await fetchSnapshot();
  }).catch((error) => {
    const result = document.getElementById("apiResult");
    if (result) result.textContent = error.message;
  });
});
for (const button of document.querySelectorAll("[data-page-target]")) {
  button.addEventListener("click", async () => {
    switchPage(button.dataset.pageTarget);
    if (button.dataset.pageTarget === "config" && !state.dictionary.length) {
      await loadConfigurationPage().catch((error) => setConfigStatus(error.message, "error"));
    }
    if (button.dataset.pageTarget === "sources" && !state.sourcesLoaded) {
      await loadSourcesPage().catch((error) => setSourcesStatus(error.message, "error"));
    }
  });
}
document.getElementById("nodeFilter").addEventListener("input", (event) => {
  state.filter = event.target.value;
  state.nodesPage = 1;
  if (state.snapshot) renderNodes(state.snapshot.nodes);
});
document.getElementById("nodesPrev")?.addEventListener("click", () => {
  state.nodesPage = Math.max(1, state.nodesPage - 1);
  if (state.snapshot) renderNodes(state.snapshot.nodes);
});
document.getElementById("nodesNext")?.addEventListener("click", () => {
  state.nodesPage += 1;
  if (state.snapshot) renderNodes(state.snapshot.nodes);
});
document.getElementById("nodesSelectPage")?.addEventListener("change", (event) => {
  const pageNodes = currentMonitoringPageNodes();
  for (const node of pageNodes) {
    if (!node.config_id) continue;
    if (event.target.checked) {
      state.selectedMonitoringNodeIds.add(node.config_id);
    } else {
      state.selectedMonitoringNodeIds.delete(node.config_id);
    }
  }
  if (state.snapshot) renderNodes(state.snapshot.nodes);
});
document.getElementById("nodesEnableSelected")?.addEventListener("click", (event) => {
  updateSelectedNodesEnabled(true, event.currentTarget).catch((error) => showApiError(error));
});
document.getElementById("nodesDisableSelected")?.addEventListener("click", (event) => {
  updateSelectedNodesEnabled(false, event.currentTarget).catch((error) => showApiError(error));
});
document.getElementById("overloadCounterToggle")?.addEventListener("click", (event) => {
  const enabledNow = event.currentTarget.dataset.enabled === "true";
  updateStatusOverloadCounterEnabled(!enabledNow, event.currentTarget).catch((error) => showApiError(error));
});
document.getElementById("publishAuditPrev")?.addEventListener("click", () => {
  state.publishAuditPage = Math.max(1, state.publishAuditPage - 1);
  renderPublishAudit(state.snapshot?.diagnostics?.publish_audit || []);
});
document.getElementById("publishAuditNext")?.addEventListener("click", () => {
  state.publishAuditPage += 1;
  renderPublishAudit(state.snapshot?.diagnostics?.publish_audit || []);
});
document.getElementById("mappingFilter").addEventListener("input", () => {
  renderMappings();
});
document.getElementById("loadConfigButton").addEventListener("click", () => {
  const button = document.getElementById("loadConfigButton");
  withBusy(button, loadConfigurationPage).catch((error) => setConfigStatus(error.message, "error"));
});
document.getElementById("saveConfigButton").addEventListener("click", () => {
  const button = document.getElementById("saveConfigButton");
  withBusy(button, saveConfiguration).catch((error) => setConfigStatus(error.message, "error"));
});
document.getElementById("bindingsTemplateButton")?.addEventListener("click", (event) => {
  withBusy(event.currentTarget, downloadBindingsTemplate).catch((error) => setConfigStatus(error.message, "error"));
});
document.getElementById("bindingsExportButton")?.addEventListener("click", (event) => {
  withBusy(event.currentTarget, exportBindings).catch((error) => setConfigStatus(error.message, "error"));
});
document.getElementById("bindingsImportButton")?.addEventListener("click", () => {
  document.getElementById("bindingsFileInput")?.click();
});
document.getElementById("bindingsFileInput")?.addEventListener("change", (event) => {
  const file = event.target.files?.[0];
  const button = document.getElementById("bindingsImportButton");
  withBusy(button, () => importBindingsFile(file)).catch((error) => setConfigStatus(error.message, "error"));
  event.target.value = "";
});
document.getElementById("configBrowseButton").addEventListener("click", () => {
  const button = document.getElementById("configBrowseButton");
  withBusy(button, browseForConfig).catch((error) => setConfigStatus(error.message, "error"));
});
document.getElementById("bindSelectionButton")?.addEventListener("click", () => {
  assignSelectedPair();
});

// Drop zone for drag-and-drop from tree
(function setupDropZone() {
  const zone = document.querySelector(".mapping-workspace-col");
  if (!zone) return;
  let dragCounter = 0;
  zone.addEventListener("dragenter", (event) => {
    if (!hasDragType(event.dataTransfer, OPC_NODE_DRAG_TYPE)) return;
    dragCounter++;
    zone.classList.add("drag-over");
  });
  zone.addEventListener("dragover", (event) => {
    if (!hasDragType(event.dataTransfer, OPC_NODE_DRAG_TYPE)) return;
    event.preventDefault();
    event.dataTransfer.dropEffect = "copy";
  });
  zone.addEventListener("dragleave", () => {
    dragCounter--;
    if (dragCounter <= 0) { dragCounter = 0; zone.classList.remove("drag-over"); }
  });
  zone.addEventListener("drop", (event) => {
    dragCounter = 0;
    zone.classList.remove("drag-over");
    const raw = event.dataTransfer.getData(OPC_NODE_DRAG_TYPE);
    if (!raw) return;
    event.preventDefault();
    try {
      const node = JSON.parse(raw);
      const endpointId = node.endpoint_id || document.getElementById("configEndpoint").value;
      if (state.draftNodes.some((n) => n.endpoint_id === endpointId && n.node_id === node.node_id)) {
        setConfigStatus("Нода уже в рабочей области.", "info");
        return;
      }
      addBrowseNodeToDraft({ ...node, endpoint_id: endpointId });
      renderMappings();
      renderConfigBrowseTree();
    } catch { /* ignore malformed data */ }
  });
})();

// Re-render working area when endpoint selection changes
document.getElementById("configEndpoint")?.addEventListener("change", () => {
  renderMappings();
  renderConfigBrowseTree();
});

// Tree search
document.getElementById("configTreeSearch")?.addEventListener("input", (event) => {
  state.configTreeFilter = event.target.value;
  renderConfigBrowseTree();
});

// Dict modal close
document.getElementById("dictModalClose")?.addEventListener("click", () => {
  cancelDictModal();
});
document.getElementById("dictFilter")?.addEventListener("input", (event) => {
  state.dictFilter = event.target.value;
  state.dictVisibleLimit = DICTIONARY_PAGE_SIZE;
  clearTimeout(dictionaryFilterTimer);
  dictionaryFilterTimer = setTimeout(renderDictionary, 100);
});
document.getElementById("dictModalOverlay")?.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    // This modal is intentionally persistent: accidental pointer release on
    // the backdrop and Escape must not discard an in-progress search.
    event.preventDefault();
    return;
  }
  if (event.key !== "Tab") return;
  const modal = event.currentTarget.querySelector(".dict-modal");
  const focusable = [...modal.querySelectorAll('button:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])')];
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable.at(-1);
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last.focus();
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first.focus();
  }
});

window.addEventListener("beforeunload", (event) => {
  if (!state.pendingConfigChanges) return;
  event.preventDefault();
  event.returnValue = "";
});

switchPage(state.activePage);

const initialSectionLoad = state.activePage === "config"
  ? loadConfigurationPage()
  : state.activePage === "sources"
    ? loadSourcesPage()
    : Promise.resolve();

Promise.all([loadOperations(), fetchSnapshot(), initialSectionLoad]).catch((error) => {
  const readyBadge = document.getElementById("readyBadge");
  readyBadge.className = "badge badge-bad";
  readyBadge.textContent = "error";
  setText("updatedAt", error.message);
});

renderConfigBrowseTree();
renderSelectionBridge();

// ── Sources page ──────────────────────────────────────────────────────────────

state.endpoints = [];
state.sourcesLoaded = false;
state.editingEndpointId = null;
state.editingEndpoint = null;
state.pendingDeleteEndpointId = null;
state.endpointModalReturnFocus = null;

function setSourcesStatus(message, tone = "info") {
  const element = document.getElementById("sourcesStatus");
  if (!element) return;
  element.textContent = message;
  element.className = `config-status tone-${tone}`;
  element.classList.toggle("hidden", !message);
}

async function loadSourcesPage() {
  setSourcesStatus("Загрузка списка источников...", "info");
  try {
    const [response] = await Promise.all([
      fetchJson("/api/config/endpoints"),
      fetchSnapshot().catch(() => state.snapshot),
    ]);
    state.endpoints = Array.isArray(response.endpoints) ? response.endpoints : [];
    state.sourcesLoaded = true;
    setSourcesStatus("", "info");
    renderSourcesList();
  } catch (error) {
    setSourcesStatus(`Ошибка загрузки: ${error.message}`, "error");
  }
}

function getEndpointConnectionStatus(endpointId) {
  if (!state.snapshot?.connections) return null;
  return state.snapshot.connections.find((c) => c.endpoint_id === endpointId) || null;
}

function endpointStatusPresentation(endpoint, connection) {
  if (!endpoint.enabled) {
    return { value: "disabled", text: "Выключен", hint: "Подключение отключено в конфигурации" };
  }
  const value = connection?.state || "unknown";
  const labels = {
    connected: "Подключён",
    disconnected: "Отключён",
    reconnecting: "Переподключение",
    connecting: "Подключение",
    degraded: "Нестабильно",
    failed: "Ошибка",
    unknown: "Нет данных",
  };
  const phaseLabels = {
    discovery: "Поиск endpoint",
    session: "Открытие сессии",
    subscriptions: "Создание подписок",
    monitoring: "Получение данных",
  };
  return {
    value,
    text: labels[value] || value,
    hint: phaseLabels[connection?.connection_phase] || connection?.connection_phase || "Состояние ещё не получено",
  };
}

function renderSourcesList() {
  const root = document.getElementById("endpointsList");
  if (!root) return;
  if (!state.endpoints.length) {
    root.innerHTML = `
      <div class="tree-empty">
        <strong>Источники пока не настроены.</strong><br />
        Добавьте профиль подключения к OPC UA серверу.
      </div>`;
    return;
  }
  root.innerHTML = state.endpoints.map((ep) => renderEndpointCardHtml(ep)).join("");

  for (const editBtn of root.querySelectorAll("[data-edit-endpoint]")) {
    editBtn.addEventListener("click", () => {
      const ep = state.endpoints.find((e) => e.id === editBtn.dataset.editEndpoint);
      if (ep) {
        state.pendingDeleteEndpointId = null;
        showEndpointForm(ep);
      }
    });
  }
  for (const delBtn of root.querySelectorAll("[data-delete-endpoint]")) {
    delBtn.addEventListener("click", () => {
      state.pendingDeleteEndpointId = delBtn.dataset.deleteEndpoint;
      renderSourcesList();
      root.querySelector(`[data-confirm-delete-endpoint="${CSS.escape(state.pendingDeleteEndpointId)}"]`)?.focus();
    });
  }
  for (const cancelBtn of root.querySelectorAll("[data-cancel-delete-endpoint]")) {
    cancelBtn.addEventListener("click", () => {
      state.pendingDeleteEndpointId = null;
      renderSourcesList();
    });
  }
  for (const confirmBtn of root.querySelectorAll("[data-confirm-delete-endpoint]")) {
    confirmBtn.addEventListener("click", () => deleteEndpoint(confirmBtn.dataset.confirmDeleteEndpoint, confirmBtn));
  }
}

function renderEndpointCardHtml(ep) {
  const connStatus = getEndpointConnectionStatus(ep.id);
  const presentation = endpointStatusPresentation(ep, connStatus);
  const badgeClass = ep.enabled ? statusBadge(presentation.value, connStatus?.connected) : "badge-muted";
  const authLabels = {
    anonymous: "Anonymous",
    username_password: `Логин: ${ep.auth?.username || "не задан"}`,
    certificate: "Сертификат",
  };
  const authText = authLabels[ep.auth?.mode] || ep.auth?.mode || "Anonymous";
  const lastError = connStatus?.last_error || "";
  const security = `${ep.security_policy || "None"} / ${ep.security_mode || "None"}`;
  const pendingDelete = state.pendingDeleteEndpointId === ep.id;
  return `
    <article class="endpoint-card">
      <div class="endpoint-card-header">
        <div class="endpoint-card-main">
          <span class="badge ${badgeClass}">${escapeHtml(presentation.text)}</span>
          <div class="endpoint-title-block">
            <strong class="endpoint-id">${escapeHtml(ep.id)}</strong>
            <span class="endpoint-state-hint">${escapeHtml(presentation.hint)}</span>
          </div>
        </div>
        <div class="endpoint-card-actions">
          <button class="btn" type="button" data-edit-endpoint="${escapeHtml(ep.id)}">Изменить</button>
          <button class="btn btn-danger" type="button" data-delete-endpoint="${escapeHtml(ep.id)}">Удалить</button>
        </div>
      </div>
      <div class="endpoint-card-body">
        <div class="endpoint-url mono">${escapeHtml(ep.url)}</div>
        <span class="endpoint-connection-detail">${escapeHtml(authText)}</span>
        <span class="endpoint-connection-detail">${escapeHtml(security)}</span>
        ${lastError ? `<div class="endpoint-error"><strong>Ошибка подключения:</strong> ${escapeHtml(lastError)}</div>` : ""}
      </div>
      ${pendingDelete ? `
        <div class="endpoint-delete-confirm" role="alert">
          <span>Будут удалены источник и все его ноды из конфигурации.</span>
          <button class="btn" type="button" data-cancel-delete-endpoint="${escapeHtml(ep.id)}">Отмена</button>
          <button class="btn btn-danger" type="button" data-confirm-delete-endpoint="${escapeHtml(ep.id)}">Удалить безвозвратно</button>
        </div>` : ""}
    </article>
  `;
}

function setEndpointFormError(message = "") {
  const element = document.getElementById("endpointFormError");
  element.textContent = message;
  element.classList.toggle("hidden", !message);
}

function updateEndpointAuthFields() {
  const mode = document.getElementById("efAuthMode").value;
  const editing = Boolean(state.editingEndpointId);
  document.getElementById("efAuthFields").classList.toggle("hidden", mode !== "username_password");
  document.getElementById("efCertificateFields").classList.toggle("hidden", mode !== "certificate");
  document.getElementById("efPasswordRequired").classList.toggle("hidden", editing);
  document.getElementById("efPasswordHint").textContent = editing
    ? "Оставьте пустым, чтобы сохранить текущий пароль."
    : "Обязателен для нового источника.";
}

function showEndpointForm(endpoint = null) {
  state.endpointModalReturnFocus = document.activeElement;
  state.editingEndpointId = endpoint ? endpoint.id : null;
  state.editingEndpoint = endpoint ? clone(endpoint) : null;

  document.getElementById("endpointFormTitle").textContent = endpoint
    ? `Настройки «${endpoint.id}»`
    : "Новое подключение";
  document.getElementById("endpointFormHint").textContent = endpoint
    ? "Измените параметры подключения к OPC UA серверу."
    : "Создайте профиль подключения к OPC UA серверу.";

  const efId = document.getElementById("efId");
  efId.value = endpoint?.id || "";
  efId.disabled = Boolean(endpoint);

  document.getElementById("efUrl").value = endpoint?.url || "";
  document.getElementById("efEnabled").checked = endpoint ? endpoint.enabled !== false : true;

  const authMode = endpoint?.auth?.mode || "anonymous";
  document.getElementById("efAuthMode").value = authMode;
  document.getElementById("efUsername").value = endpoint?.auth?.username || "";
  document.getElementById("efPassword").value = "";
  document.getElementById("efCertificatePath").value = endpoint?.auth?.certificate_path || "";
  document.getElementById("efPrivateKeyPath").value = endpoint?.auth?.private_key_path || "";

  document.getElementById("efSecurityPolicy").value = endpoint?.security_policy || "None";
  document.getElementById("efSecurityMode").value = endpoint?.security_mode || "None";
  document.getElementById("efSessionTimeout").value = endpoint?.session_timeout_ms ?? 60000;
  document.getElementById("efRequestTimeout").value = endpoint?.request_timeout_seconds ?? 10;

  setEndpointFormError("");
  for (const field of document.querySelectorAll("#endpointForm .field-error, #endpointForm [aria-invalid]")) {
    field.classList.remove("field-error");
    field.removeAttribute("aria-invalid");
  }
  updateEndpointAuthFields();

  const wrap = document.getElementById("endpointFormWrap");
  wrap.classList.remove("hidden");
  document.body.classList.add("endpoint-modal-open");
  window.setTimeout(() => (endpoint ? document.getElementById("efUrl") : efId).focus(), 0);
}

function hideEndpointForm() {
  const returnFocus = state.endpointModalReturnFocus;
  state.editingEndpointId = null;
  state.editingEndpoint = null;
  state.endpointModalReturnFocus = null;
  document.getElementById("endpointFormWrap").classList.add("hidden");
  document.body.classList.remove("endpoint-modal-open");
  document.getElementById("endpointForm").reset();
  document.getElementById("efAuthFields").classList.add("hidden");
  document.getElementById("efCertificateFields").classList.add("hidden");
  setEndpointFormError("");
  if (returnFocus instanceof HTMLElement && document.contains(returnFocus)) returnFocus.focus();
}

function defaultEndpointMetadata(endpointId) {
  return {
    source_id: endpointId,
    id_source: null,
    source_system_id: null,
    owner_type: "opcua_endpoint",
    owner_id: endpointId,
    site_id: null,
    asset_id: null,
    well_id: null,
    tags: [],
  };
}

function collectEndpointFormData() {
  const authMode = document.getElementById("efAuthMode").value;
  const password = document.getElementById("efPassword").value;
  const base = state.editingEndpoint ? clone(state.editingEndpoint) : {};
  const auth = { ...(base.auth || {}), mode: authMode };
  if (authMode === "username_password") {
    auth.username = document.getElementById("efUsername").value.trim() || null;
    auth.password = password || null;
    auth.certificate_path = null;
    auth.private_key_path = null;
  } else if (authMode === "certificate") {
    auth.username = null;
    auth.password = null;
    auth.certificate_path = document.getElementById("efCertificatePath").value.trim() || null;
    auth.private_key_path = document.getElementById("efPrivateKeyPath").value.trim() || null;
  } else {
    auth.username = null;
    auth.password = null;
    auth.certificate_path = null;
    auth.private_key_path = null;
  }

  const endpointId = document.getElementById("efId").value.trim();
  const metadata = base.metadata ? clone(base.metadata) : defaultEndpointMetadata(endpointId);

  return {
    ...base,
    id: endpointId,
    url: document.getElementById("efUrl").value.trim(),
    enabled: document.getElementById("efEnabled").checked,
    security_policy: document.getElementById("efSecurityPolicy").value.trim() || "None",
    security_mode: document.getElementById("efSecurityMode").value,
    session_timeout_ms: Number(document.getElementById("efSessionTimeout").value),
    request_timeout_seconds: Number(document.getElementById("efRequestTimeout").value),
    auth,
    metadata,
  };
}

function validateEndpointForm(data) {
  const errors = [];
  const addError = (fieldId, message) => {
    const field = document.getElementById(fieldId);
    field.setAttribute("aria-invalid", "true");
    field.classList.add("field-error");
    errors.push({ field, message });
  };
  for (const field of document.querySelectorAll("#endpointForm .field-error")) {
    field.classList.remove("field-error");
    field.removeAttribute("aria-invalid");
  }
  if (!data.id) addError("efId", "Укажите ID источника.");
  else if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(data.id)) {
    addError("efId", "ID может содержать латинские буквы, цифры, точку, дефис и подчёркивание.");
  }
  if (!data.url) addError("efUrl", "Укажите URL OPC UA сервера.");
  else if (!/^opc\.tcp:\/\/[^\s/]+(?::\d+)?(?:\/.*)?$/i.test(data.url)) {
    addError("efUrl", "URL должен начинаться с opc.tcp:// и содержать адрес сервера.");
  }
  if (data.auth.mode === "username_password") {
    if (!data.auth.username) addError("efUsername", "Укажите имя пользователя.");
    if (!state.editingEndpointId && !data.auth.password) addError("efPassword", "Укажите пароль для нового источника.");
  }
  if (data.auth.mode === "certificate") {
    if (!data.auth.certificate_path) addError("efCertificatePath", "Укажите путь к сертификату внутри контейнера клиента.");
    if (!data.auth.private_key_path) addError("efPrivateKeyPath", "Укажите путь к закрытому ключу внутри контейнера клиента.");
  }
  if (!Number.isFinite(data.session_timeout_ms) || data.session_timeout_ms < 1000) {
    addError("efSessionTimeout", "Session timeout должен быть не меньше 1000 мс.");
  }
  if (!Number.isFinite(data.request_timeout_seconds) || data.request_timeout_seconds < 0.1) {
    addError("efRequestTimeout", "Request timeout должен быть не меньше 0,1 с.");
  }
  if (data.security_policy === "None" && data.security_mode !== "None") {
    addError("efSecurityMode", "Для Security policy None режим также должен быть None.");
  }
  if (errors.length) {
    setEndpointFormError(errors.map((error) => `• ${error.message}`).join("\n"));
    errors[0].field.focus();
    return false;
  }
  setEndpointFormError("");
  return true;
}

async function submitEndpointForm(event) {
  event.preventDefault();
  const submitBtn = document.getElementById("submitEndpointForm");
  const data = collectEndpointFormData();

  if (!validateEndpointForm(data)) return;
  setBusyState(submitBtn, true);
  try {
    let statusMessage;
    if (state.editingEndpointId) {
      await fetchJson(`/api/config/endpoints/${encodeURIComponent(state.editingEndpointId)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      statusMessage = `Источник «${state.editingEndpointId}» обновлён.`;
    } else {
      await fetchJson("/api/config/endpoints", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      });
      statusMessage = `Источник «${data.id}» создан.`;
    }
    hideEndpointForm();
    await loadSourcesPage();
    setSourcesStatus(statusMessage, "success");
  } catch (error) {
    setEndpointFormError(`Не удалось сохранить источник. ${error.message}`);
    setSourcesStatus(`Ошибка сохранения: ${error.message}`, "error");
  } finally {
    setBusyState(submitBtn, false);
  }
}

async function deleteEndpoint(endpointId, button) {
  setBusyState(button, true);
  try {
    await fetchJson(`/api/config/endpoints/${encodeURIComponent(endpointId)}`, { method: "DELETE" });
    state.pendingDeleteEndpointId = null;
    await loadSourcesPage();
    setSourcesStatus(`Источник «${endpointId}» и связанные с ним ноды удалены.`, "success");
  } catch (error) {
    setSourcesStatus(`Ошибка удаления: ${error.message}`, "error");
  } finally {
    setBusyState(button, false);
  }
}

document.getElementById("loadSourcesButton").addEventListener("click", () => {
  const button = document.getElementById("loadSourcesButton");
  withBusy(button, loadSourcesPage).catch((error) => setSourcesStatus(error.message, "error"));
});
document.getElementById("createEndpointButton").addEventListener("click", () => {
  state.pendingDeleteEndpointId = null;
  showEndpointForm(null);
});
document.getElementById("cancelEndpointForm").addEventListener("click", () => {
  hideEndpointForm();
});
document.getElementById("closeEndpointForm").addEventListener("click", () => {
  hideEndpointForm();
});
document.getElementById("endpointForm").addEventListener("submit", (event) => {
  submitEndpointForm(event);
});
document.getElementById("efAuthMode").addEventListener("change", () => {
  updateEndpointAuthFields();
});
document.getElementById("endpointForm").addEventListener("input", (event) => {
  if (event.target.matches("input, select")) {
    event.target.classList.remove("field-error");
    event.target.removeAttribute("aria-invalid");
    setEndpointFormError("");
  }
});
document.getElementById("endpointFormWrap").addEventListener("keydown", (event) => {
  if (event.key !== "Tab") return;
  const focusable = [...event.currentTarget.querySelectorAll(
    'button:not([disabled]), input:not([disabled]), select:not([disabled]), summary, [tabindex]:not([tabindex="-1"])',
  )].filter((element) => !element.closest(".hidden"));
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last.focus();
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first.focus();
  }
});

setInterval(() => {
  if (!["dashboard", "diagnostics", "sources"].includes(state.activePage)) return;
  fetchSnapshot().catch(() => undefined);
}, 5000);

setInterval(() => {
  if (state.activePage !== "diagnostics") return;
  fetchStatusOverloadCounter();
}, 1000);

setInterval(() => {
  updateStatusOverloadCounterTimer();
}, 1000);
