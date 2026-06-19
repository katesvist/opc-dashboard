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
  configNodes: [],
  draftNodes: [],
  selectedBrowseNode: null,
  selectedDictParamId: null,
  pendingAssignNodeId: null,
  pendingAssignGroupId: null,
  workspaceCollapsed: new Set(),
  pendingConfigChanges: false,
  activePage: "dashboard",
  snapshotLoading: false,
  overloadCounterLoading: false,
  publishAuditPage: 1,
  nodesPage: 1,
  selectedMonitoringNodeIds: new Set(),
  overloadCounterEnabled: false,
  overloadCounterStartedAtMs: null,
};

const GROUP_SUBSCRIBE_MAX_DEPTH = "2";
const OPC_NODE_DRAG_TYPE = "application/x-opc-node";
const PUBLISH_AUDIT_PAGE_SIZE = 200;
const MONITORING_NODES_PAGE_SIZE = 20;

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

const clone = (value) => {
  if (typeof structuredClone === "function") return structuredClone(value);
  return JSON.parse(JSON.stringify(value));
};

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

const buildDictByName = () => {
  const result = new Map();
  for (const param of state.dictionary) {
    if (param.name) result.set(param.name, param);
    if (param.description) result.set(param.description, param);
  }
  return result;
};

function setConfigStatus(message, tone = "info") {
  const element = document.getElementById("configStatus");
  if (!element) return;
  element.textContent = message;
  element.className = `config-status tone-${tone}`;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, { cache: "no-store", ...options });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) throw new Error(pretty(payload));
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
  const enabled = counter.enabled !== false;
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
}

function clearConfigSelection() {
  state.selectedBrowseNode = null;
  state.selectedDictParamId = null;
  state.pendingAssignNodeId = null;
  state.pendingAssignGroupId = null;
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
    await fetchJson("/api/client/request", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        operation: "set_status_overload_counter_enabled",
        enabled,
      }),
    });
    await fetchSnapshot();
  });
}

function showApiError(error) {
  const result = document.getElementById("apiResult");
  if (result) result.textContent = error.message;
}

function switchPage(page) {
  state.activePage = page;
  for (const section of document.querySelectorAll("[data-page]")) {
    section.classList.toggle("hidden", section.dataset.page !== page);
  }
  for (const button of document.querySelectorAll("[data-page-target]")) {
    button.classList.toggle("active", button.dataset.pageTarget === page);
  }
}

async function loadConfigurationPage() {
  setConfigStatus("Загружаю текущую конфигурацию клиента и справочник параметров...", "info");
  const [configResult, dictionaryResult] = await Promise.allSettled([
    fetchJson("/api/config/nodes"),
    fetchJson("/api/dictionary"),
  ]);

  const messages = [];
  if (configResult.status === "fulfilled") {
    state.configNodes = Array.isArray(configResult.value.nodes) ? configResult.value.nodes : [];
    state.draftNodes = clone(state.configNodes);
    updatePendingConfigChanges();
    messages.push(`Конфигурация клиента: ${state.draftNodes.length} нод`);
  } else {
    messages.push(`Конфигурация клиента не загружена: ${configResult.reason.message}`);
  }

  if (dictionaryResult.status === "fulfilled") {
    state.dictionary = Array.isArray(dictionaryResult.value.params) ? dictionaryResult.value.params : [];
    messages.push(`Справочник параметров: ${state.dictionary.length} параметров`);
    messages.push(`Сервис параметров: ${dictionaryResult.value.base_url || state.snapshot?.client?.params_service_base_url || "-"}`);
  } else {
    messages.push(`Справочник не загружен: ${dictionaryResult.reason.message}`);
  }

  messages.push(`OPC UA клиент: ${state.snapshot?.client?.base_url || "-"}`);
  clearConfigSelection();
  setConfigStatus(messages.join("\n"), configResult.status === "fulfilled" || dictionaryResult.status === "fulfilled" ? "success" : "error");
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

  const variableCount = state.configBrowseItems.filter((item) => item.node_class === "Variable").length;
  setText("configBrowseMeta", `${state.configBrowseItems.length} узлов · ${variableCount} variable`);
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
          <div class="tree-row" style="padding-left: ${8 + level * 18}px">
            ${
              expandable
                ? `<button class="tree-toggle" type="button" data-config-toggle="${escapeHtml(item.node_id)}">${expanded ? "−" : "+"}</button>`
                : `<span class="tree-spacer"></span>`
            }
            <div class="tree-content config-tree-content ${isVariable ? "tree-node-variable" : "tree-content-static"} ${highlight ? "in-workspace" : ""}"
              data-config-node='${nodeJson}'
              ${isVariable ? `draggable="true" data-drag-node='${nodeJson}'` : ""}>
              <div class="tree-title">
                <span class="tree-name">${label}</span>
                <span class="tree-class">${escapeHtml(item.node_class || "")}</span>
                ${highlight ? `<span class="ws-dot" title="В рабочей области"></span>` : ""}
                ${mapped ? `<span class="inline-badge">в работе</span>` : ""}
                ${showAllBtn ? `<button class="group-subscribe-btn" type="button" data-group-subscribe='${nodeJson}' title="Добавить все дочерние Variable-ноды">
                  <span class="group-subscribe-icon" aria-hidden="true"></span>
                  <span>все</span>
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
    element.addEventListener("click", (event) => {
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
  const nodeId = makeNodeConfigId(endpointId, param?.name || browseNode.browse_name || browseNode.node_id);
  state.draftNodes.push({
    id: nodeId,
    endpoint_id: endpointId,
    node_id: browseNode.node_id,
    browse_name: browseNode.browse_name || null,
    display_name: browseNode.display_name || null,
    dict_param_id: param?.id || null,
    parameter_code: param?.name || null,
    parameter_name: param?.description || param?.name || null,
    acquisition_mode: "subscription",
    read_enabled: true,
    write_enabled: false,
    sampling_interval_ms: 1000,
    polling_interval_seconds: 5,
    expected_type: param ? mapDatatypeToExpectedType(param.datatype_name) : null,
    value_shape: "scalar",
    unit: param?.unit_symbol || param?.unit_name || null,
    group_id: groupData?.group_id || null,
    group_path: groupData?.group_path || null,
    group_display_name: groupData?.group_display_name || null,
    value_transform: { scale_factor: 1, offset: 0, target_unit: param?.unit_symbol || param?.unit_name || null },
    input_control: { stale_after_seconds: 30, suppress_duplicates: false },
    metadata: {
      opcua_browse_name: browseNode.browse_name || null,
      opcua_display_name: browseNode.display_name || null,
      opcua_data_type: browseNode.data_type || null,
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
        addGroupFromItems(child.node_id, subPath, {
          group_id: makeNodeConfigId(endpointId, subPath.join("/")),
          group_path: subPath,
          group_display_name: child.display_name || child.browse_name || child.node_id,
        }, allItems, endpointId, ctx);
      }
    } else if (child.node_class === "Object") {
      const subName = child.browse_name || child.display_name || child.node_id;
      const subPath = [...groupPath, subName];
      addGroupFromItems(child.node_id, subPath, {
        group_id: makeNodeConfigId(endpointId, subPath.join("/")),
        group_path: subPath,
        group_display_name: child.display_name || child.browse_name || child.node_id,
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
  const query = state.dictFilter.trim().toLowerCase();
  const visible = query
    ? state.dictionary.filter((param) =>
        [
          param.name,
          param.description,
          param.datatype_name,
          param.unit_name,
          param.unit_symbol,
        ].join(" ").toLowerCase().includes(query),
      )
    : state.dictionary;

  setText("dictMeta", `${visible.length} / ${state.dictionary.length} параметров`);
  if (!visible.length) {
    root.innerHTML = `<div class="tree-empty">Параметры не найдены.</div>`;
    return;
  }

  root.innerHTML = visible
    .map((param) => {
      const mapped = hasPersistedMappingForParam(param.id);
      const selected = state.selectedDictParamId === param.id;
      return `
        <article class="dict-card ${selected ? "selected" : ""}" data-dict-id="${escapeHtml(param.id)}">
          <div class="dict-name">${escapeHtml(param.name)}</div>
          <div class="dict-description">${escapeHtml(param.description || "-")}</div>
          <div class="dict-meta">
            <span>${escapeHtml(param.datatype_name || "-")}</span>
            <span>${escapeHtml(param.unit_symbol || param.unit_name || "без единиц")}</span>
            ${mapped ? `<span class="badge badge-ok">назначен</span>` : ""}
          </div>
        </article>
      `;
    })
    .join("");

  for (const card of root.querySelectorAll("[data-dict-id]")) {
    card.addEventListener("click", () => {
      if (state.pendingAssignGroupId) {
        assignParamToGroup(state.pendingAssignGroupId, card.dataset.dictId);
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
}

function renderSelectionBridge() {
  // selection-bridge UI is currently commented out in HTML
}

function openDictModal(title = "Справочник параметров") {
  const titleEl = document.querySelector(".dict-modal-title");
  if (titleEl) titleEl.textContent = title;
  const overlay = document.getElementById("dictModalOverlay");
  if (overlay) overlay.classList.remove("hidden");
  renderDictionary();
  document.getElementById("dictFilter")?.focus();
}

function closeDictModal() {
  const overlay = document.getElementById("dictModalOverlay");
  if (overlay) overlay.classList.add("hidden");
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

function assignParamToGroup(groupId, dictParamId) {
  const param = state.dictionary.find((item) => item.id === dictParamId);
  if (!param) return;
  const currentEndpoint = document.getElementById("configEndpoint")?.value || "";
  const baseNode = state.draftNodes.find((n) => n.group_id === groupId);
  if (!baseNode) return;
  const basePath = Array.isArray(baseNode.group_path) && baseNode.group_path.length > 0
    ? baseNode.group_path
    : null;
  // Assign to nodes in this group AND all descendant groups (group_path starts with basePath)
  const groupNodes = state.draftNodes.filter((n) => {
    if (currentEndpoint && n.endpoint_id !== currentEndpoint) return false;
    if (!basePath) return n.group_id === groupId; // no path info: exact group_id match only
    if (!n.group_path) return n.group_id === groupId;
    if (n.group_path.length < basePath.length) return false;
    return basePath.every((seg, i) => seg === n.group_path[i]);
  });
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
  closeDictModal();
  updatePendingConfigChanges();
  renderMappings();
  renderConfigBrowseTree();
  const groupName = baseNode?.group_display_name || groupId;
  const actionText = reassignedCount > 0 ? "переназначен" : "назначен";
  setConfigStatus(`Параметр «${param.name}» ${actionText} ${groupNodes.length} нодам группы «${groupName}». Не забудьте сохранить.`, "warn");
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
    id: previous?.id || makeNodeConfigId(endpointId, param.name),
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

  const makeNodeRow = (node, rowIdx, depth = 0) => {
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
    const indentLevel = Math.max(depth, 0);
    return `
      <tr class="ua-row ${rowClass}" data-node-id="${escapeHtml(node.id)}">
        <td class="col-num">${rowIdx}</td>
        <td class="col-nodeid" title="${escapeHtml(node.node_id + (node.browse_name ? '\n' + node.browse_name : ''))}">
          <div class="ua-node-cell depth-${Math.min(indentLevel, 6)}">
            <code>${escapeHtml(node.node_id)}</code>
            ${node.browse_name ? `<span class="ua-browse-name">${escapeHtml(node.browse_name)}</span>` : ""}
          </div>
        </td>
        <td class="col-param-cell" ${paramTitle ? `title="${paramTitle}"` : ""}>${paramCell}</td>
        <td class="col-mode">${escapeHtml(node.acquisition_mode || "-")}</td>
        <td class="col-type">${escapeHtml(node.expected_type || "-")}</td>
        <td class="col-del"><button class="ua-del-btn" type="button" data-remove-node="${escapeHtml(node.id)}" title="Удалить">×</button></td>
      </tr>`;
  };

  // Build group map keyed by group_id
  const groupMap = new Map();
  const ungrouped = [];
  for (const node of filtered) {
    if (node.group_id) {
      if (!groupMap.has(node.group_id)) {
        groupMap.set(node.group_id, {
          group_id: node.group_id,
          group_path: node.group_path || [],
          display_name: node.group_display_name || node.group_id,
          nodes: [],
          children: [],
        });
      }
      groupMap.get(node.group_id).nodes.push(node);
    } else {
      ungrouped.push(node);
    }
  }

  // Build parent-child relationships from group_path prefix matching
  const allGroups = [...groupMap.values()].sort((a, b) => a.group_path.length - b.group_path.length);
  for (const group of allGroups) {
    if (group.group_path.length < 2) continue;
    const parentPath = group.group_path.slice(0, -1);
    const parent = allGroups.find(
      (g) => g.group_path.length === parentPath.length && parentPath.every((seg, i) => seg === g.group_path[i]),
    );
    if (parent) parent.children.push(group);
  }
  const topGroups = allGroups.filter((g) => {
    if (g.group_path.length < 2) return true;
    const parentPath = g.group_path.slice(0, -1);
    return !allGroups.some(
      (other) => other !== g && other.group_path.length === parentPath.length && parentPath.every((seg, i) => seg === other.group_path[i]),
    );
  });

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
    const unbound = hasUnbound(group);
    const assignGroupLabel = unbound ? "Назначить всем" : "Переназначить";
    // Always show full path so the name is never empty
    const pathLabel = Array.isArray(group.group_path) && group.group_path.length > 0
      ? group.group_path.join(" / ")
      : (group.display_name || group.group_id);
    html += `
      <tr class="ua-group-header${depth > 0 ? " ua-group-sub" : ""}">
        <td class="ua-group-toggle-cell">
          <button class="ua-group-toggle" type="button" data-toggle-group="${escapeHtml(group.group_id)}">${isCollapsed ? "▶" : "▼"}</button>
        </td>
        <td colspan="5" class="ua-group-cell">
          <div class="ua-group-inner depth-${Math.min(depth, 6)}">
            <span class="ua-group-name-text" title="${escapeHtml(pathLabel)}">${escapeHtml(pathLabel)}</span>
            <span class="ua-group-count">${totalCount} нод</span>
            <button class="ua-assign-group-btn ${unbound ? "has-unbound" : ""}" type="button"
              data-assign-group="${escapeHtml(group.group_id)}"
              data-group-name="${escapeHtml(pathLabel)}"
              data-assign-mode="${unbound ? "assign" : "reassign"}">${assignGroupLabel}</button>
          </div>
        </td>
      </tr>`;
    if (!isCollapsed) {
      let localIdx = 0;
      for (const node of group.nodes) {
        localIdx++;
        html += makeNodeRow(node, localIdx, depth + 1);
      }
      for (const child of group.children) {
        renderGroup(child, depth + 1);
      }
    }
  }

  for (const group of topGroups) {
    renderGroup(group, 0);
  }
  let ungroupedIdx = 0;
  for (const node of ungrouped) {
    ungroupedIdx++;
    html += makeNodeRow(node, ungroupedIdx, 0);
  }

  root.innerHTML = html;

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

  for (const button of root.querySelectorAll("[data-remove-node]")) {
    button.addEventListener("click", () => {
      if (state.pendingAssignNodeId === button.dataset.removeNode) state.pendingAssignNodeId = null;
      state.draftNodes = state.draftNodes.filter((node) => node.id !== button.dataset.removeNode);
      updatePendingConfigChanges();
      setConfigStatus("Нода удалена из черновика.", "warn");
      renderConfigBrowseTree();
      renderMappings();
    });
  }

  for (const button of root.querySelectorAll("[data-assign-node]")) {
    button.addEventListener("click", () => {
      state.pendingAssignNodeId = button.dataset.assignNode;
      state.pendingAssignGroupId = null;
      renderMappings();
      openDictModal(button.dataset.assignMode === "reassign" ? "Переназначить параметр" : "Назначить параметр");
    });
  }

  for (const button of root.querySelectorAll("[data-assign-group]")) {
    button.addEventListener("click", () => {
      state.pendingAssignGroupId = button.dataset.assignGroup;
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

  const response = await fetchJson("/api/config/nodes", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ nodes: state.draftNodes }),
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

function makeNodeConfigId(endpointId, paramName) {
  const normalized = `${endpointId}-${paramName}`
    .toLowerCase()
    .replace(/[^a-z0-9а-яё]+/gi, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 96);
  return normalized || `node-${Date.now()}`;
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

document.getElementById("refreshButton").addEventListener("click", (event) => {
  withBusy(event.currentTarget, fetchSnapshot).catch(() => undefined);
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
  state.pendingAssignNodeId = null;
  state.pendingAssignGroupId = null;
  closeDictModal();
  renderMappings();
});
document.getElementById("dictModalOverlay")?.addEventListener("click", (event) => {
  if (event.target === event.currentTarget) {
    state.pendingAssignNodeId = null;
    state.pendingAssignGroupId = null;
    closeDictModal();
    renderMappings();
  }
});
document.getElementById("dictFilter")?.addEventListener("input", (event) => {
  state.dictFilter = event.target.value;
  renderDictionary();
});

switchPage(state.activePage);

Promise.all([loadOperations(), fetchSnapshot()]).catch((error) => {
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
    const response = await fetchJson("/api/config/endpoints");
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

function renderSourcesList() {
  const root = document.getElementById("endpointsList");
  if (!root) return;
  if (!state.endpoints.length) {
    root.innerHTML = `<div class="tree-empty">Источники не настроены. Нажмите «+ Добавить источник».</div>`;
    return;
  }
  root.innerHTML = state.endpoints.map((ep) => renderEndpointCardHtml(ep)).join("");

  for (const editBtn of root.querySelectorAll("[data-edit-endpoint]")) {
    editBtn.addEventListener("click", () => {
      const ep = state.endpoints.find((e) => e.id === editBtn.dataset.editEndpoint);
      if (ep) showEndpointForm(ep);
    });
  }
  for (const delBtn of root.querySelectorAll("[data-delete-endpoint]")) {
    delBtn.addEventListener("click", async () => {
      const id = delBtn.dataset.deleteEndpoint;
      if (
        !confirm(
          `Удалить источник «${id}»?\n\nВсе ноды этого источника будут автоматически удалены из конфигурации клиента.`,
        )
      )
        return;
      await deleteEndpoint(id, delBtn);
    });
  }
}

function renderEndpointCardHtml(ep) {
  const connStatus = getEndpointConnectionStatus(ep.id);
  const stateValue = connStatus?.state || (ep.enabled ? "unknown" : "disabled");
  const badgeClass = ep.enabled ? statusBadge(stateValue, connStatus?.connected) : "badge-muted";
  const badgeText = ep.enabled ? stateValue || "unknown" : "disabled";
  const authText =
    ep.auth?.mode === "username_password" ? `user: ${ep.auth?.username || "—"}` : "anonymous";
  const lastError = connStatus?.last_error || "";
  return `
    <article class="endpoint-card">
      <div class="endpoint-card-header">
        <div class="endpoint-card-main">
          <span class="badge ${badgeClass}">${escapeHtml(badgeText)}</span>
          <strong class="endpoint-id">${escapeHtml(ep.id)}</strong>
        </div>
        <div class="endpoint-card-actions">
          <button class="btn" type="button" data-edit-endpoint="${escapeHtml(ep.id)}">Изменить</button>
          <button class="btn btn-danger" type="button" data-delete-endpoint="${escapeHtml(ep.id)}">Удалить</button>
        </div>
      </div>
      <div class="endpoint-card-body">
        <div class="endpoint-url mono">${escapeHtml(ep.url)}</div>
        <div class="endpoint-meta">
          <span>Auth: ${escapeHtml(authText)}</span>
          ${ep.metadata?.source_id ? `<span>Source: ${escapeHtml(ep.metadata.source_id)}</span>` : ""}
          ${ep.metadata?.owner_id ? `<span>Owner: ${escapeHtml(ep.metadata.owner_type || "")} / ${escapeHtml(ep.metadata.owner_id)}</span>` : ""}
          ${connStatus?.reconnect_attempts ? `<span>Reconnects: ${connStatus.reconnect_attempts}</span>` : ""}
          ${lastError ? `<span class="endpoint-error">Error: ${escapeHtml(lastError)}</span>` : ""}
        </div>
      </div>
    </article>
  `;
}

function showEndpointForm(endpoint = null) {
  state.editingEndpointId = endpoint ? endpoint.id : null;
  state.editingEndpoint = endpoint ? clone(endpoint) : null;

  document.getElementById("endpointFormTitle").textContent = endpoint
    ? `Редактирование: ${endpoint.id}`
    : "Новый источник";

  const efId = document.getElementById("efId");
  efId.value = endpoint?.id || "";
  efId.disabled = Boolean(endpoint);

  document.getElementById("efUrl").value = endpoint?.url || "";
  document.getElementById("efEnabled").checked = endpoint ? endpoint.enabled !== false : true;

  const authMode = endpoint?.auth?.mode || "anonymous";
  document.getElementById("efAuthMode").value = authMode;
  document.getElementById("efUsername").value = endpoint?.auth?.username || "";
  document.getElementById("efPassword").value = "";
  document.getElementById("efAuthFields").classList.toggle("hidden", authMode !== "username_password");

  document.getElementById("efSourceId").value = endpoint?.metadata?.source_id || "";
  document.getElementById("efOwnerType").value = endpoint?.metadata?.owner_type || "";
  document.getElementById("efOwnerId").value = endpoint?.metadata?.owner_id || "";
  document.getElementById("efSourceSystemId").value = endpoint?.metadata?.source_system_id || "";
  document.getElementById("efSiteId").value = endpoint?.metadata?.site_id || "";
  document.getElementById("efAssetId").value = endpoint?.metadata?.asset_id || "";
  document.getElementById("efWellId").value = endpoint?.metadata?.well_id || "";

  const wrap = document.getElementById("endpointFormWrap");
  wrap.classList.remove("hidden");
  wrap.scrollIntoView({ behavior: "smooth", block: "start" });
}

function hideEndpointForm() {
  state.editingEndpointId = null;
  state.editingEndpoint = null;
  document.getElementById("endpointFormWrap").classList.add("hidden");
  document.getElementById("endpointForm").reset();
  document.getElementById("efAuthFields").classList.add("hidden");
}

function collectEndpointFormData() {
  const authMode = document.getElementById("efAuthMode").value;
  const password = document.getElementById("efPassword").value;
  const auth = { mode: authMode };
  if (authMode === "username_password") {
    auth.username = document.getElementById("efUsername").value || null;
    auth.password = password || null;
  }

  const metadata = {
    source_id: document.getElementById("efSourceId").value.trim(),
    owner_type: document.getElementById("efOwnerType").value.trim(),
    owner_id: document.getElementById("efOwnerId").value.trim(),
  };
  const sourceSystemId = document.getElementById("efSourceSystemId").value.trim();
  const siteId = document.getElementById("efSiteId").value.trim();
  const assetId = document.getElementById("efAssetId").value.trim();
  const wellId = document.getElementById("efWellId").value.trim();
  if (sourceSystemId) metadata.source_system_id = sourceSystemId;
  if (siteId) metadata.site_id = siteId;
  if (assetId) metadata.asset_id = assetId;
  if (wellId) metadata.well_id = wellId;

  const base = state.editingEndpoint ? clone(state.editingEndpoint) : {};
  return {
    ...base,
    id: document.getElementById("efId").value.trim(),
    url: document.getElementById("efUrl").value.trim(),
    enabled: document.getElementById("efEnabled").checked,
    auth,
    metadata,
  };
}

async function submitEndpointForm(event) {
  event.preventDefault();
  const submitBtn = document.getElementById("submitEndpointForm");
  const data = collectEndpointFormData();

  if (!data.id) {
    setSourcesStatus("ID источника обязателен.", "error");
    return;
  }
  if (!data.url) {
    setSourcesStatus("URL источника обязателен.", "error");
    return;
  }
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
    setSourcesStatus(`Ошибка сохранения: ${error.message}`, "error");
  } finally {
    setBusyState(submitBtn, false);
  }
}

async function deleteEndpoint(endpointId, button) {
  setBusyState(button, true);
  try {
    await fetchJson(`/api/config/endpoints/${encodeURIComponent(endpointId)}`, { method: "DELETE" });
    setSourcesStatus(`Источник «${endpointId}» удалён.`, "success");
    await loadSourcesPage();
    await fetchSnapshot();
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
  showEndpointForm(null);
});
document.getElementById("cancelEndpointForm").addEventListener("click", () => {
  hideEndpointForm();
});
document.getElementById("endpointForm").addEventListener("submit", (event) => {
  submitEndpointForm(event);
});
document.getElementById("efAuthMode").addEventListener("change", (event) => {
  document.getElementById("efAuthFields").classList.toggle("hidden", event.target.value !== "username_password");
});

setInterval(() => {
  if (!["dashboard", "diagnostics"].includes(state.activePage)) return;
  fetchSnapshot().catch(() => undefined);
}, 5000);

setInterval(() => {
  if (state.activePage !== "diagnostics") return;
  fetchStatusOverloadCounter();
}, 1000);

setInterval(() => {
  updateStatusOverloadCounterTimer();
}, 1000);
