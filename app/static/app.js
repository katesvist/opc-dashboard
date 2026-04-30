const state = {
  snapshot: null,
  operations: [],
  filter: "",
  dictFilter: "",
  browseItems: [],
  browseExpanded: new Set(),
  configBrowseItems: [],
  configBrowseExpanded: new Set(),
  dictionary: [],
  configNodes: [],
  draftNodes: [],
  selectedBrowseNode: null,
  selectedDictParamId: null,
  pendingConfigChanges: false,
  activePage: "dashboard",
};

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
  state.snapshot = await fetchJson("/api/snapshot");
  render();
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

  renderEndpointOptions(snapshot.connections);
  renderConnections(snapshot.connections, snapshot.readiness?.endpoints || []);
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
    if (operation.id === "browse" && response.ok && Array.isArray(response.response)) {
      applyBrowseResult(payload, response.response);
    }
    await fetchSnapshot();
  } catch (error) {
    result.classList.remove("loading");
    result.textContent = error.message;
  }
}

function applyBrowseResult(payload, items) {
  state.browseItems = items;
  state.browseExpanded = new Set(
    items
      .filter((item) => item.has_children && Number(item.depth ?? 0) < 2)
      .map((item) => item.node_id),
  );
  const nodeInfo = payload.node_id ? `, стартовый узел: ${payload.node_id}` : "";
  setText(
    "browseTreeMeta",
    `Endpoint: ${payload.endpoint_id}${nodeInfo}, depth: ${payload.max_depth}, узлов: ${items.length}`,
  );
  renderBrowseTree();
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
    max_depth: Number(document.getElementById("apiDepth").value || 1),
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
  if (item.state === "failed") return "Подключение завершилось ошибкой, но OPC UA client не передал текст причины.";
  if (item.state === "reconnecting") return "Идет переподключение к OPC UA серверу.";
  if (item.state === "disconnected") return "Соединение разорвано.";
  return "-";
}

function renderConnections(connections, readinessEndpoints = []) {
  const table = document.getElementById("connectionsTable");
  table.innerHTML = "";
  if (!connections.length) {
    table.innerHTML = `<tr><td colspan="5" class="muted">Endpoint не зарегистрированы.</td></tr>`;
    return;
  }

  for (const item of connections) {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td class="mono">${item.endpoint_id}</td>
      <td><span class="badge ${statusBadge(item.state, item.connected)}">${item.state}</span></td>
      <td>${formatDate(item.last_data_at)}</td>
      <td>${item.reconnect_attempts}</td>
      <td class="node-id">${escapeHtml(getConnectionError(item, readinessEndpoints))}</td>
    `;
    table.append(row);
  }
}

function renderBrowseTree() {
  const root = document.getElementById("browseTree");
  const items = state.browseItems;

  if (!items.length) {
    root.innerHTML = `<div class="tree-empty">Нажмите Browse, чтобы загрузить дерево.</div>`;
    return;
  }

  const byParent = new Map();
  for (const item of items) {
    const key = item.parent_node_id ?? "__root__";
    const bucket = byParent.get(key) || [];
    bucket.push(item);
    byParent.set(key, bucket);
  }

  const renderBranch = (parentId, level) => {
    const nodes = byParent.get(parentId) || [];
    return nodes
      .map((item) => {
        const children = byParent.get(item.node_id) || [];
        const expandable = children.length > 0;
        const expanded = state.browseExpanded.has(item.node_id);
        const label = item.display_name || item.browse_name || item.node_id;
        const meta = [];
        if (item.node_class) meta.push(item.node_class);
        if (item.data_type) meta.push(item.data_type);
        if (item.access_level?.length) meta.push(item.access_level.join(", "));

        return `
          <div class="tree-row" style="padding-left: ${8 + level * 18}px">
            ${
              expandable
                ? `<button class="tree-toggle" type="button" data-node-id="${escapeHtml(item.node_id)}">${expanded ? "−" : "+"}</button>`
                : `<span class="tree-spacer"></span>`
            }
            <div class="tree-content" data-select-node-id="${escapeHtml(item.node_id)}" data-select-endpoint-id="${escapeHtml(document.getElementById("apiEndpoint").value || "")}">
              <div class="tree-title">
                <span class="tree-name">${escapeHtml(label)}</span>
                <span class="tree-class">${escapeHtml(item.node_class || "")}</span>
              </div>
              <div class="tree-meta">${meta.map((part) => `<span>${escapeHtml(part)}</span>`).join("")}</div>
              <div class="tree-node-id">${escapeHtml(item.node_id)}</div>
            </div>
          </div>
          ${expandable && expanded ? renderBranch(item.node_id, level + 1) : ""}
        `;
      })
      .join("");
  };

  root.innerHTML = `<div class="tree-list">${renderBranch("__root__", 0)}</div>`;

  for (const button of root.querySelectorAll("[data-node-id]")) {
    button.addEventListener("click", (event) => {
      const nodeId = event.currentTarget.dataset.nodeId;
      if (!nodeId) return;
      if (state.browseExpanded.has(nodeId)) {
        state.browseExpanded.delete(nodeId);
      } else {
        state.browseExpanded.add(nodeId);
      }
      renderBrowseTree();
    });
  }

  for (const element of root.querySelectorAll("[data-select-node-id]")) {
    element.addEventListener("click", (event) => {
      const nodeId = event.currentTarget.dataset.selectNodeId;
      const endpointId = event.currentTarget.dataset.selectEndpointId;
      if (endpointId) document.getElementById("apiEndpoint").value = endpointId;
      if (nodeId) document.getElementById("apiNodeId").value = nodeId;
    });
  }
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
  const query = state.filter.trim().toLowerCase();
  const visibleNodes = query
    ? nodes.filter((node) => {
        const haystack = [
          node.parameter_code,
          node.node_id,
          node.endpoint_id,
          node.acquisition_mode,
          node.read?.data_type,
          node.read?.value,
        ].join(" ").toLowerCase();
        return haystack.includes(query);
      })
    : nodes;

  table.innerHTML = "";
  if (!visibleNodes.length) {
    table.innerHTML = `<tr><td colspan="8" class="muted">Ноды не найдены.</td></tr>`;
    return;
  }

  for (const node of visibleNodes) {
    const status = node.status;
    const read = node.read;
    const active = Boolean(status?.active);
    const row = document.createElement("tr");
    row.innerHTML = `
      <td class="mono">${node.parameter_code || "-"}</td>
      <td class="node-id">${node.node_id || "-"}</td>
      <td class="mono">${node.endpoint_id || "-"}</td>
      <td>${node.acquisition_mode || "-"}</td>
      <td><span class="value">${formatValue(read?.value)}</span>${node.read_error ? `<div class="muted">${node.read_error}</div>` : ""}</td>
      <td>${read?.status_code || "-"}</td>
      <td>${formatDate(read?.source_timestamp || status?.last_value_at)}</td>
      <td><span class="badge ${active ? "badge-ok" : "badge-muted"}">${active ? "active" : "inactive"}</span></td>
    `;
    row.addEventListener("click", () => {
      document.getElementById("apiEndpoint").value = node.endpoint_id || "";
      document.getElementById("apiNodeId").value = node.node_id || "";
    });
    row.title = "Нажмите, чтобы подставить endpoint и node_id в форму API.";
    table.append(row);
  }
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
  const nodeId = document.getElementById("configNodeId").value.trim();
  const depth = Number(document.getElementById("configDepth").value || 2);
  if (!endpointId) {
    setConfigStatus("Выберите endpoint для browse.", "warn");
    return;
  }
  setConfigStatus("Загружаю дерево OPC UA...", "info");
  const params = new URLSearchParams({
    endpoint_id: endpointId,
    max_depth: String(depth),
    include_variables: "true",
    include_objects: "true",
  });
  if (nodeId) params.set("node_id", nodeId);
  const response = await fetchJson(`/api/browse?${params.toString()}`);
  state.configBrowseItems = Array.isArray(response.items) ? response.items : [];
  state.configBrowseExpanded = new Set(
    state.configBrowseItems
      .filter((item) => item.has_children && Number(item.depth ?? 0) < 2)
      .map((item) => item.node_id),
  );
  const variableCount = state.configBrowseItems.filter((item) => item.node_class === "Variable").length;
  setText("configBrowseMeta", `${state.configBrowseItems.length} узлов · ${variableCount} variable`);
  setConfigStatus(`Дерево загружено: ${state.configBrowseItems.length} узлов, из них ${variableCount} variable.`, "success");
  renderConfigBrowseTree();
}

function renderConfigBrowseTree() {
  const root = document.getElementById("configBrowseTree");
  const items = state.configBrowseItems;
  if (!items.length) {
    root.innerHTML = `<div class="tree-empty">Нажмите Browse, чтобы загрузить дерево.</div>`;
    return;
  }

  const byParent = new Map();
  for (const item of items) {
    const key = item.parent_node_id ?? "__root__";
    const bucket = byParent.get(key) || [];
    bucket.push(item);
    byParent.set(key, bucket);
  }

  const endpointId = document.getElementById("configEndpoint").value || "";
  const renderBranch = (parentId, level) => {
    const nodes = byParent.get(parentId) || [];
    return nodes
      .map((item) => {
        const children = byParent.get(item.node_id) || [];
        const expandable = children.length > 0;
        const expanded = state.configBrowseExpanded.has(item.node_id);
        const draggable = item.node_class === "Variable";
        const selected = state.selectedBrowseNode?.endpoint_id === endpointId && state.selectedBrowseNode?.node_id === item.node_id;
        const mapped = hasPersistedMappingForBrowseNode(endpointId, item.node_id);
        const label = item.display_name || item.browse_name || item.node_id;
        const meta = [item.node_class, item.data_type].filter(Boolean);
        return `
          <div class="tree-row" style="padding-left: ${8 + level * 18}px">
            ${
              expandable
                ? `<button class="tree-toggle" type="button" data-config-toggle="${escapeHtml(item.node_id)}">${expanded ? "−" : "+"}</button>`
                : `<span class="tree-spacer"></span>`
            }
            <div class="tree-content config-tree-content ${draggable ? "tree-content-draggable" : "tree-content-static"} ${selected ? "selected" : ""}" ${draggable ? 'draggable="true"' : ""} data-config-node='${escapeHtml(JSON.stringify({ ...item, endpoint_id: endpointId }))}'>
              <div class="tree-title">
                <span class="tree-name">${escapeHtml(label)}</span>
                <span class="tree-class">${escapeHtml(item.node_class || "")}</span>
                ${mapped ? `<span class="inline-badge">mapped</span>` : ""}
              </div>
              <div class="tree-meta">${meta.map((part) => `<span>${escapeHtml(part)}</span>`).join("")}</div>
              <div class="tree-node-id">${escapeHtml(item.node_id)}</div>
            </div>
          </div>
          ${expandable && expanded ? renderBranch(item.node_id, level + 1) : ""}
        `;
      })
      .join("");
  };

  root.innerHTML = `<div class="tree-list">${renderBranch("__root__", 0)}</div>`;

  for (const button of root.querySelectorAll("[data-config-toggle]")) {
    button.addEventListener("click", (event) => {
      const nodeId = event.currentTarget.dataset.configToggle;
      if (state.configBrowseExpanded.has(nodeId)) {
        state.configBrowseExpanded.delete(nodeId);
      } else {
        state.configBrowseExpanded.add(nodeId);
      }
      renderConfigBrowseTree();
    });
  }

  for (const element of root.querySelectorAll("[data-config-node]")) {
    element.addEventListener("click", () => {
      const clickedNode = JSON.parse(element.dataset.configNode);
      state.selectedBrowseNode = isSameBrowseNode(state.selectedBrowseNode, clickedNode) ? null : clickedNode;
      renderConfigBrowseTree();
      renderSelectionBridge();
      setConfigStatus(buildSelectionStatusMessage(), "info");
    });
    element.addEventListener("dragstart", (event) => {
      const payload = element.dataset.configNode;
      event.dataTransfer.setData("application/json", payload);
      event.dataTransfer.effectAllowed = "copy";
      state.selectedBrowseNode = JSON.parse(payload);
      renderConfigBrowseTree();
      renderSelectionBridge();
      root.classList.add("is-dragging");
    });
    element.addEventListener("dragend", () => {
      root.classList.remove("is-dragging");
    });
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
            ${mapped ? `<span class="badge badge-ok">mapped</span>` : ""}
          </div>
        </article>
      `;
    })
    .join("");

  for (const card of root.querySelectorAll("[data-dict-id]")) {
    card.addEventListener("dragover", (event) => {
      event.preventDefault();
      card.classList.add("drop-target");
    });
    card.addEventListener("dragenter", (event) => {
      event.preventDefault();
      card.classList.add("drop-target");
    });
    card.addEventListener("dragleave", () => card.classList.remove("drop-target"));
    card.addEventListener("drop", (event) => {
      event.preventDefault();
      card.classList.remove("drop-target");
      const raw = event.dataTransfer.getData("application/json");
      if (!raw) return;
      assignNodeToParam(JSON.parse(raw), card.dataset.dictId);
    });
    card.addEventListener("click", () => {
      state.selectedDictParamId = state.selectedDictParamId === card.dataset.dictId ? null : card.dataset.dictId;
      renderDictionary();
      renderSelectionBridge();
      setConfigStatus(buildSelectionStatusMessage(), "info");
    });
  }
}

function renderSelectionBridge() {
  const selectedNodeCard = document.getElementById("selectedNodeCard");
  const selectedParamCard = document.getElementById("selectedParamCard");
  const bindButton = document.getElementById("bindSelectionButton");

  const node = state.selectedBrowseNode;
  const param = state.dictionary.find((item) => item.id === state.selectedDictParamId) || null;

  selectedNodeCard.classList.toggle("filled", Boolean(node));
  selectedParamCard.classList.toggle("filled", Boolean(param));

  setText("selectedNodeTitle", node?.display_name || node?.browse_name || node?.node_id || "Не выбрана");
  setText(
    "selectedNodeMeta",
    node ? `${node.endpoint_id || "-"} · ${node.node_class || "-"} · ${node.node_id}` : "Выберите variable-ноду в дереве слева.",
  );
  setText("selectedParamTitle", param?.name || "Не выбран");
  setText(
    "selectedParamMeta",
    param
      ? `${param.description || "-"} · ${param.datatype_name || "-"} · ${param.unit_symbol || param.unit_name || "без единиц"}`
      : "Выберите параметр в справочнике справа.",
  );

  bindButton.disabled = !(node && param);
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
  setText("mappingMeta", `${state.draftNodes.length} нод`);
  if (!state.draftNodes.length) {
    root.innerHTML = `<div class="tree-empty">Привязки еще не настроены.</div>`;
    return;
  }
  root.innerHTML = state.draftNodes
    .map((node) => {
      const changed = JSON.stringify(node) !== JSON.stringify(state.configNodes.find((item) => item.id === node.id));
      return `
        <article class="mapping-card ${changed ? "changed" : ""}">
          <div>
            <div class="mapping-name">${escapeHtml(node.parameter_code || node.id)}</div>
            <div class="mapping-node">${escapeHtml(node.node_id)}</div>
            <div class="mapping-meta">
              <span>${escapeHtml(node.endpoint_id)}</span>
              <span>${escapeHtml(node.acquisition_mode)}</span>
              <span>${escapeHtml(node.expected_type || "-")}</span>
              <span>${escapeHtml(node.unit || "без единиц")}</span>
            </div>
          </div>
          <button class="mapping-remove" type="button" data-remove-node="${escapeHtml(node.id)}">Удалить</button>
        </article>
      `;
    })
    .join("");

  for (const button of root.querySelectorAll("[data-remove-node]")) {
    button.addEventListener("click", () => {
      state.draftNodes = state.draftNodes.filter((node) => node.id !== button.dataset.removeNode);
      updatePendingConfigChanges();
      setConfigStatus("Привязка удалена из черновика. Не забудьте сохранить изменения в клиент.", "warn");
      renderDictionary();
      renderMappings();
    });
  }
}

async function saveConfiguration() {
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
for (const button of document.querySelectorAll("[data-page-target]")) {
  button.addEventListener("click", async () => {
    switchPage(button.dataset.pageTarget);
    if (button.dataset.pageTarget === "config" && !state.dictionary.length) {
      await loadConfigurationPage().catch((error) => setConfigStatus(error.message, "error"));
    }
  });
}
document.getElementById("nodeFilter").addEventListener("input", (event) => {
  state.filter = event.target.value;
  if (state.snapshot) renderNodes(state.snapshot.nodes);
});
document.getElementById("dictFilter").addEventListener("input", (event) => {
  state.dictFilter = event.target.value;
  renderDictionary();
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
document.getElementById("bindSelectionButton").addEventListener("click", () => {
  assignSelectedPair();
});

switchPage(state.activePage);

Promise.all([loadOperations(), fetchSnapshot()]).catch((error) => {
  const readyBadge = document.getElementById("readyBadge");
  readyBadge.className = "badge badge-bad";
  readyBadge.textContent = "error";
  setText("updatedAt", error.message);
});

renderBrowseTree();
renderConfigBrowseTree();
renderSelectionBridge();

setInterval(() => {
  fetchSnapshot().catch(() => undefined);
}, 5000);
