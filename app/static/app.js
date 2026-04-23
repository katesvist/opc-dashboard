const state = {
  snapshot: null,
  operations: [],
  filter: "",
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

function render() {
  const snapshot = state.snapshot;
  if (!snapshot) return;

  const connected = snapshot.connections.filter((item) => item.connected).length;
  const activeNodes = snapshot.nodes.filter((item) => item.status?.active).length;
  const readyBadge = document.getElementById("readyBadge");
  const healthy = Boolean(snapshot.healthy);

  readyBadge.className = `badge ${healthy ? "badge-ok" : "badge-bad"}`;
  readyBadge.textContent = healthy ? "client up" : "client down";

  setText("clientUrl", snapshot.client.base_url);
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
      ? `${snapshot.rabbitmq.messages_ready ?? 0} ready / ${snapshot.rabbitmq.messages_unacknowledged ?? 0} unacked`
      : "RabbitMQ недоступен",
  );

  renderEndpointOptions(snapshot.connections);
  renderConnections(snapshot.connections);
  renderNodes(snapshot.nodes);
}

function renderEndpointOptions(connections) {
  for (const id of ["apiEndpoint"]) {
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
    button.addEventListener("click", () => runOperation(operation));
    if (operation.group === "technical") {
      technical.append(button);
    } else {
      functional.append(button);
    }
  }
}

async function runOperation(operation) {
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

  result.textContent = pretty({
    status: `running ${operation.id}...`,
    request_preview: payload,
  });

  try {
    const response = await fetchJson("/api/client/request", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    result.textContent = pretty(response);
    await fetchSnapshot();
  } catch (error) {
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

function renderConnections(connections) {
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
      <td class="node-id">${item.last_error || "-"}</td>
    `;
    table.append(row);
  }
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

document.getElementById("refreshButton").addEventListener("click", fetchSnapshot);
document.getElementById("nodeFilter").addEventListener("input", (event) => {
  state.filter = event.target.value;
  if (state.snapshot) renderNodes(state.snapshot.nodes);
});

Promise.all([loadOperations(), fetchSnapshot()]).catch((error) => {
  const readyBadge = document.getElementById("readyBadge");
  readyBadge.className = "badge badge-bad";
  readyBadge.textContent = "error";
  setText("updatedAt", error.message);
});

setInterval(() => {
  fetchSnapshot().catch(() => undefined);
}, 5000);
