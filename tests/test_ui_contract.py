from __future__ import annotations

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[1]
APP_JS = ROOT / "app/static/app.js"
INDEX_HTML = ROOT / "app/static/index.html"


def test_node_config_id_uses_full_node_identity_and_hash() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    functions = source[
        source.index("function stableStringHash") : source.index("function mapDatatypeToExpectedType")
    ]
    script = f"""
{functions}
const ids = [
  makeNodeConfigId('remote', 'ns=3;s="DB_A"."Value"', 'node'),
  makeNodeConfigId('remote', 'ns=3;s="DB_B"."Value"', 'node'),
  makeNodeConfigId('remote', 'ns=3;s="DB_A"."Value"', 'node'),
];
console.log(JSON.stringify(ids));
"""

    result = subprocess.run(
        ["node", "-e", script],
        check=True,
        capture_output=True,
        text=True,
    )
    ids = json.loads(result.stdout)

    assert ids[0] != ids[1]
    assert ids[0] == ids[2]
    assert len(ids[0]) <= 80


def test_active_page_is_restored_after_reload_and_loads_its_data() -> None:
    source = APP_JS.read_text(encoding="utf-8")

    assert 'const PAGE_IDS = ["dashboard", "diagnostics", "config", "sources"]' in source
    assert 'activePage: getInitialPage()' in source
    assert "window.location.hash" in source
    assert "window.sessionStorage.getItem(ACTIVE_PAGE_STORAGE_KEY)" in source
    assert "window.sessionStorage.setItem(ACTIVE_PAGE_STORAGE_KEY, page)" in source
    assert 'state.activePage === "config"' in source
    assert "loadConfigurationPage()" in source
    assert 'state.activePage === "sources"' in source
    assert "loadSourcesPage()" in source


def test_sources_form_preserves_full_endpoint_contract() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    html = INDEX_HTML.read_text(encoding="utf-8")
    block = source[source.index("function collectEndpointFormData") : source.index("function validateEndpointForm")]

    assert 'option value="certificate"' in html
    assert 'id="efIdSource"' not in html
    assert 'id="efTags"' not in html
    assert 'id="efSecurityPolicy"' in html
    assert 'id="efSecurityMode"' in html
    assert "base.metadata ? clone(base.metadata) : defaultEndpointMetadata(endpointId)" in block
    assert "certificate_path" in block
    assert "private_key_path" in block


def test_sources_edit_keeps_unexposed_metadata_and_new_source_gets_technical_defaults() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    collect = source[source.index("function defaultEndpointMetadata") : source.index("function validateEndpointForm")]
    script = f"""
const values = {{
  efAuthMode: {{value: 'username_password'}}, efPassword: {{value: ''}},
  efUsername: {{value: 'operator'}}, efCertificatePath: {{value: ''}}, efPrivateKeyPath: {{value: ''}},
  efId: {{value: 'ep-1'}},
  efUrl: {{value: 'opc.tcp://server:4840'}}, efEnabled: {{checked: true}},
  efSecurityPolicy: {{value: 'None'}}, efSecurityMode: {{value: 'None'}},
  efSessionTimeout: {{value: '60000'}}, efRequestTimeout: {{value: '10'}},
}};
const document = {{getElementById: (id) => values[id]}};
const clone = (value) => JSON.parse(JSON.stringify(value));
const state = {{editingEndpoint: {{
  id: 'ep-1', auth: {{mode: 'username_password', password: '***'}},
  metadata: {{source_id: 'old', id_source: 'uuid', owner_type: 'rig', owner_id: 'rig-1', tags: ['edge'], future_field: 'keep-me'}},
  discovery: {{enabled: true}},
}}}};
{collect}
const edited = collectEndpointFormData();
state.editingEndpoint = null;
const created = collectEndpointFormData();
console.log(JSON.stringify({{edited, created}}));
"""
    result = subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
    payload = json.loads(result.stdout)

    assert payload["edited"]["metadata"] == {
        "source_id": "old",
        "id_source": "uuid",
        "owner_type": "rig",
        "owner_id": "rig-1",
        "tags": ["edge"],
        "future_field": "keep-me",
    }
    assert payload["edited"]["auth"]["password"] is None
    assert payload["edited"]["discovery"] == {"enabled": True}
    assert payload["created"]["metadata"]["source_id"] == "ep-1"
    assert payload["created"]["metadata"]["owner_type"] == "opcua_endpoint"
    assert payload["created"]["metadata"]["owner_id"] == "ep-1"
    assert payload["created"]["metadata"]["id_source"] is None


def test_sources_have_inline_validation_and_safe_delete_confirmation() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    html = INDEX_HTML.read_text(encoding="utf-8")

    assert 'id="endpointFormError"' in html
    assert 'data-confirm-delete-endpoint=' in source
    assert 'data-cancel-delete-endpoint=' in source
    assert "confirm(" not in source[source.index("// ── Sources page") :]
    assert 'addError("efSourceId"' not in source
    assert 'addError("efOwnerType"' not in source
    assert 'addError("efOwnerId"' not in source
    delete_block = source[source.index("async function deleteEndpoint") : source.index('document.getElementById("loadSourcesButton")')]
    assert delete_block.index("await loadSourcesPage()") < delete_block.index("setSourcesStatus(`Источник")


def test_sources_use_compact_cards_and_modal_editor() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    html = INDEX_HTML.read_text(encoding="utf-8")

    assert 'id="sourcesOverview"' not in html
    assert 'id="sourcesFilter"' not in html
    assert "function renderSourcesOverview" not in source
    assert "function endpointMatchesFilter" not in source
    assert 'class="endpoint-modal" role="dialog" aria-modal="true"' in html
    assert 'id="closeEndpointForm"' in html
    assert 'class="endpoint-runtime"' not in source
    assert 'class="endpoint-card ${cardClass}"' not in source
    assert 'document.body.classList.add("endpoint-modal-open")' in source
    assert 'document.body.classList.remove("endpoint-modal-open")' in source
    assert 'event.key !== "Tab"' in source
    assert 'state.activePage === "sources" && state.sourcesLoaded' in source
    assert '["dashboard", "diagnostics", "sources"]' in source


def test_bindings_export_and_import_are_scoped_to_selected_endpoint() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    export_block = source[source.index("async function exportBindings") : source.index("function buildImportedNode")]
    import_block = source[source.index("function mergeImportedBindings") : source.index('document.getElementById("refreshButton")')]

    assert 'document.getElementById("configEndpoint")?.value' in export_block
    assert "state.draftNodes.filter((node) => node.endpoint_id === selectedEndpoint)" in export_block
    assert "JSON.stringify({ endpoint_id: selectedEndpoint, nodes: endpointNodes })" in export_block
    assert "state.draftNodes.length" not in export_block
    assert "result.endpoint_ids" in import_block
    assert "importedEndpoints.size > 1" in import_block
    assert "tableEndpoint !== selectedEndpoint" in import_block
    assert "new URLSearchParams({ endpoint_id: selectedEndpoint })" in import_block


def test_workspace_waits_for_endpoint_before_rendering_nodes() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    render = source[source.index("function renderMappings") : source.index("async function saveConfiguration")]
    endpoint_options = source[source.index("function renderEndpointOptions") : source.index("function renderOperations")]

    endpoint_guard = render.index("if (!currentEndpoint)")
    endpoint_filter = render.index(
        "state.draftNodes.filter((node) => node.endpoint_id === currentEndpoint)"
    )
    assert endpoint_guard < endpoint_filter
    assert ": state.draftNodes" not in render[:endpoint_filter]
    assert "Рабочая область появится после выбора endpoint." in render
    assert "configEndpointChanged" in endpoint_options
    assert "resetConfigBrowseStateForEndpointChange();" in endpoint_options
    assert "renderMappings();" in endpoint_options
    assert 'document.getElementById("configEndpoint")?.addEventListener("change"' in source


def test_group_nodes_are_created_from_node_id_not_browse_name() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    block = source[source.index("function addBrowseNodeToDraft") : source.index("function addGroupFromItems")]

    assert 'makeNodeConfigId(endpointId, browseNode.node_id, "node")' in block
    assert "param?.name || browseNode.browse_name" not in block


def test_group_parent_node_identity_is_preserved_for_copying() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    group_add = source[source.index("function addBrowseNodeToDraft") : source.index("async function groupSubscribeObject")]
    render = source[source.index("function renderMappings") : source.index("async function saveConfiguration")]

    assert "opcua_group_nodes" in group_add
    assert "node_id: parentNode.node_id" in source
    assert 'data-copy-text="${escapeHtml(groupNodeId)}"' in render
    assert "resolveBrowseGroupNode(path, node.endpoint_id)" in render
    assert "withResolvedGroupNodeMetadata" in source


def test_existing_group_node_id_fallback_requires_an_unambiguous_browse_path() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    functions = source[
        source.index("function normalizedGroupPath") : source.index("function groupPathStartsWith")
    ]
    script = f"""
const document = {{getElementById: () => ({{value: 'ep'}})}};
const state = {{configBrowseItems: [
  {{node_id: 'db-1', parent_node_id: 'objects', browse_name: '3:DB', display_name: 'DB', node_class: 'Object'}},
  {{node_id: 'array-1', parent_node_id: 'db-1', browse_name: '3:ARRAY', display_name: 'ARRAY', node_class: 'Variable'}},
]}};
{functions}
const found = resolveBrowseGroupNode(['DB', 'ARRAY'], 'ep');
state.configBrowseItems.push({{node_id: 'array-duplicate', parent_node_id: 'db-1', browse_name: '3:ARRAY', display_name: 'ARRAY'}});
const ambiguous = resolveBrowseGroupNode(['DB', 'ARRAY'], 'ep');
console.log(JSON.stringify({{found, ambiguous}}));
"""

    result = subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
    payload = json.loads(result.stdout)

    assert payload["found"]["node_id"] == "array-1"
    assert payload["ambiguous"] is None


def test_removal_is_single_row_and_reversible() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    removal = source[source.index('for (const button of root.querySelectorAll("[data-remove-node]"))') :]
    removal = removal[: removal.index('for (const button of root.querySelectorAll("[data-assign-node]"))')]

    assert "state.draftNodes.splice(removedIndex, 1)" in removal
    assert 'label: "Отменить"' in removal
    assert "state.draftNodes.filter" not in removal


def test_parameter_dialog_has_accessible_contract() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    html = INDEX_HTML.read_text(encoding="utf-8")

    assert 'role="dialog"' in html
    assert 'aria-modal="true"' in html
    assert 'aria-labelledby="dictModalTitle"' in html
    assert 'aria-live="polite"' in html
    assert 'document.getElementById("dictModalOverlay")?.addEventListener("click"' not in source
    keydown = source[source.index('document.getElementById("dictModalOverlay")?.addEventListener("keydown"') :]
    keydown = keydown[: keydown.index('window.addEventListener("beforeunload"')]
    escape_branch = keydown[keydown.index('if (event.key === "Escape")') : keydown.index('if (event.key !== "Tab")')]
    assert "cancelDictModal()" not in escape_branch
    assert "event.preventDefault()" in escape_branch


def test_large_workspace_groups_are_rendered_in_pages() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    render = source[source.index("function renderMappings") : source.index("async function saveConfiguration")]

    assert "const WORKSPACE_PAGE_SIZE = 100" in source
    assert "group.nodes.slice(0, visibleLimit)" in render
    assert 'data-load-more-group="${escapeHtml(group.group_id)}"' in render
    assert "state.workspaceVisibleLimits.set(groupId, currentLimit + WORKSPACE_PAGE_SIZE)" in render


def test_api_errors_show_detail_instead_of_raw_json() -> None:
    source = APP_JS.read_text(encoding="utf-8")

    assert 'if (typeof payload.detail === "string") return payload.detail' in source
    assert "throw new Error(errorMessageFromPayload(payload))" in source


def test_group_membership_includes_descendants_but_not_siblings() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    functions = source[
        source.index("function normalizedGroupPath") : source.index("function assignParamToGroup")
    ]
    script = f"""
const state = {{draftNodes: [
  {{id: 'root', endpoint_id: 'ep', group_id: 'g-root', group_path: ['DB']}},
  {{id: 'child', endpoint_id: 'ep', group_id: 'g-child', group_path: ['DB', 'Array']}},
  {{id: 'deep', endpoint_id: 'ep', group_id: 'g-deep', group_path: ['DB', 'Array', 'Part']}},
  {{id: 'sibling', endpoint_id: 'ep', group_id: 'g-sibling', group_path: ['Other']}},
]}};
{functions}
console.log(JSON.stringify(draftGroupMembers('virtual-db', ['DB'], 'ep').map(node => node.id)));
"""

    result = subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)

    assert json.loads(result.stdout) == ["root", "child", "deep"]


def test_workspace_materializes_group_prefixes_and_supports_group_undo() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    render = source[source.index("function renderMappings") : source.index("async function saveConfiguration")]
    removal = source[source.index("function removeDraftGroup") : source.index("function assignSelectedPair")]

    assert "for (let length = 1; length <= groupPath.length; length += 1)" in render
    assert "const groupsByPath = new Map()" in render
    assert "const groupsById = new Map" in render
    assert 'data-remove-group="${escapeHtml(group.group_id)}"' in render
    assert "const removedEntries = state.draftNodes" in removal
    assert 'label: "Отменить"' in removal


def test_group_removal_and_undo_restore_exact_nodes() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    membership = source[
        source.index("function normalizedGroupPath") : source.index("function assignParamToGroup")
    ]
    removal = source[source.index("function removeDraftGroup") : source.index("function assignSelectedPair")]
    script = f"""
const state = {{
  draftNodes: [
    {{id: 'a', endpoint_id: 'ep', node_id: 'a', group_id: 'g-a', group_path: ['DB']}},
    {{id: 'b', endpoint_id: 'ep', node_id: 'b', group_id: 'g-b', group_path: ['DB', 'Array']}},
    {{id: 'c', endpoint_id: 'ep', node_id: 'c', group_id: 'g-c', group_path: ['Other']}},
    {{id: 'same-id', endpoint_id: 'other', node_id: 'b', group_id: 'g-b', group_path: ['DB', 'Array']}},
  ],
  pendingAssignNodeId: null,
  pendingAssignGroupId: null,
  pendingAssignGroupPath: null,
  workspaceCollapsed: new Set(),
  workspaceVisibleLimits: new Map(),
}};
const nodeKey = (endpointId, nodeId) => `${{endpointId}}\\u0000${{nodeId}}`;
let undoAction = null;
function updatePendingConfigChanges() {{}}
function renderConfigBrowseTree() {{}}
function renderMappings() {{}}
function setConfigStatus(message, tone, action) {{ undoAction = action; }}
{membership}
{removal}
removeDraftGroup('virtual-db', ['DB'], 'ep', 'DB');
const afterRemove = state.draftNodes.map(node => `${{node.endpoint_id}}:${{node.node_id}}`);
undoAction.onClick();
const afterUndo = state.draftNodes.map(node => `${{node.endpoint_id}}:${{node.node_id}}`);
console.log(JSON.stringify({{afterRemove, afterUndo}}));
"""

    result = subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
    payload = json.loads(result.stdout)

    assert payload["afterRemove"] == ["ep:c", "other:b"]
    assert payload["afterUndo"] == ["ep:a", "ep:b", "ep:c", "other:b"]


def test_bootstrap_icons_are_embedded_with_license() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    license_file = ROOT / "app/static/vendor/BOOTSTRAP_ICONS_LICENSE.txt"

    assert 'function bootstrapIcon(name, className = "")' in source
    assert 'const nodeKind = isObject ? "object" : isVariable ? "variable" : isMethod ? "method" : "node"' in source
    assert "bootstrapIcon(nodeKind)" in source
    assert 'object: \'<path' in source
    assert 'variable: \'<path' in source
    assert 'method: \'<rect' in source
    assert 'bootstrapIcon("group")' in source
    assert 'bootstrapIcon("chevron-right", "group-subscribe-icon")' in source
    assert 'bootstrapIcon("trash")' in source
    assert "The MIT License" in license_file.read_text(encoding="utf-8")


def test_browse_tree_and_workspace_have_no_connectors() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    css = (ROOT / "app/static/app.css").read_text(encoding="utf-8")

    assert 'class="tree-child-stem"' not in source
    assert '.tree-row:not([data-tree-level="0"])::before' not in css
    assert '.tree-row:not([data-tree-level="0"])::after' not in css
    assert 'is-last-branch-item' not in source
    assert ".ua-row.is-last-branch-item" not in css
    assert ".ua-group-sub .ua-group-inner::before" not in css
    assert ".ua-group-sub .ua-group-inner::after" not in css


def test_grouped_nodes_use_the_same_compact_row_as_ungrouped_nodes() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    css = (ROOT / "app/static/app.css").read_text(encoding="utf-8")
    render = source[source.index("function renderMappings") : source.index("async function saveConfiguration")]

    assert "html += makeNodeRow(node, localIdx);" in render
    assert '<div class="ua-node-cell">' in render
    assert "--tree-depth" not in render
    assert "vertical-align: middle" in css


def test_single_node_keeps_its_browse_path_without_becoming_a_group() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    path_function = source[source.index("function browsePathForNode") : source.index("const buildDictByName")]
    render = source[source.index("function renderMappings") : source.index("async function saveConfiguration")]
    script = f"""
{path_function}
const items = [
  {{node_id: 'objects', display_name: 'Objects', parent_node_id: null}},
  {{node_id: 'server', display_name: 'Server', parent_node_id: 'objects'}},
  {{node_id: 'value', display_name: 'UrisVersion', parent_node_id: 'server'}},
];
console.log(JSON.stringify(browsePathForNode(items[2], items)));
"""

    result = subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)

    assert json.loads(result.stdout) == ["Objects", "Server", "UrisVersion"]
    assert "opcua_browse_path: browsePathForNode(browseNode)" in source
    assert 'class="ua-node-breadcrumb"' in render
    assert "node.group_id" in render


def test_add_branch_button_is_readable_and_loading_spinner_stays_inside() -> None:
    css = (ROOT / "app/static/app.css").read_text(encoding="utf-8")
    button_styles = css[css.index(".group-subscribe-btn {") : css.index("/* Compact dict cards */")]

    assert "font-size: 12px" in button_styles
    assert "min-height: 26px" in button_styles
    assert "position: relative" in button_styles
    assert "overflow: hidden" in button_styles
    assert ".group-subscribe-btn.is-loading::after" in button_styles
    assert "width: 12px" in button_styles


def test_standalone_nodes_get_a_section_divider_only_after_groups() -> None:
    source = APP_JS.read_text(encoding="utf-8")
    render = source[source.index("function renderMappings") : source.index("async function saveConfiguration")]

    assert "if (topGroups.length > 0 && ungrouped.length > 0)" in render
    assert 'class="ua-standalone-divider"' in render
    assert "Отдельные ноды" in render
    assert "${ungrouped.length}" in render
