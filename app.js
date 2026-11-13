const $ = (id) => document.getElementById(id);
let key = "",
  snapshot = null,
  selected = "",
  busy = false,
  readSequence = 0;
const unique = (prefix) => prefix + "_" + crypto.randomUUID().slice(0, 8);
$("tx-id").value = unique("T");
async function api(path, body) {
  const response = await fetch(
      path,
      body === undefined
        ? {}
        : {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          },
    ),
    data = await response.json();
  if (!response.ok && !data.event) throw Error(data.error || "Request failed.");
  return { ...data, httpStatus: response.status };
}
function transaction() {
  return snapshot?.state.transactions[selected];
}
function controls() {
  const t = transaction();
  for (const id of ["read", "write", "commit", "abort"])
    $(id).disabled = busy || !t || t.status !== "active";
  for (const id of ["export", "refresh"]) $(id).disabled = busy || !snapshot;
  for (const id of ["skew", "workspaces"]) $(id).disabled = busy;
  for (const form of ["create-form", "begin-form"])
    for (const b of $(form).querySelectorAll("button"))
      b.disabled = busy || (form === "begin-form" && !snapshot);
}
async function guarded(fn) {
  if (busy) return;
  busy = true;
  controls();
  $("error").textContent = "";
  try {
    await fn();
  } catch (e) {
    $("error").textContent = e.message;
  } finally {
    busy = false;
    controls();
  }
}
function render() {
  const state = snapshot.state,
    t = transaction();
  $("mode").textContent =
    state.mode === "snapshot" ? "Snapshot isolation" : "Serializable OCC";
  $("head").textContent = state.head;
  $("audit").textContent = snapshot.events.length + " verified events";
  const records = state.versions,
    rule =
      Object.keys(records).length === 2 &&
      snapshot.seed.records.east === 1 &&
      snapshot.seed.records.west === 1;
  $("rule").textContent = rule
    ? "east + west = " +
      (records.east.at(-1).value + records.west.at(-1).value) +
      (records.east.at(-1).value + records.west.at(-1).value >= 1
        ? " · holds"
        : " · violated")
    : "No application rule declared";
  $("view-note").textContent = t
    ? `SELECTED SNAPSHOT ${t.snapshot} / ${t.status.toUpperCase()}`
    : "LATEST COMMITTED STATE";
  $("record-list").replaceChildren(
    ...Object.entries(records).map(([name, versions]) => {
      const row = document.createElement("div");
      row.className = "record-row";
      const label = document.createElement("span");
      label.className = "record-name";
      label.textContent = name;
      const list = document.createElement("div");
      list.className = "versions";
      for (const [i, v] of versions.entries()) {
        const chip = document.createElement("span");
        chip.className =
          "version-chip " + (i === versions.length - 1 ? "latest" : "");
        chip.textContent = `v${v.version}: ${v.value}`;
        list.append(chip);
      }
      const visible = document.createElement("span");
      visible.className = "visible-value";
      if (t) {
        const own = Object.hasOwn(t.writes, name),
          v = versions.findLast((x) => x.version <= t.snapshot);
        visible.textContent = own
          ? `Transaction's staged value: ${t.writes[name]}`
          : `Snapshot value: ${v.value} @ v${v.version}`;
      } else visible.textContent = "Latest value: " + versions.at(-1).value;
      row.append(label, list, visible);
      return row;
    }),
  );
  $("transactions").replaceChildren(
    ...Object.values(state.transactions).map((tx) => {
      const b = document.createElement("button");
      b.textContent = `${tx.id} · ${tx.client}\n${tx.status} / snapshot ${tx.snapshot}`;
      b.setAttribute("aria-pressed", selected === tx.id);
      b.onclick = () => {
        selected = tx.id;
        $("read-result").textContent = "";
        render();
      };
      return b;
    }),
  );
  const old = $("record").value;
  $("record").replaceChildren(
    ...Object.keys(records).map((k) => new Option(k, k)),
  );
  if (Object.hasOwn(records, old)) $("record").value = old;
  $("tx-title").textContent = t ? t.id : "Choose or begin a transaction.";
  $("tx-state").textContent = t
    ? t.status.toUpperCase() +
      (t.commit === null ? "" : " / COMMIT HEAD " + t.commit)
    : "";
  $("reads").textContent = JSON.stringify(t?.reads || {}, null, 2);
  $("writes").textContent = JSON.stringify(t?.writes || {}, null, 2);
  $("conflicts").textContent = t?.conflicts.length
    ? t.conflicts
        .map(
          (c) =>
            `${c.key}: ${c.type} · observed version ${c.observed}, latest version ${c.latest}`,
        )
        .join("\n")
    : t?.reason || "Changes remain private until commit validation passes.";
  $("event-list").replaceChildren(
    ...snapshot.events
      .slice(-15)
      .reverse()
      .map((entry) => {
        const e = entry.event,
          r = e.result,
          tx = r.transaction,
          tr = document.createElement("tr");
        for (const value of [
          e.sequence,
          e.command.tx,
          e.command.action + (e.command.key ? " " + e.command.key : ""),
          e.command.action === "read"
            ? `${r.value} (${r.source})`
            : tx.reason || tx.status,
        ]) {
          const td = document.createElement("td");
          td.textContent = value;
          tr.append(td);
        }
        const td = document.createElement("td"),
          b = document.createElement("button");
        b.textContent = "View";
        b.setAttribute("aria-label", "View event " + e.sequence);
        b.onclick = () =>
          ($("event-detail").textContent = JSON.stringify(entry, null, 2));
        td.append(b);
        tr.append(td);
        return tr;
      }),
  );
  $("event-count").textContent =
    `LATEST ${Math.min(15, snapshot.events.length)} OF ${snapshot.events.length}`;
  controls();
}
async function refresh() {
  if (!key) return;
  const id = ++readSequence,
    workspace = key,
    data = await api("/api/workspaces/" + workspace);
  if (id !== readSequence || workspace !== key) return;
  snapshot = data;
  if (!Object.hasOwn(snapshot.state.transactions, selected))
    selected = Object.keys(snapshot.state.transactions).at(-1) || "";
  render();
}
async function listing(target) {
  const data = await api("/api/workspaces");
  const list = data.workspaces;
  $("workspaces").replaceChildren(
    ...list.map((x) => new Option(x.id + " · " + x.mode, x.id)),
  );
  key = target || list[0]?.id || "";
  $("workspaces").value = key;
  snapshot = null;
  selected = "";
  $("event-detail").textContent = "";
  $("read-result").textContent = "";
  controls();
  await refresh();
}
async function command(body) {
  const answer = await api("/api/workspaces/" + key + "/commands", body);
  $("event-detail").textContent = JSON.stringify(answer, null, 2);
  selected = body.tx;
  const r = answer.event.result;
  $("status").textContent =
    `Event ${answer.event.sequence}: ${body.action} · ${r.transaction.reason || r.transaction.status} · HTTP ${answer.httpStatus}`;
  await refresh();
  return answer;
}
$("workspaces").onchange = () =>
  guarded(async () => {
    key = $("workspaces").value;
    snapshot = null;
    selected = "";
    $("read-result").textContent = "";
    $("event-detail").textContent = "";
    await refresh();
  });
$("refresh").onclick = () => guarded(refresh);
$("create-form").onsubmit = (e) => {
  e.preventDefault();
  guarded(async () => {
    const records = JSON.parse($("seed").value),
      id = $("workspace-id").value;
    await api("/api/workspaces", { id, mode: $("create-mode").value, records });
    await listing(id);
  });
};
$("begin-form").onsubmit = (e) => {
  e.preventDefault();
  guarded(async () => {
    const tx = $("tx-id").value;
    await command({ action: "begin", tx, client: $("client").value });
    $("tx-id").value = unique("T");
  });
};
$("read").onclick = () =>
  guarded(async () => {
    const a = await command({
        action: "read",
        tx: selected,
        key: $("record").value,
      }),
      r = a.event.result;
    $("read-result").textContent =
      `Read ${r.key} = ${r.value} from ${r.source}${r.version === null ? "" : " version " + r.version}.`;
  });
$("write").onclick = () =>
  guarded(async () => {
    if ($("value").value === "") throw Error("Enter an integer value.");
    await command({
      action: "write",
      tx: selected,
      key: $("record").value,
      value: Number($("value").value),
    });
    $("read-result").textContent = "Write staged privately.";
  });
for (const action of ["commit", "abort"])
  $(action).onclick = () => guarded(() => command({ action, tx: selected }));
$("skew").onclick = () =>
  guarded(async () => {
    const mode = $("example-mode").value,
      id = unique(mode === "snapshot" ? "snapshot" : "serializable");
    await api("/api/workspaces", { id, mode, records: { east: 1, west: 1 } });
    await listing(id);
    for (const tx of ["A", "B"]) {
      await command({ action: "begin", tx, client: tx });
      for (const record of ["east", "west"])
        await command({ action: "read", tx, key: record });
    }
    await command({ action: "write", tx: "A", key: "east", value: 0 });
    await command({ action: "write", tx: "B", key: "west", value: 0 });
    await command({ action: "commit", tx: "A" });
    await command({ action: "commit", tx: "B" });
    $("status").textContent =
      mode === "snapshot"
        ? "Both disjoint writes committed. The example rule was violated."
        : "The second commit was aborted because its read set changed. The example rule still holds.";
  });
$("export").onclick = () =>
  guarded(async () => {
    await refresh();
    const url = URL.createObjectURL(
        new Blob([JSON.stringify(snapshot, null, 2)], {
          type: "application/json",
        }),
      ),
      a = document.createElement("a");
    a.href = url;
    a.download = key + "-mvcc-evidence.json";
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
guarded(() => listing());
