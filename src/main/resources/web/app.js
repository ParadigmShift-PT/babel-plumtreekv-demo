"use strict";

// PlumtreeKV web UI: poll /api/state a few times a second, render this node's
// converged registry (colour-coded by writer) + active-view neighbours, and POST
// set/delete writes. Each screen shows only this node's own replicated copy —
// no global state is gathered.

const rowsEl = document.getElementById("rows");
const keyCountEl = document.getElementById("keyCount");
const nodeIdEl = document.getElementById("nodeId");
const digestEl = document.getElementById("digest");
const neighboursEl = document.getElementById("neighbours");
const keyInput = document.getElementById("key");
const valueInput = document.getElementById("value");
const setBtn = document.getElementById("setBtn");
const delBtn = document.getElementById("delBtn");
const leaveBtn = document.getElementById("leaveBtn");
const navStatus = document.getElementById("navStatus");
const statusText = document.getElementById("statusText");

let left = false;          // set once this node has left the fleet
let prevKeys = new Set();  // to flash newly-appearing/changed rows

// Deterministic colour per writer id — so the same writer is the same colour on
// every node's screen (the "multiple trees" made visible).
function ownerColor(id) {
    let h = 0;
    for (let i = 0; i < id.length; i++) {
        h = (h * 31 + id.charCodeAt(i)) >>> 0;
    }
    return `hsl(${h % 360}, 68%, 62%)`;
}

function relTime(ms) {
    const d = Date.now() - ms;
    if (d < 1000) return "just now";
    if (d < 60000) return Math.floor(d / 1000) + "s ago";
    if (d < 3600000) return Math.floor(d / 60000) + "m ago";
    return Math.floor(d / 3600000) + "h ago";
}

function render(state) {
    nodeIdEl.textContent = state.node;
    digestEl.textContent = state.digest;
    keyCountEl.textContent = state.keys + (state.keys === 1 ? " key" : " keys");

    // Neighbours.
    neighboursEl.innerHTML = "";
    if (!state.neighbours || state.neighbours.length === 0) {
        const li = document.createElement("li");
        li.className = "muted";
        li.textContent = "(none yet)";
        neighboursEl.appendChild(li);
    } else {
        state.neighbours.forEach((n) => {
            const li = document.createElement("li");
            li.textContent = n;
            neighboursEl.appendChild(li);
        });
    }

    // Registry rows.
    const entries = state.entries || [];
    const nowKeys = new Set(entries.map((e) => e.key + "=" + e.value));
    rowsEl.innerHTML = "";
    if (entries.length === 0) {
        const tr = document.createElement("tr");
        tr.className = "empty";
        tr.innerHTML = '<td colspan="4">empty — write a key to seed the fleet</td>';
        rowsEl.appendChild(tr);
    } else {
        entries.forEach((e) => {
            const tr = document.createElement("tr");
            tr.className = "row";
            const color = ownerColor(e.owner);
            tr.style.setProperty("--owner", color);
            if (!prevKeys.has(e.key + "=" + e.value)) {
                tr.classList.add("flash");
            }
            const kd = document.createElement("td"); kd.className = "k"; kd.textContent = e.key;
            const vd = document.createElement("td"); vd.className = "v"; vd.textContent = e.value;
            const od = document.createElement("td");
            od.innerHTML = '<span class="owner-chip"><span class="swatch"></span>' + escapeHtml(e.owner) + "</span>";
            od.querySelector(".swatch").style.background = color;
            const wd = document.createElement("td"); wd.className = "ver"; wd.textContent = relTime(e.version);
            tr.append(kd, vd, od, wd);
            rowsEl.appendChild(tr);
        });
    }
    prevKeys = nowKeys;
}

function escapeHtml(s) {
    return s.replace(/[&<>"']/g, (c) =>
        ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function setLive(live) {
    navStatus.classList.toggle("live", live);
    statusText.textContent = live ? "connected" : "offline";
}

// ── Writes ──────────────────────────────────────────────────────────────────
async function postJson(url, body) {
    try {
        await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
    } catch (e) { /* transient; next poll reflects reality */ }
}

function doSet() {
    const key = keyInput.value.trim();
    if (!key) { keyInput.focus(); return; }
    postJson("/api/set", { key, value: valueInput.value });
    valueInput.value = "";
    keyInput.focus();
}

function doDelete() {
    const key = keyInput.value.trim();
    if (!key) { keyInput.focus(); return; }
    postJson("/api/delete", { key });
}

setBtn.addEventListener("click", doSet);
delBtn.addEventListener("click", doDelete);
valueInput.addEventListener("keydown", (e) => { if (e.key === "Enter") doSet(); });
keyInput.addEventListener("keydown", (e) => { if (e.key === "Enter") valueInput.focus(); });

leaveBtn.addEventListener("click", async () => {
    if (!confirm("Leave the fleet? This node will shut down.")) return;
    await postJson("/api/leave", {});
    left = true;
    navStatus.classList.remove("live");
    navStatus.classList.add("gone");
    statusText.textContent = "left";
    showGone();
});

function showGone() {
    let ov = document.querySelector(".gone-overlay");
    if (!ov) {
        ov = document.createElement("div");
        ov.className = "gone-overlay";
        ov.innerHTML = '<div class="box"><h2>This node has left the fleet.</h2>' +
            "<p>The remaining writers' trees heal and stay consistent. " +
            "Relaunch the node to rejoin.</p></div>";
        document.body.appendChild(ov);
    }
    ov.classList.add("show");
}

// ── Poll loop ─────────────────────────────────────────────────────────────────
async function poll() {
    if (left) return;
    try {
        const resp = await fetch("/api/state", { cache: "no-store" });
        if (resp.ok) {
            setLive(true);
            render(await resp.json());
        } else {
            setLive(false);
        }
    } catch (e) {
        setLive(false);
    }
}

setInterval(poll, 600);
poll();
