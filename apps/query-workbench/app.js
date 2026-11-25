/* DQSE Control Tower interactive frontend */

const state = {
  presets: {},
  overview: null,
};

function apiKey() {
  return document.getElementById("api-key-input").value;
}

async function apiFetch(path, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    "X-API-Key": apiKey(),
    ...(options.headers || {}),
  };
  const resp = await fetch(path, { ...options, headers });
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${body}`);
  }
  return resp.json();
}

function fmtBytes(b) {
  if (b == null || b === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let idx = 0;
  let val = Number(b);
  while (val >= 1024 && idx < units.length - 1) {
    val /= 1024;
    idx++;
  }
  return `${val.toFixed(idx === 0 ? 0 : 1)} ${units[idx]}`;
}

function fmtNum(v) {
  if (v == null) return "NULL";
  if (typeof v === "number") {
    return Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return String(v);
}

function collectOptimizerOptions() {
  return {
    projection_pushdown: document.getElementById("opt-proj").checked,
    predicate_pushdown: document.getElementById("opt-pred").checked,
    partition_pruning: document.getElementById("opt-part").checked,
    two_phase_agg: document.getElementById("opt-2p").checked,
    skew_salting: document.getElementById("opt-skew").checked,
    enable_cache: document.getElementById("opt-cache").checked,
    max_worker_mem_mb: parseFloat(document.getElementById("opt-mem").value || "0") || 0,
  };
}

function renderOverview(data) {
  state.overview = data;
  state.presets = data.preset_queries || {};

  const workers = data.workers || [];
  const healthy = workers.filter((w) => w.status === "HEALTHY");
  const totalVnodes = workers.reduce((acc, w) => acc + (w.assigned_vnodes || 0), 0);

  const pill = document.getElementById("cluster-pill");
  pill.textContent = `${healthy.length}/${workers.length} gRPC Workers Online`;
  pill.className = `pill ${healthy.length > 0 ? "pill-ok" : "pill-warn"}`;

  const fresh = data.freshness_utc ? data.freshness_utc.replace("T", " ").replace("Z", " UTC") : "—";
  document.getElementById("freshness-pill").textContent = `Freshness: ${fresh}`;

  document.getElementById("kpi-workers").textContent = `${healthy.length}/${workers.length} Online`;
  document.getElementById("kpi-workers-sub").textContent = `${totalVnodes} consistent-hash vnodes`;

  const tables = data.tables || [];
  const totalRows = tables.reduce((acc, t) => acc + (t.row_count || 0), 0);
  const totalParts = tables.reduce((acc, t) => acc + (t.partition_count || 0), 0);
  const totalBytes = tables.reduce((acc, t) => acc + (t.total_bytes || 0), 0);
  document.getElementById("kpi-rows").textContent = `${totalRows.toLocaleString()} rows`;
  document.getElementById("kpi-rows-sub").textContent = `${tables.length} tables · ${totalParts} parts · ${fmtBytes(totalBytes)}`;

  // Render worker cards
  const wg = document.getElementById("workers-grid");
  wg.innerHTML = workers
    .map(
      (w) => `
      <div class="worker-card">
        <div class="worker-card-top">
          <span>${w.worker_id}</span>
          <span class="pill ${w.status === "HEALTHY" ? "pill-ok" : "pill-warn"}">${w.status}</span>
        </div>
        <div class="worker-stat">gRPC: ${w.endpoint}</div>
        <div class="worker-stat">VNodes: ${w.assigned_vnodes} · Active: ${w.active_tasks}</div>
        <div class="worker-stat">Cache: ${w.cache_entries} keys (${w.cache_hits}H/${w.cache_misses}M)</div>
        <div class="worker-stat">Spilled: ${fmtBytes(w.total_spilled_bytes)}</div>
      </div>`
    )
    .join("");

  // Render catalog table
  const cbody = document.querySelector("#catalog-table tbody");
  cbody.innerHTML = tables
    .map(
      (t) => `
      <tr>
        <td><strong>${t.table_name}</strong></td>
        <td class="num">v${t.snapshot_version}</td>
        <td>${(t.partition_columns || []).join(", ") || "unpartitioned"}</td>
        <td class="num">${(t.row_count || 0).toLocaleString()}</td>
        <td class="num">${t.partition_count}</td>
        <td class="num">${fmtBytes(t.total_bytes)}</td>
      </tr>`
    )
    .join("");

  renderBenchmarks(data.benchmarks, data.ablations, data.chaos, data.quarantine_count || 0);
}

function renderBenchmarks(bench, abl, chaos, quarantineCount) {
  const container = document.getElementById("benchmark-bars");
  const localProfile = bench && bench.profiles ? bench.profiles.local || bench.profiles.smoke : null;

  if (!localProfile || !localProfile.queries) {
    container.innerHTML = `<p class="empty-note">Run <code>make benchmark</code> to populate DuckDB 2.0 &amp; DataFusion 55.0 comparisons.</p>`;
  } else {
    const entries = Object.entries(localProfile.queries);
    container.innerHTML = entries
      .map(([qname, q]) => {
        const dqMs = q.dqse_p50_ms || q.dqse_ms || 0;
        const dfMs = q.datafusion_p50_ms || q.datafusion_ms || 0;
        const dkMs = q.duckdb_p50_ms || q.duckdb_ms || 0;
        const maxVal = Math.max(dqMs, dfMs, dkMs, 1);
        const parityBadge = q.duckdb_exact_match && q.datafusion_exact_match ? "EXACT PARITY" : "CHECK";
        return `
        <div class="bench-row">
          <div class="bench-title">
            <span>${qname}</span>
            <span style="color:#86efac;font-size:11px">${parityBadge} · ${q.rows_returned} rows</span>
          </div>
          <div class="bar-line">
            <span>DQSE (3-wkr)</span>
            <div class="bar-track"><div class="bar-fill bar-dqse" style="width:${Math.max(3, (dqMs / maxVal) * 100)}%"></div></div>
            <span class="num">${dqMs.toFixed(1)} ms</span>
          </div>
          <div class="bar-line">
            <span>DataFusion 55</span>
            <div class="bar-track"><div class="bar-fill bar-df" style="width:${Math.max(3, (dfMs / maxVal) * 100)}%"></div></div>
            <span class="num">${dfMs.toFixed(1)} ms</span>
          </div>
          <div class="bar-line">
            <span>DuckDB 2.0</span>
            <div class="bar-track"><div class="bar-fill bar-duck" style="width:${Math.max(3, (dkMs / maxVal) * 100)}%"></div></div>
            <span class="num">${dkMs.toFixed(1)} ms</span>
          </div>
        </div>`;
      })
      .join("");
  }

  const summaryEl = document.getElementById("ablation-chaos-summary");
  const cards = [];
  if (abl && abl.ablations) {
    const a = abl.ablations;
    if (a.q1_two_phase_agg_vs_single_phase) {
      const m = a.q1_two_phase_agg_vs_single_phase;
      cards.push(
        `<div class="summary-card"><strong>2-Phase Agg Shuffle Cut</strong>${m.shuffle_reduction_pct}% less Arrow IPC wire traffic (${fmtBytes(m.single_phase_shuffle_bytes)} &rarr; ${fmtBytes(m.two_phase_shuffle_bytes)})</div>`
      );
    }
    if (a.q2_pruning_vs_full_scan) {
      const m = a.q2_pruning_vs_full_scan;
      cards.push(
        `<div class="summary-card"><strong>Partition + RowGroup Pruning</strong>${m.speedup_x}x faster (${m.unoptimized_ms} ms &rarr; ${m.optimized_ms} ms), ${m.partitions_pruned} parts skipped</div>`
      );
    }
    if (a.warm_cache_vs_cold) {
      const m = a.warm_cache_vs_cold;
      cards.push(
        `<div class="summary-card"><strong>Versioned LRU Cache Hit</strong>${m.speedup_x}x speedup (${m.cold_ms} ms cold &rarr; ${m.warm_ms} ms warm)</div>`
      );
    }
  }
  const passedDrills = chaos && chaos.drills ? chaos.drills.filter((d) => d.passed).length : 0;
  const totalDrills = chaos && chaos.drills ? chaos.drills.length : 0;
  cards.push(
    `<div class="summary-card"><strong>Chaos Drills &amp; DQ Guard</strong>${passedDrills}/${totalDrills || 7} fault drills verified · ${quarantineCount} quarantined anomalies</div>`
  );
  summaryEl.innerHTML = cards.join("");
}

function renderQueryResponse(resp) {
  const p = resp.profile;
  document.getElementById("kpi-latency").textContent = `${p.total_duration_ms.toFixed(1)} ms`;
  document.getElementById("kpi-latency-sub").textContent = p.result_cache_hit
    ? "Result Cache HIT (0 ms exec)"
    : `Plan ${p.planning_ms.toFixed(1)} ms · Exec ${p.execution_ms.toFixed(1)} ms`;

  document.getElementById("kpi-pruning").textContent = `${p.partitions_pruned}/${p.partitions_total} parts`;
  document.getElementById("kpi-pruning-sub").textContent = `${p.row_groups_pruned} row groups pruned`;

  document.getElementById("kpi-shuffle").textContent = fmtBytes(p.total_shuffle_bytes);
  document.getElementById("kpi-shuffle-sub").textContent =
    p.total_spilled_bytes > 0
      ? `Spilled ${fmtBytes(p.total_spilled_bytes)} (${p.total_spill_files} files)`
      : `In-memory · peak ${fmtBytes(p.peak_worker_mem_bytes)}`;

  const banner = document.getElementById("parity-banner");
  if (p.reference_parity) {
    const dk = p.reference_parity.duckdb;
    const df = p.reference_parity.datafusion;
    const ok = dk.exact_match && df.exact_match;
    document.getElementById("kpi-parity").textContent = ok ? "100% Exact" : "Mismatch";
    document.getElementById("kpi-parity-sub").textContent = `DuckDB ${dk.duration_ms}ms · DF ${df.duration_ms}ms`;
    banner.className = `parity-banner ${ok ? "" : "error"}`;
    banner.innerHTML = `
      <span>${ok ? "Exact Numerical Parity Verified" : "Parity Mismatch"}</span>
      <span>DuckDB ${dk.version}: ${dk.exact_match ? "EXACT" : dk.detail} (${dk.duration_ms} ms) · DataFusion ${df.version}: ${df.exact_match ? "EXACT" : df.detail} (${df.duration_ms} ms)</span>
    `;
  } else {
    banner.className = "parity-banner hidden";
  }

  // Result table
  const thead = document.querySelector("#result-table thead");
  const tbody = document.querySelector("#result-table tbody");
  thead.innerHTML = `<tr>${resp.columns.map((c) => `<th>${c}</th>`).join("")}</tr>`;
  tbody.innerHTML = resp.rows
    .slice(0, 100)
    .map(
      (row) =>
        `<tr>${row
          .map((cell) => `<td class="${typeof cell === "number" ? "num" : ""}">${fmtNum(cell)}</td>`)
          .join("")}</tr>`
    )
    .join("");

  // Plans & DAG
  document.getElementById("profile-meta").textContent = `${p.query_id} · ${resp.row_count} rows · ${p.task_retries} retries · trace ${p.trace_id.slice(0, 10)}`;
  document.getElementById("physical-dag-pre").textContent = p.physical_dag || "—";
  document.getElementById("logical-plan-pre").textContent = p.logical_plan || "—";
  document.getElementById("optimized-plan-pre").textContent = p.optimized_plan || "—";

  // Optimizer badges
  const rules = p.optimizer_rules_applied || [];
  document.getElementById("rules-badges").innerHTML = rules
    .map((r) => `<span class="rule-badge">${r}</span>`)
    .join("");

  // Stage Gantt
  const gantt = document.getElementById("gantt-container");
  const stages = p.stages || [];
  if (!stages.length) {
    gantt.innerHTML = `<p class="empty-note">${p.result_cache_hit ? "Served directly from Coordinator Versioned LRU Result Cache." : "No worker stages executed."}</p>`;
    return;
  }

  const totalSpan = Math.max(p.total_duration_ms, 1);
  gantt.innerHTML = stages
    .map((st) => {
      const taskRows = (st.tasks || [])
        .slice(0, 8)
        .map((t) => {
          const leftPct = Math.min(92, ((t.start_offset_ms || 0) / totalSpan) * 100);
          const widthPct = Math.max(2.5, Math.min(100 - leftPct, (t.duration_ms / totalSpan) * 100));
          const wClass = t.worker_id.endsWith("1")
            ? "w1"
            : t.worker_id.endsWith("2")
            ? "w2"
            : t.worker_id.endsWith("3")
            ? "w3"
            : "w4";
          return `
          <div class="gantt-row">
            <span>${t.worker_id} · p${t.partition_id}</span>
            <div class="gantt-track">
              <div class="gantt-bar ${wClass}" style="left:${leftPct}%;width:${widthPct}%" title="${t.task_id}: ${t.duration_ms}ms"></div>
            </div>
            <span class="num">${t.duration_ms.toFixed(1)} ms</span>
          </div>`;
        })
        .join("");
      const moreCount = Math.max(0, (st.tasks || []).length - 8);
      return `
      <div class="stage-block">
        <div class="stage-head">
          <span>${st.stage_name} (${st.task_count} tasks)</span>
          <span>${st.duration_ms.toFixed(1)} ms</span>
        </div>
        <div class="stage-metrics">
          in=${st.rows_in.toLocaleString()} out=${st.rows_out.toLocaleString()} · shuffle=${fmtBytes(st.shuffle_bytes)} · spill=${fmtBytes(st.spilled_bytes)} · rg_pruned=${st.row_groups_pruned} · retries=${st.retries}
        </div>
        ${taskRows}
        ${moreCount > 0 ? `<div class="worker-stat">+ ${moreCount} additional parallel worker tasks</div>` : ""}
      </div>`;
    })
    .join("");
}

async function runActiveQuery() {
  const statusEl = document.getElementById("query-status-msg");
  statusEl.textContent = "Executing across gRPC workers...";
  try {
    const resp = await apiFetch("/v1/query", {
      method: "POST",
      body: JSON.stringify({
        sql: document.getElementById("sql-editor").value,
        optimizer: collectOptimizerOptions(),
        verify_reference: document.getElementById("opt-verify").checked,
      }),
    });
    renderQueryResponse(resp);
    statusEl.textContent = `Succeeded in ${resp.profile.total_duration_ms.toFixed(1)} ms (${resp.row_count} rows)`;
    const ov = await apiFetch("/v1/overview");
    renderOverview(ov);
  } catch (err) {
    statusEl.textContent = `Error: ${err.message}`;
  }
}

async function runExplain() {
  const statusEl = document.getElementById("query-status-msg");
  statusEl.textContent = "Planning & optimizing DAG...";
  try {
    const resp = await apiFetch("/v1/explain", {
      method: "POST",
      body: JSON.stringify({
        sql: document.getElementById("sql-editor").value,
        optimizer: collectOptimizerOptions(),
      }),
    });
    document.getElementById("physical-dag-pre").textContent = resp.physical_dag;
    document.getElementById("logical-plan-pre").textContent = resp.logical_plan;
    document.getElementById("optimized-plan-pre").textContent = resp.optimized_plan;
    document.getElementById("kpi-pruning").textContent = `${resp.partitions_pruned}/${resp.partitions_total} parts`;
    document.querySelector('.tab[data-tab="tab-dag"]').click();
    statusEl.textContent = `EXPLAIN ready · ${resp.partitions_pruned}/${resp.partitions_total} partitions pruned`;
  } catch (err) {
    statusEl.textContent = `Explain error: ${err.message}`;
  }
}

async function loadInitial() {
  try {
    const data = await apiFetch("/v1/overview");
    renderOverview(data);
    const defaultKey = "q3_distributed_join_agg";
    const editor = document.getElementById("sql-editor");
    if (!editor.value.trim() && state.presets[defaultKey]) {
      editor.value = state.presets[defaultKey];
      document.querySelectorAll(".chip").forEach((c) => {
        c.classList.toggle("active", c.dataset.preset === defaultKey);
      });
    }
    if (editor.value.trim()) {
      await runActiveQuery();
    }
  } catch (err) {
    document.getElementById("query-status-msg").textContent = `Init error: ${err.message}`;
  }
}

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
      document.querySelectorAll(".tab-pane").forEach((p) => p.classList.remove("active"));
      btn.classList.add("active");
      document.getElementById(btn.dataset.tab).classList.add("active");
    });
  });

  document.querySelectorAll(".chip").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".chip").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      const key = btn.dataset.preset;
      if (state.presets[key]) {
        document.getElementById("sql-editor").value = state.presets[key];
      }
      document.getElementById("opt-mem").value = key === "q5_low_memory_spill_agg" ? "0.5" : "0";
      runActiveQuery();
    });
  });

  document.getElementById("btn-run").addEventListener("click", runActiveQuery);
  document.getElementById("btn-explain").addEventListener("click", runExplain);
  document.getElementById("btn-refresh").addEventListener("click", async () => {
    const data = await apiFetch("/v1/overview");
    renderOverview(data);
  });
  document.getElementById("btn-invalidate-cache").addEventListener("click", async () => {
    const statusEl = document.getElementById("query-status-msg");
    try {
      const res = await fetch("/v1/cache/invalidate?table_name=*", {
        method: "POST",
        headers: { "X-API-Key": "dev-admin-key-change-me" },
      });
      const json = await res.json();
      statusEl.textContent = `Cache flushed (coordinator=${json.coordinator_evicted})`;
      const ov = await apiFetch("/v1/overview");
      renderOverview(ov);
    } catch (err) {
      statusEl.textContent = `Cache flush error: ${err.message}`;
    }
  });

  loadInitial();
});
