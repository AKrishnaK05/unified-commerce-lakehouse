import { useState, useEffect } from "react";
import { BarChart, Bar, Cell, LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";

// ─── Fallback snapshot (used only while the live pipeline snapshot is loading) ──
let DATA = {
  last_updated: "2026-07-20T14:23:41Z",
  pipeline_run_id: "run-2026-07-20-001",

  business: {
    total_revenue: 284763.42,
    total_orders: 806,
    avg_order_value: 353.30,
    unique_customers: 147,
    total_units_sold: 1842,
    revenue_by_channel: [
      { channel: "Shopify", revenue: 171234.50, orders: 510 },
      { channel: "Amazon", revenue: 113528.92, orders: 296 },
    ],
    revenue_trend: [
      { month: "Apr", revenue: 38200 },
      { month: "May", revenue: 52100 },
      { month: "Jun", revenue: 71400 },
      { month: "Jul", revenue: 84700 },
    ],
    top_products: [
      { product_id: "PROD-0023", units: 87, revenue: 14250 },
      { product_id: "PROD-0051", units: 72, revenue: 12890 },
      { product_id: "PROD-0007", units: 65, revenue: 11340 },
      { product_id: "PROD-0039", units: 58, revenue: 9870 },
      { product_id: "PROD-0014", units: 54, revenue: 9120 },
    ],
    inventory_health: {
      total_available: 18240,
      total_reserved: 1430,
      low_stock_products: 4,
      avg_turnover_ratio: 0.14,
    },
  },

  engineering: {
    pipeline_runs: [
      { dag: "bronze_ingestion", status: "success", duration_min: 6.96, run_at: "14:12" },
      { dag: "silver_transformations", status: "success", duration_min: 8.20, run_at: "14:20" },
      { dag: "gold_transformations", status: "success", duration_min: 14.50, run_at: "14:29" },
      { dag: "dq_lineage_checkpoint", status: "success", duration_min: 0.40, run_at: "14:44" },
    ],
    total_runs: 12,
    successful_runs: 12,
    failed_runs: 4,
    records: {
      bronze_shopify: 510,
      bronze_amazon: 306,
      bronze_inventory: 50,
      silver_orders: 789,
      silver_customers: 147,
      silver_products: 80,
      silver_inventory: 50,
      gold_revenue_mart: 312,
      gold_channel_mart: 8,
      gold_customer_360: 147,
      gold_inventory_mart: 50,
    },
    dq: {
      bronze_checks_passed: 9,
      bronze_checks_failed: 0,
      silver_checks_passed: 12,
      silver_checks_failed: 0,
      quality_score: 100,
      late_arriving_records: 16,
      duplicate_records_removed: 17,
      schema_violations_caught: 3,
      contract_warnings: 3,
    },
  },

  lineage: {
    nodes: [
      { id: "shopify_csv", label: "Shopify CSV", layer: "source", x: 60, y: 80 },
      { id: "amazon_csv", label: "Amazon CSV", layer: "source", x: 60, y: 180 },
      { id: "sftp_csv", label: "SFTP CSV", layer: "source", x: 60, y: 280 },
      { id: "bronze_shopify", label: "bronze.shopify_orders", layer: "bronze", x: 220, y: 80 },
      { id: "bronze_amazon", label: "bronze.amazon_orders", layer: "bronze", x: 220, y: 180 },
      { id: "bronze_inventory", label: "bronze.inventory_feed", layer: "bronze", x: 220, y: 280 },
      { id: "silver_orders", label: "silver.orders", layer: "silver", x: 400, y: 120 },
      { id: "silver_customers", label: "silver.customers", layer: "silver", x: 400, y: 200 },
      { id: "silver_products", label: "silver.products", layer: "silver", x: 400, y: 280 },
      { id: "silver_inventory", label: "silver.inventory", layer: "silver", x: 400, y: 360 },
      { id: "gold_revenue", label: "gold.revenue_mart", layer: "gold", x: 580, y: 80 },
      { id: "gold_channel", label: "gold.channel_performance", layer: "gold", x: 580, y: 160 },
      { id: "gold_customer", label: "gold.customer_360", layer: "gold", x: 580, y: 240 },
      { id: "gold_inventory", label: "gold.inventory_turnover", layer: "gold", x: 580, y: 320 },
    ],
    edges: [
      { from: "shopify_csv", to: "bronze_shopify" },
      { from: "amazon_csv", to: "bronze_amazon" },
      { from: "sftp_csv", to: "bronze_inventory" },
      { from: "bronze_shopify", to: "silver_orders" },
      { from: "bronze_amazon", to: "silver_orders" },
      { from: "bronze_shopify", to: "silver_customers" },
      { from: "bronze_amazon", to: "silver_customers" },
      { from: "bronze_shopify", to: "silver_products" },
      { from: "bronze_amazon", to: "silver_products" },
      { from: "bronze_inventory", to: "silver_products" },
      { from: "bronze_inventory", to: "silver_inventory" },
      { from: "silver_orders", to: "gold_revenue" },
      { from: "silver_orders", to: "gold_channel" },
      { from: "silver_orders", to: "gold_customer" },
      { from: "silver_customers", to: "gold_customer" },
      { from: "silver_inventory", to: "gold_inventory" },
      { from: "silver_orders", to: "gold_inventory" },
    ],
  },
};

// ─── Live pipeline snapshot ──────────────────────────────────────────────────────
const SNAPSHOT_URL =
  "https://raw.githubusercontent.com/AKrishnaK05/unified-commerce-lakehouse/main/public/dashboard_snapshot.json";

const FALLBACK_DATA = DATA;

function normalizeSnapshot(snapshot) {
  const metrics = snapshot.metrics || {};

  const revenueByChannel = (snapshot.revenue_by_channel || []).map((row) => ({
    channel: row.channel || row.source || "Unknown",
    revenue: Number(row.total_revenue ?? row.revenue ?? 0),
    orders: Number(row.order_count ?? row.orders ?? 0),
  }));

  const revenueTrend = (snapshot.revenue_trend || []).map((row) => ({
    month: String(row.order_date ?? row.month ?? "").slice(0, 10),
    revenue: Number(row.total_revenue ?? row.revenue ?? 0),
  }));

  const inventoryRows = snapshot.inventory_health || [];
  const inventoryTotals = inventoryRows.reduce(
    (acc, row) => {
      acc.available += Number(
        row.total_available ?? row.available ?? row.available_units ?? 0
      );
      acc.reserved += Number(
        row.total_reserved ?? row.reserved ?? row.reserved_units ?? 0
      );
      acc.turnover += Number(
        row.avg_turnover_ratio ?? row.turnover_ratio ?? row.turnover ?? 0
      );
      acc.rows += 1;
      return acc;
    },
    { available: 0, reserved: 0, turnover: 0, rows: 0 }
  );

  return {
    ...FALLBACK_DATA,
    last_updated: snapshot.last_updated || FALLBACK_DATA.last_updated,
    pipeline_run_id: String(snapshot.pipeline_run_id || "live"),
    business: {
      ...FALLBACK_DATA.business,
      total_revenue: Number(metrics.total_revenue ?? FALLBACK_DATA.business.total_revenue),
      total_orders: Number(metrics.total_orders ?? FALLBACK_DATA.business.total_orders),
      avg_order_value: Number(
        metrics.average_order_value ?? FALLBACK_DATA.business.avg_order_value
      ),
      unique_customers: Number(
        metrics.customers ?? FALLBACK_DATA.business.unique_customers
      ),
      total_units_sold: Number(
        metrics.units_sold ?? FALLBACK_DATA.business.total_units_sold
      ),
      revenue_by_channel:
        revenueByChannel.length > 0
          ? revenueByChannel
          : FALLBACK_DATA.business.revenue_by_channel,
      revenue_trend:
        revenueTrend.length > 0
          ? revenueTrend
          : FALLBACK_DATA.business.revenue_trend,
      inventory_health:
        inventoryRows.length > 0
          ? {
              total_available:
                inventoryTotals.available || FALLBACK_DATA.business.inventory_health.total_available,
              total_reserved:
                inventoryTotals.reserved || FALLBACK_DATA.business.inventory_health.total_reserved,
              low_stock_products: inventoryRows.filter((row) => {
                const value = Number(
                  row.available ?? row.available_units ?? row.total_available ?? 0
                );
                return value > 0 && value < 20;
              }).length,
              avg_turnover_ratio:
                inventoryTotals.rows > 0
                  ? inventoryTotals.turnover / inventoryTotals.rows
                  : FALLBACK_DATA.business.inventory_health.avg_turnover_ratio,
            }
          : FALLBACK_DATA.business.inventory_health,
    },
  };
}

async function fetchLiveSnapshot() {
  const response = await fetch(`${SNAPSHOT_URL}?t=${Date.now()}`, {
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Snapshot request failed (${response.status})`);
  }

  const snapshot = await response.json();
  return normalizeSnapshot(snapshot);
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
const fmt = (n) => n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(n);
const fmtCurrency = (n) => `$${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
const LAYER_COLOR = { source: "#6B7280", bronze: "#D97706", silver: "#6366F1", gold: "#F59E0B" };
const LAYER_BG = { source: "#1F2937", bronze: "#451A03", silver: "#1E1B4B", gold: "#451A03" };

// ─── Sub-components ───────────────────────────────────────────────────────────
function StatCard({ label, value, sub, color = "#3B82F6" }) {
  return (
    <div className="stat-card" style={{ background: "#111827", border: "1px solid #1F2937", borderRadius: 8, padding: "16px 20px", display: "flex", flexDirection: "column", gap: 4 }}>
      <span style={{ fontSize: 11, color: "#6B7280", letterSpacing: "0.05em" }}>{label}</span>
      <span style={{ fontSize: 28, fontWeight: 600, fontFamily: "monospace", color }}>{value}</span>
      {sub && <span style={{ fontSize: 11, color: "#9CA3AF" }}>{sub}</span>}
    </div>
  );
}

function Badge({ status }) {
  const map = { success: ["#10B981", "#022C22", "success"], failed: ["#EF4444", "#1F0606", "failed"], running: ["#3B82F6", "#0B1437", "running"] };
  const [color, bg, text] = map[status] || map.success;
  return <span className="status-badge" style={{ background: bg, color, border: `1px solid ${color}`, borderRadius: 4, padding: "2px 8px", fontSize: 11, fontFamily: "monospace" }}>{text}</span>;
}

// ─── Tab 1: Business Overview ─────────────────────────────────────────────────
function BusinessTab() {
  const { business: b } = DATA;
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      {/* Hero */}
      <div className="revenue-hero" style={{ background: "linear-gradient(135deg, #0F172A 0%, #1E3A5F 100%)", border: "1px solid #1E3A5F", borderRadius: 12, padding: "32px 36px", display: "flex", alignItems: "flex-end", justifyContent: "space-between", flexWrap: "wrap", gap: 16 }}>
        <div>
          <div style={{ fontSize: 12, color: "#60A5FA", marginBottom: 8, letterSpacing: "0.1em" }}>TOTAL REVENUE</div>
          <div style={{ fontSize: 52, fontWeight: 700, fontFamily: "monospace", color: "#FFFFFF", lineHeight: 1 }}>{fmtCurrency(b.total_revenue)}</div>
          <div style={{ fontSize: 13, color: "#93C5FD", marginTop: 10 }}>across {b.total_orders.toLocaleString()} orders · {b.unique_customers} customers · {b.total_units_sold.toLocaleString()} units sold</div>
        </div>
        <div style={{ display: "flex", gap: 24 }}>
          <div style={{ textAlign: "right" }}>
            <div style={{ fontSize: 11, color: "#6B7280" }}>Avg order value</div>
            <div style={{ fontSize: 24, fontFamily: "monospace", color: "#10B981" }}>{fmtCurrency(b.avg_order_value)}</div>
          </div>
        </div>
      </div>

      {/* Channel split */}
      <div>
        <h3 style={{ fontSize: 13, color: "#9CA3AF", margin: "0 0 12px 0", fontWeight: 500 }}>Revenue by channel</h3>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          {b.revenue_by_channel.map(ch => {
            const pct = ((ch.revenue / b.total_revenue) * 100).toFixed(1);
            return (
              <div className="channel-card" key={ch.channel} style={{ background: "#111827", border: "1px solid #1F2937", borderRadius: 8, padding: 16 }}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
                  <span style={{ color: "#E5E7EB", fontWeight: 500 }}>{ch.channel}</span>
                  <span style={{ fontFamily: "monospace", color: "#F59E0B" }}>{pct}%</span>
                </div>
                <div style={{ fontSize: 22, fontFamily: "monospace", color: "#FFFFFF", marginBottom: 4 }}>{fmtCurrency(ch.revenue)}</div>
                <div style={{ background: "#1F2937", borderRadius: 4, height: 4 }}>
                  <div style={{ width: `${pct}%`, height: 4, background: "#3B82F6", borderRadius: 4 }} />
                </div>
                <div style={{ fontSize: 11, color: "#6B7280", marginTop: 6 }}>{ch.orders} orders</div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Revenue trend */}
      <div>
        <h3 style={{ fontSize: 13, color: "#9CA3AF", margin: "0 0 12px 0", fontWeight: 500 }}>Revenue trend (last 4 months)</h3>
        <div className="chart-card" style={{ background: "#111827", border: "1px solid #1F2937", borderRadius: 8, padding: 16 }}>
          <ResponsiveContainer width="100%" height={140}>
            <LineChart data={b.revenue_trend}>
              <XAxis dataKey="month" tick={{ fill: "#6B7280", fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: "#6B7280", fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={v => `$${(v/1000).toFixed(0)}k`} />
              <Tooltip contentStyle={{ background: "#1F2937", border: "1px solid #374151", borderRadius: 6, color: "#E5E7EB", fontSize: 12 }} formatter={v => [fmtCurrency(v), "Revenue"]} />
              <Line type="monotone" dataKey="revenue" stroke="#3B82F6" strokeWidth={2.5} dot={{ fill: "#3B82F6", r: 4 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Top products + inventory */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <div>
          <h3 style={{ fontSize: 13, color: "#9CA3AF", margin: "0 0 12px 0", fontWeight: 500 }}>Top products by revenue</h3>
          <div className="list-card" style={{ background: "#111827", border: "1px solid #1F2937", borderRadius: 8, overflow: "hidden" }}>
            {b.top_products.map((p, i) => (
              <div key={p.product_id} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "10px 14px", borderBottom: i < b.top_products.length - 1 ? "1px solid #1F2937" : "none" }}>
                <div>
                  <div style={{ fontFamily: "monospace", fontSize: 12, color: "#E5E7EB" }}>{p.product_id}</div>
                  <div style={{ fontSize: 11, color: "#6B7280" }}>{p.units} units</div>
                </div>
                <div style={{ fontFamily: "monospace", color: "#10B981", fontSize: 13 }}>{fmtCurrency(p.revenue)}</div>
              </div>
            ))}
          </div>
        </div>
        <div>
          <h3 style={{ fontSize: 13, color: "#9CA3AF", margin: "0 0 12px 0", fontWeight: 500 }}>Inventory health</h3>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <StatCard label="Available units" value={fmt(b.inventory_health.total_available)} color="#10B981" />
            <StatCard label="Reserved units" value={fmt(b.inventory_health.total_reserved)} color="#F59E0B" />
            <StatCard label="Low stock products" value={b.inventory_health.low_stock_products} color="#EF4444" />
            <StatCard label="Avg turnover ratio" value={b.inventory_health.avg_turnover_ratio.toFixed(2)} color="#6366F1" />
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Tab 2: Data Engineering ──────────────────────────────────────────────────
function EngineeringTab() {
  const { engineering: e } = DATA;
  const successRate = ((e.successful_runs / (e.successful_runs + e.failed_runs)) * 100).toFixed(1);
  const runColors = ["#D97706", "#818CF8", "#F59E0B", "#53D7D1"];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      {/* Pipeline run timeline */}
      <div>
        <h3 style={{ fontSize: 13, color: "#9CA3AF", margin: "0 0 12px 0", fontWeight: 500 }}>Last pipeline run — {DATA.last_updated.slice(0, 10)}</h3>
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {e.pipeline_runs.map((run) => (
            <div className="pipeline-row" key={run.dag} style={{ background: "#111827", border: "1px solid #1F2937", borderRadius: 8, padding: "12px 16px", display: "flex", alignItems: "center", gap: 16 }}>
              <div style={{ width: 6, height: 6, borderRadius: "50%", background: run.status === "success" ? "#10B981" : "#EF4444", flexShrink: 0 }} />
              <div style={{ flex: 1 }}>
                <div style={{ fontFamily: "monospace", fontSize: 13, color: "#E5E7EB" }}>{run.dag}</div>
                <div style={{ fontSize: 11, color: "#6B7280", marginTop: 2 }}>started {run.run_at}</div>
              </div>
              <div style={{ fontFamily: "monospace", fontSize: 13, color: "#9CA3AF" }}>{run.duration_min} min</div>
              <Badge status={run.status} />
            </div>
          ))}
        </div>
      </div>

      {/* Run stats */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
        <StatCard label="Total runs" value={e.total_runs} color="#3B82F6" />
        <StatCard label="Success rate" value={`${successRate}%`} color="#10B981" />
        <StatCard label="DQ score" value={`${e.dq.quality_score}%`} color="#10B981" sub="all checks passing" />
      </div>

      {/* Run duration */}
      <div>
        <h3 style={{ fontSize: 13, color: "#9CA3AF", margin: "0 0 12px 0", fontWeight: 500 }}>Run duration by stage</h3>
        <div className="chart-card engineering-chart" style={{ background: "#111827", border: "1px solid #1F2937", borderRadius: 8, padding: 16 }}>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={e.pipeline_runs} layout="vertical" margin={{ top: 4, right: 12, left: 12, bottom: 4 }}>
              <XAxis type="number" hide />
              <YAxis dataKey="dag" type="category" width={150} tick={{ fill: "#9CA3AF", fontSize: 10, fontFamily: "monospace" }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background: "#172231", border: "1px solid #334155", borderRadius: 6, color: "#E5E7EB", fontSize: 12 }} formatter={(value) => [`${value} min`, "Duration"]} />
              <Bar dataKey="duration_min" radius={[0, 4, 4, 0]} barSize={18}>
                {e.pipeline_runs.map((run, index) => <Cell key={run.dag} fill={runColors[index % runColors.length]} />)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Records across layers */}
      <div>
        <h3 style={{ fontSize: 13, color: "#9CA3AF", margin: "0 0 12px 0", fontWeight: 500 }}>Records by layer</h3>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
          <div className="layer-card layer-bronze" style={{ background: "#451A03", border: "1px solid #78350F", borderRadius: 8, padding: 16 }}>
            <div style={{ fontSize: 11, color: "#D97706", marginBottom: 8, fontWeight: 500 }}>BRONZE</div>
            {[["shopify_orders", e.records.bronze_shopify], ["amazon_orders", e.records.bronze_amazon], ["inventory_feed", e.records.bronze_inventory]].map(([k, v]) => (
              <div key={k} style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #78350F22" }}>
                <span style={{ fontSize: 11, color: "#9CA3AF" }}>{k}</span>
                <span style={{ fontFamily: "monospace", fontSize: 12, color: "#D97706" }}>{v.toLocaleString()}</span>
              </div>
            ))}
          </div>
          <div className="layer-card layer-silver" style={{ background: "#1E1B4B", border: "1px solid #3730A3", borderRadius: 8, padding: 16 }}>
            <div style={{ fontSize: 11, color: "#818CF8", marginBottom: 8, fontWeight: 500 }}>SILVER</div>
            {[["orders", e.records.silver_orders], ["customers", e.records.silver_customers], ["products", e.records.silver_products], ["inventory", e.records.silver_inventory]].map(([k, v]) => (
              <div key={k} style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #3730A322" }}>
                <span style={{ fontSize: 11, color: "#9CA3AF" }}>{k}</span>
                <span style={{ fontFamily: "monospace", fontSize: 12, color: "#818CF8" }}>{v.toLocaleString()}</span>
              </div>
            ))}
          </div>
          <div className="layer-card layer-gold" style={{ background: "#1C1003", border: "1px solid #92400E", borderRadius: 8, padding: 16 }}>
            <div style={{ fontSize: 11, color: "#F59E0B", marginBottom: 8, fontWeight: 500 }}>GOLD</div>
            {[["revenue_mart", e.records.gold_revenue_mart], ["channel_perf", e.records.gold_channel_mart], ["customer_360", e.records.gold_customer_360], ["inv_turnover", e.records.gold_inventory_mart]].map(([k, v]) => (
              <div key={k} style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #92400E22" }}>
                <span style={{ fontSize: 11, color: "#9CA3AF" }}>{k}</span>
                <span style={{ fontFamily: "monospace", fontSize: 12, color: "#F59E0B" }}>{v.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* DQ metrics */}
      <div>
        <h3 style={{ fontSize: 13, color: "#9CA3AF", margin: "0 0 12px 0", fontWeight: 500 }}>Data quality metrics</h3>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 12 }}>
          <StatCard label="Duplicates removed" value={e.dq.duplicate_records_removed} color="#F59E0B" sub="via window-function dedup in Silver" />
          <StatCard label="Late-arriving records" value={e.dq.late_arriving_records} color="#F59E0B" sub="captured by ingestion-date partitioning" />
          <StatCard label="Schema violations caught" value={e.dq.schema_violations_caught} color="#EF4444" sub="blocked before Bronze write" />
          <StatCard label="Contract warnings" value={e.dq.contract_warnings} color="#F59E0B" sub="promo_code_applied schema drift" />
        </div>
      </div>
    </div>
  );
}

// ─── Tab 3: Lineage ───────────────────────────────────────────────────────────
function LineageTab() {
  const [selected, setSelected] = useState(null);
  const { lineage } = DATA;

  const nodeById = Object.fromEntries(lineage.nodes.map(n => [n.id, n]));

  const getUpstream = (id, visited = new Set()) => {
    if (visited.has(id)) return [];
    visited.add(id);
    const parents = lineage.edges.filter(e => e.to === id).map(e => e.from);
    return [...parents, ...parents.flatMap(p => getUpstream(p, visited))];
  };

  const getDownstream = (id, visited = new Set()) => {
    if (visited.has(id)) return [];
    visited.add(id);
    const children = lineage.edges.filter(e => e.from === id).map(e => e.to);
    return [...children, ...children.flatMap(c => getDownstream(c, visited))];
  };

  const highlighted = selected
    ? new Set([selected, ...getUpstream(selected), ...getDownstream(selected)])
    : null;

  const isHighlighted = (id) => !highlighted || highlighted.has(id);
  const isEdgeHighlighted = (e) => !highlighted || (highlighted.has(e.from) && highlighted.has(e.to));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      <div className="lineage-intro" style={{ fontSize: 12, color: "#6B7280" }}>
        Click any table to trace its upstream sources and downstream consumers.
        {selected && <span style={{ color: "#3B82F6", marginLeft: 8, cursor: "pointer" }} onClick={() => setSelected(null)}>Clear selection ×</span>}
      </div>

      <div className="lineage-summary">
        <span><strong>{lineage.nodes.length}</strong> tables</span>
        <span><strong>{lineage.edges.length}</strong> dependencies</span>
        <div className="lineage-legend">
          {Object.entries(LAYER_COLOR).map(([layer, color]) => (
            <span key={layer}><i style={{ background: color }} />{layer}</span>
          ))}
        </div>
      </div>

      {/* SVG graph */}
      <div className="lineage-graph" style={{ background: "#0D1117", border: "1px solid #1F2937", borderRadius: 12, padding: 16, overflowX: "auto" }}>
        <svg width="760" height="420" viewBox="0 0 760 420" style={{ display: "block" }}>
          {/* Layer labels */}
          {[["SOURCE", 60, "#6B7280"], ["BRONZE", 220, "#D97706"], ["SILVER", 400, "#818CF8"], ["GOLD", 580, "#F59E0B"]].map(([label, x, color]) => (
            <text key={label} x={x} y={20} textAnchor="middle" fill={color} fontSize={10} fontFamily="monospace" letterSpacing="0.08em">{label}</text>
          ))}

          {/* Edges */}
          {lineage.edges.map((edge, i) => {
            const from = nodeById[edge.from];
            const to = nodeById[edge.to];
            if (!from || !to) return null;
            const opacity = isEdgeHighlighted(edge) ? 1 : 0.08;
            const color = isEdgeHighlighted(edge) ? "#3B82F6" : "#374151";
            const x1 = from.x + 70, y1 = from.y + 14;
            const x2 = to.x, y2 = to.y + 14;
            const cx = (x1 + x2) / 2;
            return (
              <path key={i} d={`M${x1},${y1} C${cx},${y1} ${cx},${y2} ${x2},${y2}`}
                fill="none" stroke={color} strokeWidth={isEdgeHighlighted(edge) ? 1.5 : 1} opacity={opacity} strokeDasharray={isEdgeHighlighted(edge) ? "none" : "4 4"} />
            );
          })}

          {/* Nodes */}
          {lineage.nodes.map(node => {
            const dim = !isHighlighted(node.id);
            const isSelected = selected === node.id;
            const color = LAYER_COLOR[node.layer];
            return (
              <g key={node.id} style={{ cursor: "pointer" }} onClick={() => setSelected(selected === node.id ? null : node.id)}>
                <rect x={node.x} y={node.y} width={140} height={28} rx={5}
                  fill={isSelected ? color : LAYER_BG[node.layer]}
                  stroke={isSelected ? color : dim ? "#1F2937" : color}
                  strokeWidth={isSelected ? 2 : 1}
                  opacity={dim ? 0.25 : 1} />
                <text x={node.x + 70} y={node.y + 18} textAnchor="middle"
                  fill={isSelected ? "#000" : dim ? "#4B5563" : color}
                  fontSize={10} fontFamily="monospace" opacity={dim ? 0.3 : 1}>
                  {node.label.length > 22 ? node.label.slice(0, 22) + "…" : node.label}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      {/* Selected node detail */}
      {selected && (
        <div className="lineage-detail" style={{ background: "#111827", border: "1px solid #3B82F6", borderRadius: 8, padding: 16 }}>
          <div style={{ fontFamily: "monospace", color: "#3B82F6", fontSize: 13, marginBottom: 12 }}>{nodeById[selected]?.label}</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
            <div>
              <div style={{ fontSize: 11, color: "#6B7280", marginBottom: 8 }}>UPSTREAM SOURCES</div>
              {getUpstream(selected).length === 0
                ? <div style={{ fontSize: 12, color: "#4B5563" }}>No upstream dependencies</div>
                : getUpstream(selected).map(id => (
                  <div key={id} style={{ fontFamily: "monospace", fontSize: 11, color: "#9CA3AF", padding: "3px 0", cursor: "pointer" }}
                    onClick={() => setSelected(id)}>← {nodeById[id]?.label}</div>
                ))}
            </div>
            <div>
              <div style={{ fontSize: 11, color: "#6B7280", marginBottom: 8 }}>DOWNSTREAM CONSUMERS</div>
              {getDownstream(selected).length === 0
                ? <div style={{ fontSize: 12, color: "#4B5563" }}>No downstream consumers</div>
                : getDownstream(selected).map(id => (
                  <div key={id} style={{ fontFamily: "monospace", fontSize: 11, color: "#9CA3AF", padding: "3px 0", cursor: "pointer" }}
                    onClick={() => setSelected(id)}>{nodeById[id]?.label} →</div>
                ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Main App ─────────────────────────────────────────────────────────────────
export default function App() {
  const [tab, setTab] = useState("business");
  const [time, setTime] = useState(new Date());
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [, setDataVersion] = useState(0);

  const refreshData = async () => {
    setLoading(true);
    setLoadError("");

    try {
      const liveData = await fetchLiveSnapshot();
      DATA = liveData;
      setDataVersion((version) => version + 1);
    } catch (error) {
      console.error("Unable to load live dashboard snapshot:", error);
      setLoadError("Live snapshot unavailable — showing the last fallback snapshot.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const refreshTimeout = setTimeout(() => refreshData(), 0);

    const t = setInterval(() => setTime(new Date()), 1000);
    return () => {
      clearTimeout(refreshTimeout);
      clearInterval(t);
    };
  }, []);

  const tabs = [
    { id: "business", label: "Business Overview" },
    { id: "engineering", label: "Data Engineering" },
    { id: "lineage", label: "Lineage" },
  ];

  return (
    <div className="dashboard-shell" style={{ minHeight: "100vh", background: "#0A0E1A", color: "#E5E7EB", fontFamily: "'Inter', system-ui, sans-serif" }}>
      {/* Header */}
      <div className="dashboard-header" style={{ borderBottom: "1px solid #1F2937", padding: "16px 32px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <div style={{ fontSize: 18, fontWeight: 600, color: "#FFFFFF", letterSpacing: "-0.01em" }}>Unified Commerce Lakehouse</div>
          <div style={{ fontSize: 12, color: "#6B7280", marginTop: 2 }}>
            Adwaid Krishna K · Bronze → Silver → Gold · PySpark · Delta Lake · Airflow · Terraform
          </div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, justifyContent: "flex-end" }}>
            <div style={{ width: 6, height: 6, borderRadius: "50%", background: loading ? "#F59E0B" : loadError ? "#EF4444" : "#10B981" }} />
            <span style={{ fontSize: 11, color: loading ? "#F59E0B" : loadError ? "#EF4444" : "#10B981", fontFamily: "monospace" }}>
              {loading ? "loading snapshot" : loadError ? "snapshot fallback" : "live pipeline data"}
            </span>
            <button
              onClick={refreshData}
              disabled={loading}
              className="refresh-button"
              style={{
                background: "#111827",
                border: "1px solid #374151",
                color: "#9CA3AF",
                borderRadius: 5,
                padding: "3px 8px",
                fontSize: 10,
                cursor: loading ? "default" : "pointer",
                fontFamily: "monospace",
              }}
            >
              {loading ? "loading…" : "refresh"}
            </button>
          </div>
          <div style={{ fontSize: 11, color: "#4B5563", fontFamily: "monospace", marginTop: 2 }}>
            {time.toUTCString().replace(" GMT", " UTC")}
          </div>
        </div>
      </div>

      {/* Tab nav */}
      <div className="dashboard-tabs" style={{ borderBottom: "1px solid #1F2937", padding: "0 32px", display: "flex", gap: 0 }}>
        {tabs.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)}
            className={`dashboard-tab ${tab === t.id ? "is-active" : ""}`}
            style={{ padding: "12px 20px", background: "none", border: "none", cursor: "pointer", fontSize: 13, fontWeight: 500,
              color: tab === t.id ? "#3B82F6" : "#6B7280",
              borderBottom: tab === t.id ? "2px solid #3B82F6" : "2px solid transparent",
              marginBottom: -1, transition: "color 0.15s" }}>
            {t.label}
          </button>
        ))}
      </div>

      {/* Content */}
      {loadError && (
        <div style={{ maxWidth: 900, margin: "16px auto 0", padding: "0 32px" }}>
          <div style={{ background: "#1F0606", border: "1px solid #7F1D1D", color: "#FCA5A5", borderRadius: 8, padding: "10px 14px", fontSize: 11, fontFamily: "monospace" }}>
            {loadError}
          </div>
        </div>
      )}
      <div className="dashboard-content" style={{ maxWidth: 900, margin: "0 auto", padding: "28px 32px" }}>
        {tab === "business" && <BusinessTab />}
        {tab === "engineering" && <EngineeringTab />}
        {tab === "lineage" && <LineageTab />}
      </div>

      {/* Footer */}
      <div className="dashboard-footer" style={{ borderTop: "1px solid #1F2937", padding: "12px 32px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span style={{ fontSize: 11, color: "#4B5563" }}>
          Live snapshot: {DATA.last_updated.replace("T", " ").replace("Z", " UTC")} · Run {DATA.pipeline_run_id}
        </span>
        <a href="https://github.com/AKrishnaK05/unified-commerce-lakehouse"
          target="_blank" rel="noopener noreferrer"
          style={{ fontSize: 11, color: "#3B82F6", textDecoration: "none" }}>
          github.com/AKrishnaK05/unified-commerce-lakehouse →
        </a>
      </div>
    </div>
  );
}