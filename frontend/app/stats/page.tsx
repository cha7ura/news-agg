"use client";

import { useEffect, useState } from "react";
import { Nav } from "@/components/nav";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import type { MonthlySourceCount } from "@/lib/types";
import { getMonthlyStats } from "@/lib/api";
import { formatNumber } from "@/lib/utils";

const PERIODS = [
  { label: "3 mo", months: 3 },
  { label: "6 mo", months: 6 },
  { label: "12 mo", months: 12 },
] as const;

const SOURCE_COLORS: Record<string, string> = {
  "ada-derana-en": "#58a6ff",
  "ada-derana-si": "#3b82f6",
  "daily-mirror-en": "#f97316",
  "newsfirst-en": "#22c55e",
  "island-en": "#a855f7",
  "economynext-en": "#ec4899",
  "colombo-gazette-en": "#14b8a6",
  "news19-si": "#eab308",
  "sunday-observer-en": "#f43f5e",
  "lanka-news-web-en": "#06b6d4",
  "lankatruth-si": "#8b5cf6",
  "daily-news-en": "#d946ef",
  "dinamina-si": "#84cc16",
  "news-lk-en": "#fb923c",
  "ada-si": "#2dd4bf",
  "colombo-telegraph-en": "#f87171",
  "deshaya-si": "#a3e635",
  "divaina-si": "#c084fc",
  "ft-en": "#fbbf24",
  "hiru-news-en": "#34d399",
  "lankadeepa-si": "#f472b6",
  "lakresa-si": "#67e8f9",
  "newsnow-si": "#facc15",
};

function fallbackColor(slug: string): string {
  let hash = 0;
  for (let i = 0; i < slug.length; i++) {
    hash = slug.charCodeAt(i) + ((hash << 5) - hash);
  }
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 70%, 55%)`;
}

interface ChartRow {
  month: string;
  [source: string]: string | number;
}

function pivotData(
  raw: MonthlySourceCount[]
): { rows: ChartRow[]; slugs: string[] } {
  const monthMap = new Map<string, Record<string, number>>();
  const slugSet = new Set<string>();

  for (const r of raw) {
    slugSet.add(r.slug);
    if (!monthMap.has(r.month)) monthMap.set(r.month, {});
    monthMap.get(r.month)![r.slug] = r.count;
  }

  const months = [...monthMap.keys()].sort();
  const slugs = [...slugSet].sort();

  const rows: ChartRow[] = months.map((m) => {
    const row: ChartRow = { month: m };
    for (const s of slugs) {
      row[s] = monthMap.get(m)?.[s] ?? 0;
    }
    return row;
  });

  return { rows, slugs };
}

function CustomTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: { name: string; value: number; color: string }[];
  label?: string;
}) {
  if (!active || !payload?.length) return null;

  const sorted = [...payload].filter((p) => p.value > 0).sort((a, b) => b.value - a.value);
  const total = sorted.reduce((s, p) => s + p.value, 0);

  return (
    <div className="rounded-lg border border-border bg-surface-raised p-3 shadow-xl max-h-80 overflow-y-auto">
      <p className="text-xs font-mono text-text-muted mb-2">{label}</p>
      <p className="text-sm font-semibold text-accent mb-2">
        Total: {formatNumber(total)}
      </p>
      {sorted.map((entry) => (
        <div key={entry.name} className="flex items-center gap-2 py-0.5">
          <span
            className="inline-block h-2.5 w-2.5 rounded-sm flex-shrink-0"
            style={{ background: entry.color }}
          />
          <span className="text-xs text-text-secondary truncate max-w-[140px]">
            {entry.name}
          </span>
          <span className="text-xs font-mono text-text ml-auto pl-2">
            {formatNumber(entry.value)}
          </span>
        </div>
      ))}
    </div>
  );
}

export default function StatsPage() {
  const [data, setData] = useState<MonthlySourceCount[]>([]);
  const [period, setPeriod] = useState(6);
  const [loading, setLoading] = useState(true);
  const [nameMap, setNameMap] = useState<Record<string, string>>({});

  useEffect(() => {
    setLoading(true);
    getMonthlyStats(period)
      .then((res) => {
        setData(res.data);
        const nm: Record<string, string> = {};
        for (const r of res.data) nm[r.slug] = r.name;
        setNameMap(nm);
      })
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [period]);

  const { rows, slugs } = pivotData(data);

  const totalArticles = data.reduce((s, r) => s + r.count, 0);
  const totalSources = slugs.length;
  const totalMonths = rows.length;

  return (
    <div className="min-h-screen">
      <Nav />

      <main className="mx-auto max-w-7xl px-4 py-8">
        <div className="flex items-center justify-between mb-6">
          <h1 className="text-2xl font-bold text-text">Monthly Articles by Source</h1>
          <div className="flex gap-1 rounded-lg border border-border bg-surface p-1">
            {PERIODS.map((p) => (
              <button
                key={p.months}
                onClick={() => setPeriod(p.months)}
                className={`rounded-md px-3 py-1 text-xs font-medium transition-colors ${
                  period === p.months
                    ? "bg-accent text-bg"
                    : "text-text-secondary hover:text-text"
                }`}
              >
                {p.label}
              </button>
            ))}
          </div>
        </div>

        {/* Summary row */}
        <div className="grid grid-cols-3 gap-4 mb-6">
          {[
            { label: "Total Articles", value: totalArticles },
            { label: "Sources", value: totalSources },
            { label: "Months", value: totalMonths },
          ].map((stat) => (
            <div
              key={stat.label}
              className="rounded-lg border border-border bg-surface p-4"
            >
              <p className="text-xs text-text-muted uppercase tracking-wider">
                {stat.label}
              </p>
              <p className="text-2xl font-bold font-mono text-accent mt-1">
                {formatNumber(stat.value)}
              </p>
            </div>
          ))}
        </div>

        {/* Chart */}
        {loading ? (
          <div className="flex items-center justify-center py-20">
            <div className="text-text-muted font-mono text-sm animate-pulse">
              Loading stats...
            </div>
          </div>
        ) : rows.length === 0 ? (
          <div className="rounded-lg border border-border border-dashed bg-surface/50 p-12 text-center">
            <p className="text-text-muted font-mono text-sm">
              No article data available. Make sure the backend is running.
            </p>
          </div>
        ) : (
          <div className="rounded-lg border border-border bg-surface p-4">
            <ResponsiveContainer width="100%" height={420}>
              <BarChart
                data={rows}
                margin={{ top: 8, right: 8, left: 0, bottom: 4 }}
              >
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="var(--border)"
                  vertical={false}
                />
                <XAxis
                  dataKey="month"
                  tick={{ fill: "var(--text-secondary)", fontSize: 12 }}
                  axisLine={{ stroke: "var(--border)" }}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fill: "var(--text-secondary)", fontSize: 12 }}
                  axisLine={false}
                  tickLine={false}
                  tickFormatter={(v: number) => formatNumber(v)}
                />
                <Tooltip
                  content={<CustomTooltip />}
                  cursor={{ fill: "rgba(255,255,255,0.03)" }}
                />
                <Legend
                  wrapperStyle={{ fontSize: 11, paddingTop: 12 }}
                  formatter={(value: string) => (
                    <span className="text-text-secondary">{value}</span>
                  )}
                />
                {slugs.map((slug) => (
                  <Bar
                    key={slug}
                    dataKey={slug}
                    name={nameMap[slug] ?? slug}
                    stackId="a"
                    fill={SOURCE_COLORS[slug] ?? fallbackColor(slug)}
                    radius={0}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Source breakdown table */}
        {rows.length > 0 && (
          <div className="mt-6 rounded-lg border border-border bg-surface overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border bg-surface-raised">
                    <th className="text-left px-4 py-2.5 text-xs font-medium text-text-muted uppercase tracking-wider sticky left-0 bg-surface-raised z-10">
                      Source
                    </th>
                    {rows.map((r) => (
                      <th
                        key={r.month}
                        className="text-right px-3 py-2.5 text-xs font-mono text-text-muted"
                      >
                        {r.month}
                      </th>
                    ))}
                    <th className="text-right px-4 py-2.5 text-xs font-medium text-text-muted uppercase tracking-wider">
                      Total
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {slugs.map((slug) => {
                    const total = rows.reduce(
                      (s, r) => s + ((r[slug] as number) ?? 0),
                      0
                    );
                    return (
                      <tr
                        key={slug}
                        className="border-b border-border/50 hover:bg-surface-raised/50 transition-colors"
                      >
                        <td className="px-4 py-2 sticky left-0 bg-surface z-10">
                          <div className="flex items-center gap-2">
                            <span
                              className="inline-block h-2.5 w-2.5 rounded-sm flex-shrink-0"
                              style={{
                                background:
                                  SOURCE_COLORS[slug] ?? fallbackColor(slug),
                              }}
                            />
                            <span className="text-text truncate max-w-[180px]">
                              {nameMap[slug] ?? slug}
                            </span>
                          </div>
                        </td>
                        {rows.map((r) => {
                          const count = (r[slug] as number) ?? 0;
                          return (
                            <td
                              key={r.month}
                              className={`text-right px-3 py-2 font-mono text-xs ${
                                count === 0
                                  ? "text-text-muted"
                                  : "text-text-secondary"
                              }`}
                            >
                              {count === 0 ? "-" : formatNumber(count)}
                            </td>
                          );
                        })}
                        <td className="text-right px-4 py-2 font-mono text-xs font-semibold text-accent">
                          {formatNumber(total)}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
