"use client";
import React, { useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";

export type SeriesByParty = Record<
  string,
  Array<{ date: string; value: number }>
>;

function filterByRange(
  data: Array<{ date: string; value: number }>,
  range: string
) {
  if (range === "all") return data;
  const now = new Date();
  const cutoff = new Date(now);
  if (range === "1y") cutoff.setFullYear(now.getFullYear() - 1);
  if (range === "6m") cutoff.setMonth(now.getMonth() - 6);
  if (range === "3m") cutoff.setMonth(now.getMonth() - 3);
  return data.filter((d) => new Date(d.date) >= cutoff);
}

export default function TimeSeriesChart({
  seriesByParty,
}: {
  seriesByParty: SeriesByParty;
}) {
  const [range, setRange] = useState<"3m" | "6m" | "1y" | "all">("all");

  const option = useMemo(() => {
    const series = Object.entries(seriesByParty || {}).map(([name, arr]) => ({
      name,
      type: "line",
      smooth: true,
      showSymbol: false,
      data: filterByRange(arr, range).map((d) => [d.date, d.value]),
    }));
    return {
      tooltip: { trigger: "axis" },
      legend: { type: "scroll" },
      xAxis: { type: "time" },
      yAxis: { type: "value", axisLabel: { formatter: "{value} %" } },
      dataZoom: [{ type: "slider" }, { type: "inside" }],
      series,
    };
  }, [seriesByParty, range]);

  return (
    <div>
      <div style={{ display: "flex", gap: 8, margin: "6px 0" }}>
        {(["3m", "6m", "1y", "all"] as const).map((r) => (
          <button
            key={r}
            onClick={() => setRange(r)}
            style={{
              padding: "4px 8px",
              borderRadius: 4,
              border: "1px solid #ddd",
              background: r === range ? "#eee" : "#fff",
            }}
          >
            {r}
          </button>
        ))}
      </div>
      <ReactECharts option={option as any} style={{ height: 400 }} />
    </div>
  );
}
