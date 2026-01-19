"use client";
import React, { useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";

export type SeriesByParty = Record<
  string,
  Array<{ date: string; value: number }>
>;

export type DatapointsByParty = Record<
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
  datapointsByParty,
}: {
  seriesByParty: SeriesByParty;
  datapointsByParty?: DatapointsByParty;
}) {
  const [range, setRange] = useState<"3m" | "6m" | "1y" | "all">("all");
  const [stacked, setStacked] = useState(false);

  const option = useMemo(() => {
    const series: any[] = [];
    const colors = [
      '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de',
      '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc', '#d48265',
    ];
    
    if (stacked) {
      // Stacked mode uses pre-calculated daily series
      Object.entries(seriesByParty || {}).forEach(([name, arr], index) => {
        const color = colors[index % colors.length];
        const filteredData = filterByRange(arr, range);
        
        series.push({
          name,
          type: "line",
          smooth: false,
          showSymbol: false,
          stack: "total",
          areaStyle: {},
          lineStyle: { width: 0 },
          itemStyle: { color: color },
          data: filteredData.map((d) => [d.date, d.value]),
        });
      });
    } else {
      // Non-stacked mode shows scatter + line
      Object.entries(seriesByParty || {}).forEach(([name, arr], index) => {
        const color = colors[index % colors.length];
        const filteredSeries = filterByRange(arr, range);
        
        // Add scatter series for individual poll points (from datapointsByParty if available)
        if (datapointsByParty && datapointsByParty[name]) {
          const filteredDatapoints = filterByRange(datapointsByParty[name], range);
          series.push({
            name: `${name}`,
            type: "scatter",
            symbolSize: 6,
            itemStyle: { 
              color: color,
              opacity: 0.6 
            },
            data: filteredDatapoints.map((d) => [d.date, d.value]),
            // Hide from legend and tooltip
            legendHoverLink: true,
            legend: { show: false },
            tooltip: { show: false },
          });
        }
        
        // Add line series for rolling average (pre-calculated in seriesByParty)
        series.push({
          name,
          type: "line",
          smooth: false,
          showSymbol: false,
          lineStyle: { width: 2, color: color },
          itemStyle: { color: color },
          data: filteredSeries.map((d) => [d.date, d.value]),
        });
      });
    }
    
    return {
      tooltip: { 
        trigger: "axis",
        axisPointer: { type: "cross" },
        valueFormatter: (value: any) => `${Number(value).toFixed(1)}%`,
        formatter: (params: any) => {
          if (!Array.isArray(params)) return '';
          
          // Filter out series with 0 or null values
          const filtered = params.filter((p: any) => p.value && p.value[1] > 0);
          
          if (filtered.length === 0) return '';
          
          const date = new Date(filtered[0].value[0]).toLocaleDateString();
          let result = `${date}<br/>`;
          
          filtered.forEach((p: any) => {
            result += `${p.marker} ${p.seriesName}: ${Number(p.value[1]).toFixed(1)}%<br/>`;
          });
          
          return result;
        },
      },
      legend: { 
        type: "scroll",
      },
      xAxis: { type: "time" },
      yAxis: { type: "value", axisLabel: { formatter: "{value} %" } },
      dataZoom: [{ type: "slider" }, { type: "inside" }],
      series,
    };
  }, [seriesByParty, range, stacked]);

  return (
    <div>
      <div style={{ display: "flex", gap: 8, margin: "6px 0", flexWrap: "wrap" }}>
        <div style={{ display: "flex", gap: 8 }}>
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
        <button
          onClick={() => setStacked(!stacked)}
          style={{
            padding: "4px 8px",
            borderRadius: 4,
            border: "1px solid #ddd",
            background: stacked ? "#eee" : "#fff",
            marginLeft: "auto",
          }}
        >
          {stacked ? "Stacked ✓" : "Stacked"}
        </button>
      </div>
      <ReactECharts 
        key={stacked ? 'stacked' : 'normal'} 
        option={option as any} 
        style={{ height: 400 }} 
        notMerge={true}
      />
    </div>
  );
}
