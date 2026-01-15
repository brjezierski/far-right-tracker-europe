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

function calculateRollingAverage(
  data: Array<{ date: string; value: number }>,
  windowSize: number = 5
): Array<[string, number]> {
  if (data.length < windowSize) {
    // If not enough data points, return empty array
    return [];
  }
  
  const result: Array<[string, number]> = [];
  for (let i = windowSize - 1; i < data.length; i++) {
    const window = data.slice(i - windowSize + 1, i + 1);
    const avg = window.reduce((sum, d) => sum + d.value, 0) / windowSize;
    result.push([data[i].date, avg]);
  }
  return result;
}

export default function TimeSeriesChart({
  seriesByParty,
}: {
  seriesByParty: SeriesByParty;
}) {
  const [range, setRange] = useState<"3m" | "6m" | "1y" | "all">("all");

  const option = useMemo(() => {
    const series: any[] = [];
    const colors = [
      '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de',
      '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc', '#d48265',
    ];
    
    Object.entries(seriesByParty || {}).forEach(([name, arr], index) => {
      const filteredData = filterByRange(arr, range);
      const rollingAvg = calculateRollingAverage(filteredData, 5);
      const color = colors[index % colors.length];
      
      // Add scatter series for individual poll points
      series.push({
        name: `${name}`,
        type: "scatter",
        symbolSize: 6,
        itemStyle: { 
          color: color,
          opacity: 0.6 
        },
        data: filteredData.map((d) => [d.date, d.value]),
        // Hide from legend and tooltip
        legendHoverLink: true,
        legend: { show: false },
        tooltip: { show: false },
      });
      
      // Add line series for rolling average
      series.push({
        name,
        type: "line",
        smooth: false,
        showSymbol: false,
        lineStyle: { width: 2, color: color },
        itemStyle: { color: color },
        data: rollingAvg,
      });
    });
    
    return {
      tooltip: { 
        trigger: "axis",
        axisPointer: { type: "cross" },
        valueFormatter: (value: any) => `${Number(value).toFixed(1)}%`,
      },
      legend: { 
        type: "scroll",
      },
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
