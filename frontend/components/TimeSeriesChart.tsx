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
  const [stacked, setStacked] = useState(false);

  const option = useMemo(() => {
    const series: any[] = [];
    const colors = [
      '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de',
      '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc', '#d48265',
    ];
    
    if (stacked) {
      // For stacked mode, we need to align all data on the same dates
      // Collect all unique dates across all parties
      const allDatesSet = new Set<string>();
      const rollingAvgsByParty: Record<string, Map<string, number>> = {};
      
      Object.entries(seriesByParty || {}).forEach(([name, arr]) => {
        const filteredData = filterByRange(arr, range);
        const rollingAvg = calculateRollingAverage(filteredData, 5);
        
        const avgMap = new Map<string, number>();
        rollingAvg.forEach(([date, value]) => {
          allDatesSet.add(date);
          avgMap.set(date, value);
        });
        rollingAvgsByParty[name] = avgMap;
      });
      
      // Sort dates chronologically
      const sortedDates = Array.from(allDatesSet).sort();
      
      // Create series with aligned data
      Object.entries(seriesByParty || {}).forEach(([name, _arr], index) => {
        const color = colors[index % colors.length];
        const avgMap = rollingAvgsByParty[name];
        
        // Find first and last dates with actual data for this party
        const partyDates = sortedDates.filter(date => avgMap.has(date));
        if (partyDates.length === 0) {
          return; // Skip if no data
        }
        const firstDate = partyDates[0];
        const lastDate = partyDates[partyDates.length - 1];
        
        // Create data array with interpolation for gaps
        const alignedData = sortedDates.map((date, idx) => {
          const value = avgMap.get(date);
          
          // If we have actual data, use it
          if (value !== undefined) {
            return [date, value];
          }
          
          // Only interpolate within the range of actual data
          if (date < firstDate || date > lastDate) {
            return [date, null];
          }
          
          // Find previous and next known values
          let prevValue: number | null = null;
          let nextValue: number | null = null;
          let prevIdx = idx - 1;
          let nextIdx = idx + 1;
          
          while (prevIdx >= 0 && prevValue === null) {
            const prevDate = sortedDates[prevIdx];
            if (avgMap.has(prevDate)) {
              prevValue = avgMap.get(prevDate)!;
            }
            prevIdx--;
          }
          
          while (nextIdx < sortedDates.length && nextValue === null) {
            const nextDate = sortedDates[nextIdx];
            if (avgMap.has(nextDate)) {
              nextValue = avgMap.get(nextDate)!;
            }
            nextIdx++;
          }
          
          // Interpolate if we have both previous and next values
          if (prevValue !== null && nextValue !== null) {
            return [date, (prevValue + nextValue) / 2];
          }
          
          return [date, null];
        });
        
        series.push({
          name,
          type: "line",
          smooth: false,
          showSymbol: false,
          stack: "total",
          areaStyle: {},
          lineStyle: { width: 0 },
          itemStyle: { color: color },
          data: alignedData,
        });
      });
    } else {
      // Non-stacked mode (original behavior)
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
    }
    
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
