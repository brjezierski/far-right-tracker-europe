"use client";
import { useState, useEffect, useMemo } from "react";
import dynamic from "next/dynamic";
import TimelineSlider from "./TimelineSlider";

const LeafletMap = dynamic(() => import("./LeafletMap"), { ssr: false });

type Summary = {
  updatedAt: string;
  countries: Record<
    string,
    {
      country: string;
      iso2: string;
      parties: string[];
      activeParties?: string[];
      latestSupport: number | null;
    }
  >;
};

type CountryData = {
  country: string;
  iso2: string;
  activeParties?: string[];
  seriesByParty: Record<string, Array<{ date: string; value: number }>>;
  dailySupportSeries?: Array<{ date: string; value: number }>;
  activePartiesByDate?: Record<string, string[]>;
  datapointsByParty?: Record<string, Array<{ date: string; value: number }>>;
};

type MapWithTimelineProps = {
  summary?: Summary;
  countriesData: Record<string, CountryData>;
};

// Helper function to determine active parties based on target date
// A party is active if it appears in at least one of the last 3 polling dates before or on the target date
function getActivePartiesForDate(
  datapointsByParty: Record<string, Array<{ date: string; value: number }>> | undefined,
  targetDate: Date
): string[] {
  if (!datapointsByParty) {
    return [];
  }

  const targetDateStr = targetDate.toISOString().split("T")[0];

  // Collect all unique polling dates across all parties that are <= target date
  const allDates = new Set<string>();
  for (const [_party, datapoints] of Object.entries(datapointsByParty)) {
    for (const point of datapoints) {
      if (point.date <= targetDateStr) {
        allDates.add(point.date);
      }
    }
  }

  if (allDates.size === 0) {
    return [];
  }

  // Sort dates and get the 3 most recent
  const sortedDates = Array.from(allDates).sort().reverse().slice(0, 3);

  // Find parties that have data in at least one of these 3 dates
  const activeParties: string[] = [];
  for (const [party, datapoints] of Object.entries(datapointsByParty)) {
    const partyDates = new Set(datapoints.map((p) => p.date));
    if (sortedDates.some((date) => partyDates.has(date))) {
      activeParties.push(party);
    }
  }

  return activeParties;
}

// Helper function to calculate support using the same logic as backend calculate_support_from_series
function calculateSupportFromSeries(
  datapointsByParty: Record<string, Array<{ date: string; value: number }>> | undefined,
  seriesByParty: Record<string, Array<{ date: string; value: number }>>,
  targetDate: Date
): number {
  if (!datapointsByParty) {
    return 0;
  }

  const targetDateStr = targetDate.toISOString().split("T")[0];

//   // Find the latest date across all datapoints (raw polling data)
//   let latestDataDate: string | null = null;
//   for (const [_party, datapoints] of Object.entries(datapointsByParty)) {
//     for (const point of datapoints) {
//       if (!latestDataDate || point.date > latestDataDate) {
//         latestDataDate = point.date;
//       }
//     }
//   }

//   // If target date is beyond the latest available data, return 0 (no active parties)
//   if (!latestDataDate || targetDateStr > latestDataDate) {
//     return 0;
//   }

  // Get active parties for this date
  const activeParties = getActivePartiesForDate(datapointsByParty, targetDate);

  let totalSupport = 0;

  for (const party of activeParties) {
    const series = seriesByParty[party];
    if (!series || series.length === 0) {
      continue;
    }

    // Find the latest data point on or before the target date
    let latestValue: number | null = null;
    let latestDate: string | null = null;

    for (const point of series) {
      if (point.date <= targetDateStr) {
        if (!latestDate || point.date > latestDate) {
          latestDate = point.date;
          latestValue = point.value;
        }
      }
    }

    if (latestValue !== null) {
      totalSupport += latestValue;
    }
  }

  return totalSupport;
}

// Helper function to get support at a specific date
function getSupportAtDate(
  dailySupportSeries: Array<{ date: string; value: number }> | undefined,
  datapointsByParty: Record<string, Array<{ date: string; value: number }>> | undefined,
  seriesByParty: Record<string, Array<{ date: string; value: number }>>,
  targetDate: Date
): number {
  const targetDateStr = targetDate.toISOString().split("T")[0];

  // If we have dailySupportSeries and the date is within range, use it
  if (dailySupportSeries && dailySupportSeries.length > 0) {
    const firstDate = dailySupportSeries[0].date;
    const lastDate = dailySupportSeries[dailySupportSeries.length - 1].date;

    if (targetDateStr >= firstDate && targetDateStr <= lastDate) {
      // Find the exact date or the closest date before
      let support = 0;
      for (const point of dailySupportSeries) {
        if (point.date <= targetDateStr) {
          support = point.value;
        } else {
          break;
        }
      }
      return support;
    }
  }

  // Outside the dailySupportSeries range - calculate using the same logic as backend
  return calculateSupportFromSeries(datapointsByParty, seriesByParty, targetDate);
}

export default function MapWithTimeline({
  summary,
  countriesData,
}: MapWithTimelineProps) {
  const startDate = new Date("2017-01-01");
  const endDate = new Date();
  const [currentDate, setCurrentDate] = useState(endDate);
  const [isPlaying, setIsPlaying] = useState(false);

  // Calculate summary for the selected date
  const timeFilteredSummary = useMemo(() => {
    if (!summary) return undefined;

    const filteredCountries: Summary["countries"] = {};

    for (const [iso2, countryInfo] of Object.entries(summary.countries)) {
      const countryData = countriesData[iso2];
      if (!countryData) {
        // If no time series data, keep original
        filteredCountries[iso2] = countryInfo;
        continue;
      }

      const supportAtDate = getSupportAtDate(
        countryData.dailySupportSeries,
        countryData.datapointsByParty,
        countryData.seriesByParty,
        currentDate
      );

      filteredCountries[iso2] = {
        ...countryInfo,
        latestSupport: supportAtDate,
      };
    }

    return {
      ...summary,
      countries: filteredCountries,
    };
  }, [summary, countriesData, currentDate]);

  // Animation effect
  useEffect(() => {
    if (!isPlaying) return;

    const interval = setInterval(() => {
      setCurrentDate((prev) => {
        const nextDate = new Date(prev.getTime() + 30 * 24 * 60 * 60 * 1000); // Move forward 1 month
        if (nextDate >= endDate) {
          setIsPlaying(false);
          return endDate;
        }
        return nextDate;
      });
    }, 500); // Update every 500ms

    return () => clearInterval(interval);
  }, [isPlaying, endDate]);

  const handlePlayPause = () => {
    if (currentDate >= endDate && !isPlaying) {
      // If at the end, restart from beginning
      setCurrentDate(startDate);
      setIsPlaying(true);
    } else {
      setIsPlaying(!isPlaying);
    }
  };

  return (
    <>
      <TimelineSlider
        startDate={startDate}
        endDate={endDate}
        currentDate={currentDate}
        onDateChange={setCurrentDate}
        isPlaying={isPlaying}
        onPlayPause={handlePlayPause}
      />
      <LeafletMap summary={timeFilteredSummary} />
    </>
  );
}
