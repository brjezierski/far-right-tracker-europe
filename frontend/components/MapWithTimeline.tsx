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
};

type MapWithTimelineProps = {
  summary?: Summary;
  countriesData: Record<string, CountryData>;
};

// Helper function to get latest support value on or before a given date
function getSupportAtDate(
  seriesByParty: Record<string, Array<{ date: string; value: number }>>,
  activeParties: string[] | undefined,
  targetDate: Date
): number {
  let totalSupport = 0;
  let partyCount = 0;

  for (const [party, series] of Object.entries(seriesByParty)) {
    // Skip inactive parties
    if (activeParties && !activeParties.includes(party)) {
      continue;
    }
    // Find the latest data point on or before the target date
    let latestValue: number | null = null;
    let latestDate: Date | null = null;

    for (const point of series) {
      const pointDate = new Date(point.date);
      if (pointDate <= targetDate) {
        if (!latestDate || pointDate > latestDate) {
          latestDate = pointDate;
          latestValue = point.value;
        }
      }
    }

    if (latestValue !== null) {
      totalSupport += latestValue;
      partyCount++;
    }
  }

  return partyCount > 0 ? totalSupport : 0;
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
        countryData.seriesByParty,
        countryData.activeParties,
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
