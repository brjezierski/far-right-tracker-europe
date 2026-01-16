import fs from "fs";
import path from "path";
import MapWithTimeline from "../components/MapWithTimeline";
import styles from "./page.module.css";

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
  seriesByParty: Record<string, Array<{ date: string; value: number }>>;
};

function loadSummary(): Summary | null {
  try {
    const file = path.join(process.cwd(), "..", "data", "summary.json");
    const raw = fs.readFileSync(file, "utf-8");
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

function loadAllCountriesData(): Record<string, CountryData> {
  const summary = loadSummary();
  if (!summary) return {};

  const countriesData: Record<string, CountryData> = {};
  
  for (const iso2 of Object.keys(summary.countries)) {
    try {
      const file = path.join(
        process.cwd(),
        "..",
        "data",
        "countries",
        `${iso2}.json`
      );
      const raw = fs.readFileSync(file, "utf-8");
      const data = JSON.parse(raw);
      countriesData[iso2] = {
        country: data.country,
        iso2: data.iso2,
        seriesByParty: data.seriesByParty || {},
      };
    } catch (e) {
      // Skip countries with missing data
    }
  }
  
  return countriesData;
}

export default function HomePage() {
  const summary = loadSummary();
  const countriesData = loadAllCountriesData();
  
  return (
    <main className={styles.main}>
      <h1>Suport for Far-Right and National Conservative Parties</h1>
      <p>Hover to see parties and support. Click a country for trends.</p>
      <MapWithTimeline summary={summary ?? undefined} countriesData={countriesData} />
      {summary && (
        <p className={styles.updated}>
          Data updated: {new Date(summary.updatedAt).toLocaleString()}
        </p>
      )}
    </main>
  );
}
