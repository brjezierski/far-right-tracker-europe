import fs from "fs";
import path from "path";
import Link from "next/link";
import TimeSeriesChart from "../../../components/TimeSeriesChart";

function loadCountry(iso2: string) {
  try {
    const file = path.join(
      process.cwd(),
      "..",
      "data",
      "countries",
      `${iso2}.json`
    );
    const raw = fs.readFileSync(file, "utf-8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function loadSummary() {
  try {
    const file = path.join(process.cwd(), "..", "data", "summary.json");
    const raw = fs.readFileSync(file, "utf-8");
    return JSON.parse(raw);
  } catch {
    return { countries: {} };
  }
}

export async function generateStaticParams() {
  const summary = loadSummary();
  return Object.keys(summary.countries).map((iso2) => ({
    iso2,
  }));
}

export default function CountryPage({ params }: { params: { iso2: string } }) {
  const data = loadCountry(params.iso2);
  if (!data) {
    return (
      <main style={{ padding: 20 }}>
        <p>No data.</p>
        <Link href="/">Back</Link>
      </main>
    );
  }

  return (
    <main style={{ padding: 20 }}>
      <h2>{data.country}</h2>
      <p>
        <Link href="/">Back to map</Link>
      </p>
      <p>
        Latest combined support:{" "}
        {data.latestSupport != null
          ? `${data.latestSupport.toFixed(1)}%`
          : "N/A"}
      </p>
      <TimeSeriesChart seriesByParty={data.seriesByParty || {}} />
      <h3>Sources</h3>
      <ul>
        {(data.sources || []).map((url: string, idx: number) => (
          <li key={idx}>
            <a href={url} target="_blank" rel="noopener noreferrer">
              {url}
            </a>
          </li>
        ))}
      </ul>
      <p>Updated: {data.latestUpdate ? new Date(data.latestUpdate).toLocaleString() : "N/A"}</p>
    </main>
  );
}
