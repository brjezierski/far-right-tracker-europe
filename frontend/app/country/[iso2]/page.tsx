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
      <p>Parties: {data.parties.map((p: any) => p.name).join(", ")}</p>
      <p>
        Latest combined support:{" "}
        {data.latestSupport != null
          ? `${data.latestSupport.toFixed(1)}%`
          : "N/A"}
      </p>
      <TimeSeriesChart seriesByParty={data.seriesByParty || {}} />
      <h3>Sources</h3>
      <ul>
        {(data.sources || []).map((s: any, idx: number) => (
          <li key={idx}>
            {s.type}{" "}
            {s.url ? (
              <a href={s.url} target="_blank">
                {s.url}
              </a>
            ) : null}
          </li>
        ))}
      </ul>
      <p>Updated: {new Date(data.updatedAt).toLocaleString()}</p>
    </main>
  );
}
