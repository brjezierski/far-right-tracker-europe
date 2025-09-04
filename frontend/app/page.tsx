import fs from "fs";
import path from "path";
import dynamic from "next/dynamic";
import Link from "next/link";
import styles from "./page.module.css";

const Map = dynamic<{ summary?: Summary }>(
  () => import("../components/Map").then((m) => m.default),
  {
    ssr: false,
  }
);

type Summary = {
  updatedAt: string;
  countries: Record<
    string,
    {
      country: string;
      iso2: string;
      parties: string[];
      latestSupport: number | null;
    }
  >;
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

export default function HomePage() {
  const summary = loadSummary();
  return (
    <main className={styles.main}>
      <h1>Europe Nationalist Support Map</h1>
      <p>Hover to see parties and support. Click a country for trends.</p>
      <Map summary={summary ?? undefined} />
      {summary && (
        <p className={styles.updated}>
          Data updated: {new Date(summary.updatedAt).toLocaleString()}
        </p>
      )}
      <div className={styles.list}>
        {summary &&
          Object.values(summary.countries).map((c) => (
            <div key={c.iso2} className={styles.card}>
              <h3>{c.country}</h3>
              <p>
                Latest support:{" "}
                {c.latestSupport != null
                  ? `${c.latestSupport.toFixed(1)}%`
                  : "N/A"}
              </p>
              <Link href={`/country/${c.iso2}`}>View trends</Link>
            </div>
          ))}
      </div>
    </main>
  );
}
