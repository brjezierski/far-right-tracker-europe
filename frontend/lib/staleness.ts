// A country is stale when its latest poll is older than this many months.
export const STALE_AFTER_MONTHS = 18;

export function isStale(lastPollDate: string | null | undefined, asOf: Date): boolean {
  if (!lastPollDate) return false;
  const cutoff = new Date(asOf);
  cutoff.setMonth(cutoff.getMonth() - STALE_AFTER_MONTHS);
  return lastPollDate < cutoff.toISOString().split("T")[0];
}

// Latest raw poll date on or before asOf, across all parties.
export function lastPollDateAsOf(
  datapointsByParty: Record<string, Array<{ date: string }>> | undefined,
  asOf: Date
): string | null {
  const asOfStr = asOf.toISOString().split("T")[0];
  let last: string | null = null;
  for (const points of Object.values(datapointsByParty ?? {})) {
    for (const p of points) {
      if (p.date <= asOfStr && (!last || p.date > last)) last = p.date;
    }
  }
  return last;
}
