"use client";
import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import maplibregl, { Map as MapLibreMap } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import * as topojson from "topojson-client";
import { NAME_TO_ISO2 } from "../lib/iso";

// Lightweight world topojson focusing on Europe; for demo, fetch from unpkg
const EUROPE_TOPOJSON =
  "https://unpkg.com/world-atlas@2.0.2/countries-50m.json";

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
} | null;

export default function MapComponent({
  summary = null,
}: { summary?: Summary } = {}) {
  const router = useRouter();
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<MapLibreMap | null>(null);
  const geoRef = useRef<any | null>(null);
  const [hoverHtml, setHoverHtml] = useState<string | null>(null);

  const supportByIso = useMemo(() => {
    const m = new globalThis.Map<string, number>();
    if (!summary) return m;
    for (const c of Object.values(summary.countries)) {
      if (c.latestSupport != null) m.set(c.iso2, c.latestSupport);
    }
    return m;
  }, [summary]);

  // Update GeoJSON source with support values when supportByIso changes
  useEffect(() => {
    const m = mapRef.current;
    if (!m) return;
    const src = m.getSource("countries") as any;
    if (!src || !geoRef.current) return;
    const base = geoRef.current;
    const features = base.features.map((f: any) => {
      const name = f.properties?.name;
      const iso2 = NAME_TO_ISO2[name as keyof typeof NAME_TO_ISO2];
      const support = iso2 ? supportByIso.get(iso2) : undefined;
      return {
        ...f,
        properties: { ...f.properties, iso2: iso2 || null, support },
      };
    });
    src.setData({ type: "FeatureCollection", features });
  }, [supportByIso]);

  useEffect(() => {
    if (!containerRef.current) return;
    if (mapRef.current) return;

    const m = new maplibregl.Map({
      container: containerRef.current,
      style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
      center: [12, 50],
      zoom: 3.5,
    });

    m.addControl(new maplibregl.NavigationControl({}));

    // Wait for style to load before adding sources and layers
    m.on('load', () => {
      fetch(EUROPE_TOPOJSON)
        .then((r) => r.json())
        .then((topo) => {
          // convert topo to geojson
          const countries = (topojson as any).feature(
            topo,
            topo.objects.countries
          );
          geoRef.current = countries;
          m.addSource("countries", {
            type: "geojson",
            data: countries,
          });

          m.addLayer({
            id: "countries-fill",
            type: "fill",
            source: "countries",
            paint: {
              "fill-color": [
                "interpolate",
                ["linear"],
                ["coalesce", ["get", "support"], 0],
                0,
                "#fee5e5",
                10,
                "#fcb3b3",
                20,
                "#f88484",
                30,
                "#f15555",
                40,
                "#e12d2d",
                50,
                "#b90000",
              ],
              "fill-opacity": 0.8,
            },
          });

          m.addLayer({
            id: "countries-outline",
            type: "line",
            source: "countries",
            paint: { "line-color": "#555", "line-width": 0.5 },
          });

          // initial property enrichment
          const src = m.getSource("countries") as any;
          if (geoRef.current && src) {
            const base = geoRef.current;
            const features = base.features.map((f: any) => {
              const name = f.properties?.name;
              const iso2 = NAME_TO_ISO2[name as keyof typeof NAME_TO_ISO2];
              const support =
                iso2 && summary
                  ? summary.countries?.[iso2]?.latestSupport
                  : undefined;
              return {
                ...f,
                properties: { ...f.properties, iso2: iso2 || null, support },
              };
            });
            src.setData({ type: "FeatureCollection", features });
          }

          // Hover popup
          const popup = new maplibregl.Popup({
            closeButton: false,
            closeOnClick: false,
          });
          m.on("mousemove", "countries-fill", (e: any) => {
            const f = e.features?.[0];
            const iso2 = f?.properties?.iso2;
            const countryName = f?.properties?.name;
            let html = `<strong>${countryName || ""}</strong>`;
            if (iso2 && summary && summary.countries[iso2]) {
              const c = summary.countries[iso2];
              const parties = c.parties.slice(0, 6).join(", ");
              const support =
                c.latestSupport != null
                  ? `${c.latestSupport.toFixed(1)}%`
                  : "N/A";
              html += `<br/>Parties: ${parties}<br/>Support: ${support}`;
            }
            popup.setLngLat(e.lngLat).setHTML(html).addTo(m);
            setHoverHtml(html);
          });
          m.on("mouseleave", "countries-fill", () => {
            popup.remove();
            setHoverHtml(null);
          });

          // Click to navigate
          m.on("click", "countries-fill", (e: any) => {
            const f = e.features?.[0];
            const iso2 = f?.properties?.iso2;
            if (iso2) {
              router.push(`/country/${iso2}`);
            }
          });
        });
    });

    mapRef.current = m;
    return () => {
      m.remove();
      mapRef.current = null;
    };
  }, [summary]);

  return (
    <div
      ref={containerRef}
      style={{
        width: "100%",
        height: "70vh",
        border: "1px solid #e5e7eb",
        borderRadius: 8,
      }}
    />
  );
}
