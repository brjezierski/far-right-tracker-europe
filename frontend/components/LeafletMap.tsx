"use client";
import { useEffect, useRef, useMemo } from "react";
import { useRouter } from "next/navigation";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { NAME_TO_ISO2 } from "../lib/iso";

const COUNTRIES_GEOJSON =
  "https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_50m_admin_0_countries.geojson";

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
} | null;

function getColor(support: number | undefined | null): string {
  if (!support || support === 0) return "#e8e8e8";  // Light gray for 0%
  if (support < 10) return "#fee5e5";
  if (support < 20) return "#fcb3b3";
  if (support < 30) return "#f88484";
  if (support < 40) return "#f15555";
  if (support < 50) return "#e12d2d";
  return "#b90000";
}

export default function LeafletMap({
  summary = null,
}: { summary?: Summary } = {}) {
  const router = useRouter();
  const mapRef = useRef<L.Map | null>(null);
  const geoLayerRef = useRef<L.GeoJSON | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const supportByIso = useMemo(() => {
    const m = new globalThis.Map<string, number>();
    if (!summary) return m;
    for (const c of Object.values(summary.countries)) {
      if (c.latestSupport != null) m.set(c.iso2, c.latestSupport);
    }
    return m;
  }, [summary]);

  // Initialize map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = L.map(containerRef.current, {
      center: [50, 12],
      zoom: 4,
      minZoom: 3,
      maxZoom: 7,
      scrollWheelZoom: false,
      dragging: true,
      touchZoom: false,
      doubleClickZoom: true,
      boxZoom: false,
      keyboard: false,
    });

    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      subdomains: "abcd",
    }).addTo(map);

    mapRef.current = map;
    let isMounted = true;

    // Load GeoJSON
    fetch(COUNTRIES_GEOJSON)
      .then((r) => r.json())
      .then((geojson) => {
        if (!isMounted || !mapRef.current) return;
        
        const geoLayer = L.geoJSON(geojson, {
          style: (feature) => {
            const name = feature?.properties?.name;
            const iso2 = name ? NAME_TO_ISO2[name as keyof typeof NAME_TO_ISO2] : undefined;
            const support = iso2 ? supportByIso.get(iso2) : undefined;
            
            return {
              fillColor: getColor(support),
              weight: 0.5,
              opacity: 1,
              color: "#555",
              fillOpacity: 0.8,
            };
          },
          onEachFeature: (feature, layer) => {
            const name = feature.properties?.name;
            const iso2 = name ? NAME_TO_ISO2[name as keyof typeof NAME_TO_ISO2] : undefined;

            // Hover tooltip
            layer.on("mouseover", function () {
              let html = `<strong>${name || ""}</strong>`;
              if (iso2 && summary && summary.countries[iso2]) {
                const c = summary.countries[iso2];
                const partiesToShow = c.activeParties || c.parties || [];
                const parties = partiesToShow.slice(0, 6).join(", ");
                const support =
                  c.latestSupport != null
                    ? `${c.latestSupport.toFixed(1)}%`
                    : "N/A";
                if (parties) {
                  html += `<br/>Parties: ${parties}<br/>Support: ${support}`;
                } else {
                  html += `<br/>Support: ${support}`;
                }
              }
              layer.bindPopup(html, { autoPan: false }).openPopup();
            });

            layer.on("mouseout", function () {
              layer.closePopup();
            });

            // Click to navigate
            layer.on("click", function () {
              if (iso2) {
                router.push(`/country/${iso2}`);
              }
            });
          },
        }).addTo(map);

        geoLayerRef.current = geoLayer;
      })
      .catch((error) => {
        console.error("Error loading GeoJSON:", error);
      });

    return () => {
      isMounted = false;
      map.remove();
      mapRef.current = null;
      geoLayerRef.current = null;
    };
  }, []);

  // Update layer styles when supportByIso changes
  useEffect(() => {
    if (!geoLayerRef.current) return;

    geoLayerRef.current.eachLayer((layer: any) => {
      const feature = layer.feature;
      const name = feature?.properties?.name;
      const iso2 = name ? NAME_TO_ISO2[name as keyof typeof NAME_TO_ISO2] : undefined;
      const support = iso2 ? supportByIso.get(iso2) : undefined;

      layer.setStyle({
        fillColor: getColor(support),
      });

      // Update tooltip handlers with current data
      layer.off("mouseover");
      layer.on("mouseover", function () {
        let html = `<strong>${name || ""}</strong>`;
        if (iso2 && summary && summary.countries[iso2]) {
          const c = summary.countries[iso2];
          const partiesToShow = c.activeParties || c.parties || [];
          const parties = partiesToShow.slice(0, 6).join(", ");
          const supportValue =
            c.latestSupport != null
              ? `${c.latestSupport.toFixed(1)}%`
              : "N/A";
          if (parties) {
            html += `<br/>Parties: ${parties}<br/>Support: ${supportValue}`;
          } else {
            html += `<br/>Support: ${supportValue}`;
          }
        }
        layer.bindPopup(html, { autoPan: false }).openPopup();
      });
    });
  }, [supportByIso, summary]);

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
