"use client";
import { useEffect, useRef, useMemo } from "react";
import { useRouter } from "next/navigation";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import { NAME_TO_ISO2 } from "../lib/iso";

const COUNTRIES_GEOJSON =
  "https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_50m_admin_0_countries.geojson";

// Capital coordinates for tooltip positioning (avoids overseas territories)
const CAPITAL_COORDS: Record<string, [number, number]> = {
  AL: [41.3275, 19.8187], // Tirana
  AM: [40.1792, 44.4991], // Yerevan
  AT: [48.2082, 16.3738], // Vienna
  BA: [43.8564, 18.4131], // Sarajevo
  BE: [50.8503, 4.3517], // Brussels
  BG: [42.6977, 23.3219], // Sofia
  CH: [46.9480, 7.4474], // Bern
  CY: [35.1856, 33.3823], // Nicosia
  CZ: [50.0755, 14.4378], // Prague
  DE: [52.5200, 13.4050], // Berlin
  DK: [55.6761, 12.5683], // Copenhagen
  EE: [59.4370, 24.7536], // Tallinn
  ES: [40.4168, -3.7038], // Madrid
  FI: [60.1695, 24.9354], // Helsinki
  FR: [48.8566, 2.3522], // Paris
  GB: [51.5074, -0.1278], // London
  GE: [41.7151, 44.8271], // Tbilisi
  GR: [37.9838, 23.7275], // Athens
  HR: [45.8150, 15.9819], // Zagreb
  HU: [47.4979, 19.0402], // Budapest
  IE: [53.3498, -6.2603], // Dublin
  IS: [64.1466, -21.9426], // Reykjavik
  IT: [41.9028, 12.4964], // Rome
  LT: [54.6872, 25.2797], // Vilnius
  LU: [49.6116, 6.1319], // Luxembourg
  LV: [56.9496, 24.1052], // Riga
  MD: [47.0105, 28.8638], // Chisinau
  ME: [42.4304, 19.2594], // Podgorica
  MK: [41.9973, 21.4280], // Skopje
  MT: [35.8989, 14.5146], // Valletta
  NL: [52.3702, 4.8952], // Amsterdam
  NO: [59.9139, 10.7522], // Oslo
  PL: [52.2297, 21.0122], // Warsaw
  PT: [38.7223, -9.1393], // Lisbon
  RO: [44.4268, 26.1025], // Bucharest
  RS: [44.7866, 20.4489], // Belgrade
  RU: [55.7558, 37.6173], // Moscow
  SE: [59.3293, 18.0686], // Stockholm
  SI: [46.0569, 14.5058], // Ljubljana
  SK: [48.1486, 17.1077], // Bratislava
  TR: [39.9334, 32.8597], // Ankara
  UA: [50.4501, 30.5234], // Kyiv
  XK: [42.6629, 21.1655], // Pristina
};

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
              
              // Position popup at capital coordinates if available
              if (iso2 && CAPITAL_COORDS[iso2] && mapRef.current) {
                const coords = CAPITAL_COORDS[iso2];
                L.popup({ autoPan: false })
                  .setLatLng([coords[0], coords[1]])
                  .setContent(html)
                  .openOn(mapRef.current);
              } else {
                layer.bindPopup(html, { autoPan: false }).openPopup();
              }
            });

            layer.on("mouseout", function () {
              if (mapRef.current) {
                mapRef.current.closePopup();
              }
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
        
        // Position popup at capital coordinates if available
        if (iso2 && CAPITAL_COORDS[iso2] && mapRef.current) {
          const coords = CAPITAL_COORDS[iso2];
          L.popup({ autoPan: false })
            .setLatLng([coords[0], coords[1]])
            .setContent(html)
            .openOn(mapRef.current);
        } else {
          layer.bindPopup(html, { autoPan: false }).openPopup();
        }
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
