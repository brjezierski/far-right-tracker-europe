export function supportToColor(value: number | null | undefined) {
  if (value == null) return "#eee";
  const stops = [
    { t: 0, c: [254, 229, 229] },
    { t: 10, c: [252, 179, 179] },
    { t: 20, c: [248, 132, 132] },
    { t: 30, c: [241, 85, 85] },
    { t: 40, c: [225, 45, 45] },
    { t: 50, c: [185, 0, 0] },
  ];
  let a = stops[0],
    b = stops[stops.length - 1];
  for (let i = 1; i < stops.length; i++) {
    if (value < stops[i].t) {
      b = stops[i];
      a = stops[i - 1];
      break;
    }
  }
  const ratio = Math.min(1, Math.max(0, (value - a.t) / (b.t - a.t)));
  const rgb = [0, 1, 2].map((i) =>
    Math.round(a.c[i] + (b.c[i] - a.c[i]) * ratio)
  );
  return `rgb(${rgb[0]},${rgb[1]},${rgb[2]})`;
}
