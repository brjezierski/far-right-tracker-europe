declare module 'topojson-client' {
  export function feature(topology: any, object: any): any;
  export function mesh(topology: any, object: any, filter?: any): any;
  export function meshArcs(topology: any, object: any, filter?: any): any;
  export function merge(topology: any, objects: any[]): any;
  export function mergeArcs(topology: any, objects: any[]): any;
  export function neighbors(objects: any[]): number[][];
  export function bbox(topology: any): [number, number, number, number];
  export function quantize(topology: any, transform: any): any;
}
