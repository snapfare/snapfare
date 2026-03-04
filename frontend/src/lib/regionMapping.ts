/**
 * Maps IATA airport codes to destination regions.
 * Used for filtering deals by user's preferred region preference.
 *
 * Regions:
 *   europe        → European destinations
 *   americas      → North & South America + Caribbean
 *   asia_pacific  → Asia and Oceania
 *   middle_east   → Middle East
 *   africa        → African destinations
 *   oceania       → Pacific islands, Indian Ocean
 */

export type Region =
  | "europe"
  | "americas"
  | "asia_pacific"
  | "middle_east"
  | "africa"
  | "oceania";

export const REGION_LABELS: Record<Region, string> = {
  europe: "Europa",
  americas: "Amerika",
  asia_pacific: "Asien / Pazifik",
  middle_east: "Naher Osten",
  africa: "Afrika",
  oceania: "Ozeanien",
};

export const IATA_REGION: Record<string, Region> = {
  // ── Europe ──────────────────────────────────────────────────────────────────
  LHR: "europe", LGW: "europe", LCY: "europe", STN: "europe", LTN: "europe",
  CDG: "europe", ORY: "europe", NCE: "europe", MRS: "europe", LYS: "europe",
  FCO: "europe", MXP: "europe", LIN: "europe", VCE: "europe", NAP: "europe",
  MAD: "europe", BCN: "europe", PMI: "europe", AGP: "europe", ALC: "europe",
  IBZ: "europe", VLC: "europe", SVQ: "europe",
  VIE: "europe",
  BER: "europe", MUC: "europe", FRA: "europe", HAM: "europe", DUS: "europe",
  CGN: "europe", STR: "europe", NUE: "europe",
  AMS: "europe",
  BRU: "europe",
  ZRH: "europe", GVA: "europe", BSL: "europe",
  PRG: "europe",
  BUD: "europe",
  WAW: "europe", KRK: "europe",
  ATH: "europe", SKG: "europe",
  IST: "europe", SAW: "europe",  // Istanbul is on border — counted Europe for deals
  LIS: "europe", OPO: "europe",
  CPH: "europe", ARN: "europe", OSL: "europe", HEL: "europe",
  OTP: "europe", SOF: "europe",
  ZAG: "europe",
  MLA: "europe",
  PMO: "europe", CTA: "europe",
  TLS: "europe", BOD: "europe",
  DBV: "europe", SPU: "europe",
  RIX: "europe", TLL: "europe", VNO: "europe",
  TIA: "europe",
  SKP: "europe",
  SJJ: "europe",
  BEG: "europe",

  // ── Americas ─────────────────────────────────────────────────────────────────
  JFK: "americas", EWR: "americas", LGA: "americas", BOS: "americas",
  MIA: "americas", FLL: "americas", MCO: "americas", TPA: "americas",
  LAX: "americas", SFO: "americas", SJC: "americas", SEA: "americas",
  ORD: "americas", MDW: "americas",
  DFW: "americas", IAH: "americas",
  ATL: "americas",
  YYZ: "americas", YVR: "americas", YUL: "americas",
  GRU: "americas", GIG: "americas", CNF: "americas",
  EZE: "americas", AEP: "americas",
  SCL: "americas",
  BOG: "americas",
  LIM: "americas",
  UIO: "americas",
  CUN: "americas", MEX: "americas",
  PTY: "americas",  // Panama Hub
  PUJ: "americas", SDQ: "americas",  // Dominican Republic
  MBJ: "americas", KIN: "americas",  // Jamaica
  NAS: "americas",  // Bahamas
  SJU: "americas",  // Puerto Rico
  BGI: "americas",  // Barbados
  POS: "americas",  // Trinidad

  // ── Asia / Pacific ───────────────────────────────────────────────────────────
  HND: "asia_pacific", NRT: "asia_pacific", KIX: "asia_pacific",
  ICN: "asia_pacific", GMP: "asia_pacific",
  PEK: "asia_pacific", PVG: "asia_pacific", CAN: "asia_pacific",
  BKK: "asia_pacific", HKT: "asia_pacific", CNX: "asia_pacific",
  SIN: "asia_pacific",
  KUL: "asia_pacific", PEN: "asia_pacific",
  MNL: "asia_pacific", CEB: "asia_pacific",
  CGK: "asia_pacific", DPS: "asia_pacific",
  HAN: "asia_pacific", SGN: "asia_pacific",
  DAD: "asia_pacific",
  DEL: "asia_pacific", BOM: "asia_pacific", MAA: "asia_pacific",
  CCU: "asia_pacific", HYD: "asia_pacific",
  CMB: "asia_pacific",
  DAC: "asia_pacific",
  KTM: "asia_pacific",
  SYD: "asia_pacific", MEL: "asia_pacific", BNE: "asia_pacific",
  PER: "asia_pacific", ADL: "asia_pacific",
  AKL: "asia_pacific", CHC: "asia_pacific",

  // ── Middle East ───────────────────────────────────────────────────────────────
  DXB: "middle_east", AUH: "middle_east", SHJ: "middle_east",
  DOH: "middle_east",
  RUH: "middle_east", JED: "middle_east",
  MCT: "middle_east",
  AMM: "middle_east", BGW: "middle_east",
  BEY: "middle_east",
  TLV: "middle_east",
  KWI: "middle_east",
  BAH: "middle_east",

  // ── Africa ────────────────────────────────────────────────────────────────────
  CPT: "africa", JNB: "africa", DUR: "africa",
  NBO: "africa",
  ADD: "africa",
  CMN: "africa", RAK: "africa", AGA: "africa",
  CAI: "africa", HRG: "africa", SSH: "africa",
  TUN: "africa",
  ALG: "africa",
  LOS: "africa", ABV: "africa",
  ACC: "africa",
  DAK: "africa",

  // ── Oceania / Indian Ocean ────────────────────────────────────────────────────
  MLE: "oceania",  // Maldives
  SEZ: "oceania",  // Seychelles
  RUN: "oceania",  // Réunion
  MRU: "oceania",  // Mauritius
  TNR: "oceania",  // Madagascar
  PPT: "oceania",  // Tahiti
  NOU: "oceania",  // New Caledonia
};

/**
 * Returns the region for a given IATA code, or null if unknown.
 */
export function getRegion(iata: string | null | undefined): Region | null {
  if (!iata) return null;
  return IATA_REGION[iata.toUpperCase()] ?? null;
}

/**
 * Checks if a deal's destination matches any of the user's preferred regions.
 * Returns true if preferred_regions is empty (no filter) or if there's a match.
 */
export function matchesRegion(
  destinationIata: string | null | undefined,
  preferredRegions: string[]
): boolean {
  if (!preferredRegions.length) return true;
  const region = getRegion(destinationIata);
  if (!region) return true;  // Unknown region — include by default
  return preferredRegions.includes(region);
}
